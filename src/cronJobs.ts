import { mockProvider } from "./mockProvider";
import { app, prisma } from "./server";

let isProcessing = false;

// Durable outbound job runner: one process worker, persisted jobs, randomized mock provider failure ~20%.
export const worker = setInterval(async () => {
  if (isProcessing) return;
  isProcessing = true;
  try {
    const job = await prisma.$transaction(
      async (tx) => {
        const candidates = await tx.$queryRaw<
          any[]
        >`SELECT id FROM "OutboundJob" WHERE status='pending' AND "nextAttemptAt"<=NOW() ORDER BY "createdAt" LIMIT 1 FOR UPDATE SKIP LOCKED`;
        if (!candidates.length) return null;
        const id = candidates[0].id;
        return tx.outboundJob.update({
          where: { id },
          data: {
            status: "processing",
            lockedAt: new Date(),
            attempts: { increment: 1 },
          },
          include: { message: true },
        });
      },
      {
        maxWait: 10000,
        timeout: 20000,
      },
    );
    if (!job) return;
    const fail = Math.random() < 0.2;
    if (!fail) {
      const providerResponse = await mockProvider.sendMessage({
        channel: job.message.channel,
        body: job.message.body,
      });
      await prisma.$transaction(
        async (tx) => {
          await tx.outboundJob.update({
            where: { id: job.id },
            data: { status: "completed", lockedAt: null },
          });
          await tx.message.update({
            where: { id: job.messageId },
            data: {
              status: "sent",
              providerMessageId: providerResponse.providerMessageId,
            },
          });
          await tx.outboxEvent.create({
            data: {
              locationId: job.message.locationId,
              eventType: "message.status",
              payload: {
                messageId: job.messageId,
                conversationId: job.conversationId,
                status: "sent",
              },
            },
          });
        },
        {
          maxWait: 10000,
          timeout: 20000,
        },
      );
    } else if (job.attempts >= 3) {
      await prisma.$transaction(
        async (tx) => {
          await tx.outboundJob.update({
            where: { id: job.id },
            data: {
              status: "failed",
              lockedAt: null,
              lastError: "Mock provider failure",
            },
          });
          await tx.message.update({
            where: { id: job.messageId },
            data: { status: "failed" },
          });
        },
        {
          maxWait: 10000,
          timeout: 20000,
        },
      );
    } else {
      const delay = 1000 * Math.pow(2, job.attempts - 1);
      await prisma.outboundJob.update({
        where: { id: job.id },
        data: {
          status: "pending",
          lockedAt: null,
          nextAttemptAt: new Date(Date.now() + delay),
          lastError: "Mock provider failure",
        },
      });
    }
  } catch (err) {
    app.log.error(err);
  } finally {
    isProcessing = false;
  }
}, 500);
