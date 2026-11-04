import Fastify from "fastify";
import cors from "@fastify/cors";
import {
  PrismaClient,
  Channel,
  Direction,
  MessageStatus,
  ConversationStatus,
} from "@prisma/client";
import { z } from "zod";
import { phone, email, preview, advisoryKey, locHeader } from "./utils";
import { channel, inboundSchema, rank, statusSchema } from "./constant";
import { worker } from "./cronJobs";

export const prisma = new PrismaClient();
export const app = Fastify({ logger: true });
app.register(cors, { origin: true });

app.get("/health", async () => ({ ok: true }));

//webhook of inbound messages from provider, create/update contact/conversation/message, 
// emit outboxEvent for SSE
app.post("/webhooks/inbound", async (req: any, reply: any) => {
  const p = inboundSchema.safeParse(req.body);
  if (!p.success) return reply.code(400).send({ error: p.error.flatten() });
  const d = p.data,
    identity = d.channel === "email" ? email(d.from) : phone(d.from),
    sentAt = new Date(d.sentAt);
  try {
    const result = await prisma.$transaction(
      async (tx) => {
        // update/insert contact as per email/phone number match
        const contact = await tx.contact.upsert({
          where:
            d.channel === "email"
              ? {
                  locationId_emailNormalized: {
                    locationId: d.locationId,
                    emailNormalized: identity,
                  },
                }
              : {
                  locationId_phoneNormalized: {
                    locationId: d.locationId,
                    phoneNormalized: identity,
                  },
                },
          create: {
            locationId: d.locationId,
            ...(d.channel === "email"
              ? { emailNormalized: identity }
              : { phoneNormalized: identity }),
          },
          update: {},
        });
        //update/insert one conversation per contact per location
        const conv = await tx.conversation.upsert({
          where: {
            locationId_contactId: {
              locationId: d.locationId,
              contactId: contact.id,
            },
          },
          create: { locationId: d.locationId, contactId: contact.id },
          update: {},
        });
        const inserted = await tx.message.createMany({
          data: [
            {
              locationId: d.locationId,
              conversationId: conv.id,
              providerMessageId: d.providerMessageId,
              channel: d.channel as Channel,
              direction: Direction.inbound,
              body: d.body,
              status: MessageStatus.delivered,
              sentAt,
            },
          ],
          skipDuplicates: true,
        });

        //means already received this message, so don't increment unread count or update lastMessageAt/lastMessagePreview
        if (!inserted.count)
          return { duplicate: true, conversationId: conv.id };

        await tx.conversation.update({
          where: { id: conv.id },
          data: {
            unreadCount: { increment: 1 },
            status: ConversationStatus.open,
          },
        });
        await tx.$executeRaw`UPDATE "Conversation" SET "lastMessageAt"=${sentAt}, "lastMessagePreview"=${preview(d.body)}, "updatedAt"=NOW() WHERE "id"=${conv.id} AND ("lastMessageAt" IS NULL OR "lastMessageAt"<=${sentAt})`;
        await tx.outboxEvent.create({
          data: {
            locationId: d.locationId,
            eventType: "message.created",
            payload: {
              conversationId: conv.id,
              providerMessageId: d.providerMessageId,
              channel: d.channel,
              body: d.body,
              sentAt: d.sentAt,
            },
          },
        });
        // Associate and reconcile status events received before the message existed.
        const pending = await tx.messageStatusEvent.findMany({
          where: { providerMessageId: d.providerMessageId, applied: false },
          orderBy: { eventTimestamp: "asc" },
        });
        let next: MessageStatus = MessageStatus.delivered;
        for (const ev of pending) {
          if (ev.status === MessageStatus.failed) {
            if (next !== MessageStatus.read) next = MessageStatus.failed;
          } else if (
            next !== MessageStatus.failed &&
            rank[ev.status] > rank[next]
          )
            next = ev.status;
          await tx.messageStatusEvent.update({
            where: { id: ev.id },
            data: { applied: true },
          });
        }
        if (pending.length)
          await tx.message.updateMany({
            where: { providerMessageId: d.providerMessageId },
            data: { status: next },
          });
        return { duplicate: false, conversationId: conv.id };
      },
      {
        maxWait: 10000,
        timeout: 20000,
      },
    );
    return reply.code(result.duplicate ? 200 : 201).send(result);
  } catch (err) {
    req.log.error(err);
    return reply.code(500).send({ error: "Inbound processing failed" });
  }
});

// create outbound message for a conversation, create outboundJob, emit outboxEvent for SSE
app.post("/conversations/:id/messages", async (req: any, reply: any) => {
  const locationId = locHeader(req);
  if (!locationId)
    return reply.code(401).send({ error: "X-Location-Id required" });
  const p = z.object({ id: z.string().uuid() }).safeParse(req.params),
    b = z.object({ channel, body: z.string().min(1) }).safeParse(req.body);
  if (!p.success || !b.success)
    return reply.code(400).send({ error: "Invalid request" });
  const created = await prisma.$transaction(
    async (tx) => {
      const conv = await tx.conversation.findFirst({
        where: { id: p.data.id, locationId },
      });
      if (!conv) return null;

      const msg = await tx.message.create({
        data: {
          locationId,
          conversationId: conv.id,
          channel: b.data.channel as Channel,
          direction: Direction.outbound,
          body: b.data.body,
          status: MessageStatus.queued,
          sentAt: new Date(),
        },
      });
      await tx.outboundJob.create({
        data: { messageId: msg.id, conversationId: conv.id },
      });
      await tx.outboxEvent.create({
        data: {
          locationId,
          eventType: "message.queued",
          payload: { conversationId: conv.id, messageId: msg.id },
        },
      });
      return msg;
    },
    {
      maxWait: 10000,
      timeout: 20000,
    },
  );
  if (!created)
    return reply.code(404).send({ error: "Conversation not found" });
  return reply.code(202).send(created);
});

// webhook of message status updates from provider, update message status, emit outboxEvent for SSE
app.post("/webhooks/status", async (req: any, reply: any) => {
  const p = statusSchema.safeParse(req.body);
  if (!p.success) return reply.code(400).send({ error: p.error.flatten() });
  const d = p.data;
  try {
    const result = await prisma.$transaction(
      async (tx) => {
        await tx.$queryRaw`
          SELECT pg_advisory_xact_lock(
            ${advisoryKey(d.providerMessageId)}
          )::text
        `;
        const event = await tx.messageStatusEvent.create({
          data: {
            providerMessageId: d.providerMessageId,
            status: d.status as MessageStatus,
            eventTimestamp: new Date(d.timestamp),
          },
        });
        const msg = await tx.message.findUnique({
          where: { providerMessageId: d.providerMessageId },
        });
        if (!msg) return { accepted: true, pending: true, eventId: event.id };
        const events = await tx.messageStatusEvent.findMany({
          where: { providerMessageId: d.providerMessageId, applied: false },
          orderBy: { eventTimestamp: "asc" },
        });
        let next = msg.status;
        for (const ev of events) {
          if (ev.status === MessageStatus.failed) {
            if (next !== MessageStatus.delivered && next !== MessageStatus.read)
              next = MessageStatus.failed;
          } else if (
            next !== MessageStatus.failed &&
            rank[ev.status] > rank[next]
          )
            next = ev.status;
          await tx.messageStatusEvent.update({
            where: { id: ev.id },
            data: { applied: true, messageId: msg.id },
          });
        }
        await tx.message.update({
          where: { id: msg.id },
          data: { status: next },
        });
        await tx.outboxEvent.create({
          data: {
            locationId: msg.locationId,
            eventType: "message.status",
            payload: {
              messageId: msg.id,
              conversationId: msg.conversationId,
              status: next,
            },
          },
        });
        return { accepted: true, pending: false, status: next };
      },
      {
        maxWait: 10000,
        timeout: 20000, 
      },
    );
    return reply.code(202).send(result);
  } catch (err) {
    req.log.error(err);
    return reply.code(500).send({ error: "Status processing failed" });
  }
});

// get conversations for a location, with optional status/assignedTo filter, cursor-based pagination
app.get(
  "/locations/:locationId/conversations",
  async (req: any, reply: any) => {
    const p = z.object({ locationId: z.string().min(1) }).safeParse(req.params),
      q = z
        .object({
          status: z.enum(["open", "closed"]).optional(),
          assignedTo: z.string().optional(),
          limit: z.coerce.number().int().min(1).max(100).default(20),
          cursor: z.string().optional(),
        })
        .safeParse(req.query);
    if (!p.success || !q.success)
      return reply.code(400).send({ error: "Invalid query" });
    const loc = locHeader(req);
    if (!loc || loc !== p.data.locationId)
      return reply.code(403).send({ error: "Location mismatch" });
    const where: any = {
      locationId: loc,
      ...(q.data.status ? { status: q.data.status } : {}),
      ...(q.data.assignedTo ? { assignedTo: q.data.assignedTo } : {}),
    };
    if (q.data.cursor) {
      try {
        const c = JSON.parse(
          Buffer.from(q.data.cursor, "base64url").toString(),
        );
        where.OR = [
          { lastMessageAt: { lt: new Date(c.at) } },
          { lastMessageAt: new Date(c.at), id: { lt: c.id } },
        ];
      } catch {
        return reply.code(400).send({ error: "Invalid cursor" });
      }
    }
    const rows = await prisma.conversation.findMany({
      where,
      orderBy: [{ lastMessageAt: "desc" }, { id: "desc" }],
      take: q.data.limit + 1,
      include: { contact: true },
    });
    const more = rows.length > q.data.limit,
      items = rows.slice(0, q.data.limit),
      last = items.at(-1);
    return {
      items,
      nextCursor:
        more && last
          ? Buffer.from(
              JSON.stringify({
                at:
                  last.lastMessageAt?.toISOString() ??
                  "1970-01-01T00:00:00.000Z",
                id: last.id,
              }),
            ).toString("base64url")
          : null,
    };
  },
);

// get messages for a conversation, cursor-based pagination
app.get("/conversations/:id/messages", async (req: any, reply: any) => {
  const loc = locHeader(req);
  if (!loc) return reply.code(401).send({ error: "X-Location-Id required" });
  const p = z.object({ id: z.string().uuid() }).safeParse(req.params),
    q = z
      .object({
        limit: z.coerce.number().int().min(1).max(100).default(20),
        cursor: z.string().optional(),
      })
      .safeParse(req.query);
  if (!p.success || !q.success)
    return reply.code(400).send({ error: "Invalid request" });
  const conv = await prisma.conversation.findFirst({
    where: { id: p.data.id, locationId: loc },
  });
  if (!conv) return reply.code(404).send({ error: "Conversation not found" });
  const where: any = { conversationId: conv.id, locationId: loc };
  if (q.data.cursor) {
    try {
      const c = JSON.parse(Buffer.from(q.data.cursor, "base64url").toString());
      where.OR = [
        { sentAt: { gt: new Date(c.at) } },
        { sentAt: new Date(c.at), id: { gt: c.id } },
      ];
    } catch {
      return reply.code(400).send({ error: "Invalid cursor" });
    }
  }
  const rows = await prisma.message.findMany({
    where,
    orderBy: [{ sentAt: "asc" }, { id: "asc" }],
    take: q.data.limit + 1,
  });
  const more = rows.length > q.data.limit,
    items = rows.slice(0, q.data.limit),
    last = items.at(-1);
  return {
    items,
    nextCursor:
      more && last
        ? Buffer.from(
            JSON.stringify({ at: last.sentAt.toISOString(), id: last.id }),
          ).toString("base64url")
        : null,
  };
});

// mark a conversation as read (unreadCount=0)
app.post("/conversations/:id/read", async (req: any, reply: any) => {
  const loc = locHeader(req);
  if (!loc) return reply.code(401).send({ error: "X-Location-Id required" });
  const p = z.object({ id: z.string().uuid() }).safeParse(req.params);
  if (!p.success) return reply.code(400).send({ error: "Invalid id" });
  const r = await prisma.conversation.updateMany({
    where: { id: p.data.id, locationId: loc },
    data: { unreadCount: 0 },
  });
  return r.count
    ? { success: true }
    : reply.code(404).send({ error: "Conversation not found" });
});

// update a conversation's assignedTo and/or status
app.patch("/conversations/:id", async (req: any, reply: any) => {
  const loc = locHeader(req);
  if (!loc) return reply.code(401).send({ error: "X-Location-Id required" });
  const p = z.object({ id: z.string().uuid() }).safeParse(req.params),
    b = z
      .object({
        assignedTo: z.string().nullable().optional(),
        status: z.enum(["open", "closed"]).optional(),
      })
      .refine((x) => Object.keys(x).length > 0)
      .safeParse(req.body);
  if (!p.success || !b.success)
    return reply.code(400).send({ error: "Invalid request" });
  const r = await prisma.conversation.updateMany({
    where: { id: p.data.id, locationId: loc },
    data: {
      ...(b.data.assignedTo !== undefined
        ? { assignedTo: b.data.assignedTo }
        : {}),
      ...(b.data.status ? { status: b.data.status as ConversationStatus } : {}),
    },
  });
  return r.count
    ? { success: true }
    : reply.code(404).send({ error: "Conversation not found" });
});

// SSE: simple single-instance dispatcher. Production/multi-instance should use a shared broker.
app.get("/api/stream", async (req: any, reply: any) => {
  const loc = locHeader(req);
  if (!loc) return reply.code(401).send({ error: "X-Location-Id required" });
  reply.raw.writeHead(200, {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    Connection: "keep-alive",
  });
  reply.raw.write(
    `event: ready\ndata: ${JSON.stringify({ locationId: loc })}\n\n`,
  );
  let busy = false;
  const timer = setInterval(async () => {
    if (busy) return;
    busy = true;
    try {
      const events = await prisma.outboxEvent.findMany({
        where: { locationId: loc, publishedAt: null },
        orderBy: { createdAt: "asc" },
        take: 20,
      });
      for (const e of events) {
        reply.raw.write(
          `id: ${e.id}\nevent: ${e.eventType}\ndata: ${JSON.stringify(e.payload)}\n\n`,
        );
        await prisma.outboxEvent.update({
          where: { id: e.id },
          data: { publishedAt: new Date() },
        });
      }
    } catch (reqErr) {
      req.log.error(reqErr);
    } finally {
      busy = false;
    }
  }, 1000);
  req.raw.on("close", () => clearInterval(timer));
  return reply;
});

const port = Number(process.env.PORT ?? 3000),
  host = process.env.HOST ?? "0.0.0.0";
app.listen({ port, host }).catch((err) => {
  app.log.error(err);
  process.exit(1);
});
process.on("SIGINT", async () => {
  clearInterval(worker);
  await app.close();
  await prisma.$disconnect();
  process.exit(0);
});
