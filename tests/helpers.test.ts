import { PrismaClient, MessageStatus } from "@prisma/client";
import { afterEach, describe, expect, it } from "vitest";
import { randomUUID } from "node:crypto";

/**
 * Integration tests for the GHL Unified Inbox assignment.
 *
 * Run the API server separately (`npm run dev`) and set DATABASE_URL to a
 * dedicated test database. Optional: set TEST_BASE_URL (defaults to
 * http://localhost:3000).
 *
 * These tests use the real Fastify HTTP endpoints and PostgreSQL.
 */

const prisma = new PrismaClient();
const BASE_URL = process.env.TEST_BASE_URL ?? "http://localhost:3000";

type InboundPayload = {
  providerMessageId: string;
  locationId: string;
  channel: "sms" | "whatsapp" | "email";
  from: string;
  to: string;
  body: string;
  sentAt: string;
};

const testLocationId = () => `test-location-${randomUUID()}`;
const testProviderMessageId = (locationId: string) =>
  `test_${locationId}_${randomUUID()}`;

async function postJson(path: string, body: unknown) {
  const response = await fetch(`${BASE_URL}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });

  const responseBody = await response.json().catch(() => null);
  return { response, body: responseBody };
}

async function postInbound(payload: InboundPayload) {
  return postJson("/webhooks/inbound", payload);
}

async function postStatus(
  providerMessageId: string,
  status: "sent" | "delivered" | "read" | "failed",
  timestamp: string,
) {
  return postJson("/webhooks/status", {
    providerMessageId,
    status,
    timestamp,
  });
}

async function cleanupLocation(locationId: string) {
  // Status events have no cascading delete from Message in the current schema.
  await prisma.messageStatusEvent.deleteMany({
    where: { providerMessageId: { startsWith: `test_${locationId}_` } },
  });

  // Contact -> Conversation -> Message uses cascading deletes in this schema.
  await prisma.contact.deleteMany({ where: { locationId } });
  await prisma.outboxEvent.deleteMany({ where: { locationId } });
}

describe("GHL Unified Inbox assignment integration tests", () => {
  const locationsToCleanup = new Set<string>();

  afterEach(async () => {
    for (const locationId of locationsToCleanup) {
      await cleanupLocation(locationId);
    }
    locationsToCleanup.clear();
  });

  it("stores a duplicate inbound webhook only once", async () => {
    const locationId = testLocationId();
    locationsToCleanup.add(locationId);
    const providerMessageId = testProviderMessageId(locationId);

    const payload: InboundPayload = {
      providerMessageId,
      locationId,
      channel: "whatsapp",
      from: "+15550001001",
      to: "+15550002000",
      body: "Duplicate webhook test",
      sentAt: "2026-11-04T10:00:00.000Z",
    };

    const first = await postInbound(payload);
    const second = await postInbound(payload);

    expect(first.response.status).toBe(201);
    expect(second.response.status).toBe(200);
    expect(second.body).toMatchObject({ duplicate: true });

    const storedCount = await prisma.message.count({
      where: { providerMessageId },
    });
    expect(storedCount).toBe(1);

    const conversation = await prisma.conversation.findUnique({
      where: { id: first.body.conversationId },
    });
    expect(conversation?.unreadCount).toBe(1);
  }, 30000);

  it("reconciles out-of-order status callbacks to the correct final status", async () => {
    const locationId = testLocationId();
    locationsToCleanup.add(locationId);
    const providerMessageId = testProviderMessageId(locationId);

    // Provider callbacks arrive before the inbound message is persisted,
    // and arrive in reverse timestamp order.
    const delivered = await postStatus(
      providerMessageId,
      "delivered",
      "2026-11-04T10:00:02.000Z",
    );
    const sent = await postStatus(
      providerMessageId,
      "sent",
      "2026-11-04T10:00:01.000Z",
    );

    expect(delivered.response.status).toBe(202);
    expect(delivered.body).toMatchObject({ pending: true });
    expect(sent.response.status).toBe(202);
    expect(sent.body).toMatchObject({ pending: true });

    const inbound = await postInbound({
      providerMessageId,
      locationId,
      channel: "sms",
      from: "+15550001002",
      to: "+15550002000",
      body: "Status reconciliation test",
      sentAt: "2026-11-04T10:00:00.000Z",
    });

    expect(inbound.response.status).toBe(201);

    const message = await prisma.message.findUnique({
      where: { providerMessageId },
    });
    expect(message?.status).toBe(MessageStatus.delivered);

    const statusEvents = await prisma.messageStatusEvent.findMany({
      where: { providerMessageId },
    });
    expect(statusEvents).toHaveLength(2);
    expect(statusEvents.every((event) => event.applied)).toBe(true);
  }, 30000);

  it("keeps message cursor pagination correct when new messages arrive between page requests", async () => {
    const locationId = testLocationId();
    locationsToCleanup.add(locationId);
    const from = "+15550001004";
    const to = "+15550002000";
    const initialIds: string[] = [];

    // Seed five chronologically ordered messages in the same conversation.
    for (let index = 0; index < 5; index += 1) {
      const providerMessageId = testProviderMessageId(locationId);
      initialIds.push(providerMessageId);

      const result = await postInbound({
        providerMessageId,
        locationId,
        channel: "sms",
        from,
        to,
        body: `Initial message ${index + 1}`,
        sentAt: new Date(
          Date.parse("2026-11-04T12:00:00.000Z") + index * 1000,
        ).toISOString(),
      });

      expect(result.response.status).toBe(201);
    }

    const conversation = await prisma.conversation.findFirst({
      where: { locationId },
    });
    expect(conversation).not.toBeNull();

    const firstPageResponse = await fetch(
      `${BASE_URL}/conversations/${conversation!.id}/messages?limit=2`,
      { headers: { "X-Location-Id": locationId } },
    );
    expect(firstPageResponse.status).toBe(200);

    const firstPage = (await firstPageResponse.json()) as {
      items: Array<{ id: string; providerMessageId: string; sentAt: string }>;
      nextCursor: string | null;
    };

    expect(firstPage.items).toHaveLength(2);
    expect(firstPage.nextCursor).not.toBeNull();

    // Insert new messages after page 1 has been fetched.
    const newIds: string[] = [];
    for (let index = 0; index < 2; index += 1) {
      const providerMessageId = testProviderMessageId(locationId);
      newIds.push(providerMessageId);

      const result = await postInbound({
        providerMessageId,
        locationId,
        channel: "sms",
        from,
        to,
        body: `Inserted between pages ${index + 1}`,
        sentAt: new Date(
          Date.parse("2026-11-04T12:00:10.000Z") + index * 1000,
        ).toISOString(),
      });

      expect(result.response.status).toBe(201);
    }

    const collected = [...firstPage.items];
    let cursor = firstPage.nextCursor;

    while (cursor) {
      const url = new URL(
        `${BASE_URL}/conversations/${conversation!.id}/messages`,
      );
      url.searchParams.set("limit", "2");
      url.searchParams.set("cursor", cursor);

      const response = await fetch(url, {
        headers: { "X-Location-Id": locationId },
      });
      expect(response.status).toBe(200);

      const page = (await response.json()) as {
        items: Array<{ id: string; providerMessageId: string; sentAt: string }>;
        nextCursor: string | null;
      };

      collected.push(...page.items);
      cursor = page.nextCursor;
    }

    const ids = collected.map((message) => message.id);
    expect(new Set(ids).size).toBe(ids.length); // no duplicates

    const providerIds = new Set(
      collected.map((message) => message.providerMessageId),
    );
    for (const initialId of initialIds) {
      expect(providerIds.has(initialId)).toBe(true); // no skipped original rows
    }

    for (let index = 1; index < collected.length; index += 1) {
      expect(
        new Date(collected[index - 1].sentAt).getTime(),
      ).toBeLessThanOrEqual(new Date(collected[index].sentAt).getTime());
    }

    // A live keyset cursor may include none, some, or all newly inserted rows.
    // The essential correctness properties are no duplicates, no skipped
    // original messages, and stable chronological ordering.
    expect(collected.length).toBeGreaterThanOrEqual(initialIds.length);
    expect(collected.length).toBeLessThanOrEqual(
      initialIds.length + newIds.length,
    );
  }, 30000);
});
