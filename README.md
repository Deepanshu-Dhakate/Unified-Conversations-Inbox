# HighLevel SDE-2 Take-home: Unified Conversations Inbox

Fastify + TypeScript + Prisma + PostgreSQL implementation. This is a take-home scaffold and should be verified locally against a real PostgreSQL/Supabase instance before submission.

## Prerequisites
- Node.js 20 or later, npm
- Supabase PostgreSQL project (or local Postgres)

## Setup
1. Create a Supabase project and copy the database connection URL.
2. Copy `.env.example` to `.env`; fill in `DATABASE_URL`. Keep `.env` private and out of Git.
3. Install/generate/push schema:
   ```bash
   npm install
   npx prisma generate
   npx prisma db push
   ```
4. Start server: `npm run dev`
5. Verify `GET http://localhost:3000/health` returns `{ "ok": true }`.
6. Type check/test: `npm run typecheck && npm test`.

## API routes
- `POST /webhooks/inbound` — inbound SMS/WhatsApp/email webhook; body carries `locationId`.
- `POST /webhooks/status` — provider delivery status callback.
- `POST /conversations/:id/messages` — queue outbound message; requires `X-Location-Id`.
- `GET /locations/:locationId/conversations?status=open&assignedTo=agent&limit=20&cursor=...` — list inbox; header must match path location.
- `GET /conversations/:id/messages?limit=20&cursor=...` — chronological message history; requires location header.
- `POST /conversations/:id/read` — reset unread count; requires location header.
- `PATCH /conversations/:id` — assign/open/close; requires location header.
- `GET /api/stream` — SSE updates filtered by `X-Location-Id`.
- `GET /health`.

## Design / trade-offs
- Location-scoped unique contact identifiers and unique conversation per `(locationId, contactId)`.
- Unique provider message ID plus `createMany(skipDuplicates)` protects inbound deduplication. Unread increment only occurs when insertion succeeded and is atomic in the same transaction.
- `lastMessageAt` conditional SQL update prevents older inbound timestamp from moving conversation activity backward.
- Status callback events are recorded even if their message has not yet arrived; callbacks are serialized by transaction advisory lock when processing. Status ordering is monotonic except a failure event, which is treated as terminal only before delivered/read.
- Outbound jobs are persisted in PostgreSQL and claimed using `FOR UPDATE SKIP LOCKED`; mock provider has ~20% failure chance and retry delays.
- SSE polls the outbox for one-instance demonstration. For multiple app instances, use a shared broker and durable event dispatch/replay. The current polling implementation is not multi-instance safe.
- Cursor paging uses `(timestamp,id)` tie-breakers. Conversation `lastMessageAt` can mutate between page fetches; snapshot-stable conversation list pagination would need an activity feed/snapshot approach.
- `X-Location-Id` is a mock tenant authorization boundary for this assignment, not production auth. Add provider signature verification, auth and rate limiting for real deployment.
