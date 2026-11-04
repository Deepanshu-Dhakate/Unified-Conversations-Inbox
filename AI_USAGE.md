# AI Usage

## 1. AI Tools Used

I used the following AI tools during the development of this project:

### ChatGPT
- Used for discussing system architecture and implementation approaches.
- Helped reason about concurrency, idempotency, webhook processing, and message status handling.
- Assisted in designing integration test cases for edge cases.
- Used for debugging and understanding race conditions in concurrent operations.

### Gemini
- Used for researching alternative implementation approaches.
- Helped cross-check technical decisions and explore potential improvements.

AI-generated suggestions were reviewed and adapted before being incorporated into the project.

## 2. Example of AI-Generated Code That Needed Changes

### Problem: Race condition in unread count updates

During implementation of inbound message processing, an initial AI-generated approach suggested reading the current unread count, incrementing it in application code, and updating the conversation.

Example of the problematic approach:

```typescript
const conversation = await tx.conversation.findUnique({
  where: { id: conversationId },
});

await tx.conversation.update({
  where: { id: conversationId },
  data: {
    unreadCount: conversation.unreadCount + 1,
  },
});
```

### Why was this incorrect?

When multiple inbound webhooks arrive simultaneously, multiple requests can read the same unread count before any of them updates it.

For example, if the unread count is initially 10:

- Request A reads 10.
- Request B reads 10.
- Request A updates it to 11.
- Request B also updates it to 11.

The expected count is 12, but the actual count becomes 11.

This results in lost updates due to a race condition.

### How I identified the issue

While reviewing the concurrent webhook processing logic, I identified that reading and updating the counter separately was not safe under concurrent requests.

I also added an integration test that sends 50 inbound messages concurrently to the same conversation and verifies that the final unread count is exactly 50.

### How I fixed it

I replaced the read-modify-write approach with Prisma's atomic increment operation:

```typescript
await tx.conversation.update({
  where: { id: conversationId },
  data: {
    unreadCount: {
      increment: 1,
    },
  },
});
```

Additionally, the increment is performed only when a new message is successfully inserted. Duplicate inbound webhooks are ignored using the provider message ID as the idempotency key.

This ensures that:
- Concurrent requests do not overwrite each other's increments.
- Duplicate webhooks do not increase the unread count.
- The unread count remains consistent with newly received messages.

## 3. Other Areas Where AI Suggestions Were Reviewed

AI assistance was also used while working on:

- **Webhook idempotency:** Ensuring duplicate inbound events do not create duplicate messages.
- **Out-of-order status callbacks:** Handling delivery-status events that arrive before the corresponding message is associated with its provider message ID.
- **Message pagination:** Using cursor-based pagination to avoid duplicate or missing messages when new messages arrive between page requests.
- **Outbound processing:** Reviewing job persistence, retry handling, and PostgreSQL row locking with `FOR UPDATE SKIP LOCKED`.
- **Real-time updates:** Reviewing SSE event dispatching and ensuring events are published after successful database transactions.

## 4. Validation and Responsibility

AI-generated code was not accepted blindly. I reviewed the suggestions, adapted them to the existing codebase, and validated important behavior through automated tests.

The final implementation and technical decisions are my responsibility. AI tools were used to improve development speed, explore solutions, and identify potential edge cases rather than replace engineering judgment.


