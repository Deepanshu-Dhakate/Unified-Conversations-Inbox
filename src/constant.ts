
import { z } from "zod";

export const channel = z.enum(["sms", "whatsapp", "email"]);

export const inboundSchema = z.object({
  providerMessageId: z.string().min(1),
  locationId: z.string().min(1),
  channel,
  from: z.string().min(1),
  to: z.string().min(1),
  body: z.string(),
  sentAt: z.string().datetime(),
});

export const statusSchema = z.object({
  providerMessageId: z.string().min(1),
  status: z.enum(["sent", "delivered", "read", "failed"]),
  timestamp: z.string().datetime(),
});

export const rank: Record<string, number> = {
  queued: 0,
  sent: 1,
  delivered: 2,
  read: 3,
  failed: -1,
};
