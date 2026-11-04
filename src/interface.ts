import { Channel } from "@prisma/client";

export interface MockSendMessageInput {
  channel: Channel;
  body: string;
}

export interface MockSendMessageResult {
  providerMessageId: string;
}
