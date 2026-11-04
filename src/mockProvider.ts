
import { randomUUID } from "crypto";
import { MockSendMessageInput, MockSendMessageResult } from "./interface";

export const mockProvider = {
  async sendMessage(
    input: MockSendMessageInput,
  ): Promise<MockSendMessageResult> {
    // Simulate provider accepting the outbound message
    // await new Promise((resolve) => setTimeout(resolve, 300));

    const providerMessageId = `mock_${input.channel}_${randomUUID()}`;

    console.log("[MockProvider] Message accepted", {
      providerMessageId,
      channel: input.channel,
      body: input.body,
    });

    return {
      providerMessageId,
    };
  },
};
