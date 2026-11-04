import { createHash } from "node:crypto";

export const phone = (v: string) => v.replace(/[^\d+]/g, "").replace(/^00/, "+");
export const email = (v: string) => v.trim().toLowerCase();
export const preview = (v: string) => v.slice(0, 120);

export const advisoryKey = (v: string) =>
  BigInt("0x" + createHash("sha256").update(v).digest("hex").slice(0, 15));
export const locHeader = (req: any) =>
  typeof req.headers["x-location-id"] === "string"
    ? req.headers["x-location-id"]
    : undefined;