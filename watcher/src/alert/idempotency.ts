import { redis } from "../redis/redis-client";

const IDEMPOTENCY_TTL_SECONDS = 60 * 60 * 24; // 1 day

export async function isEventProcessed(eventId: string): Promise<boolean> {
	const key = `alert:event:${eventId}`;
	const exists = await redis.exists(key);

	return exists === 1;
}

export async function markEventAsProcessed(eventId: string): Promise<void> {
	const key = `alert:event:${eventId}`;

	await redis.set(key, "1", {
		expiration: {
			type: "EX",
			value: IDEMPOTENCY_TTL_SECONDS
		}
	});
}
