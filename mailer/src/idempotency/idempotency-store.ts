import { createClient } from "redis";

const redis = createClient({
	url: "redis://redis:6379"
});

redis.on("error", (error) => {
	console.error("[Redis] Idempotency error:", error);
});

const PROCESSED_TTL_SECONDS = 86400;
const PROCESSING_TTL_SECONDS = 300;

export async function connectIdempotencyStore(): Promise<void> {
	if (!redis.isOpen) {
		await redis.connect();
	}

	console.log("[Redis] Idempotency store connected");
}

export async function getEventStatus(eventId: string): Promise<string | null> {
	return redis.get(`mailer:event:${eventId}`);
}

export async function claimEvent(eventId: string): Promise<boolean> {
	const result = await redis.set(`mailer:event:${eventId}`, "PROCESSING", {
		NX: true,
		EX: PROCESSING_TTL_SECONDS
	});

	return result === "OK";
}

export async function markEventProcessed(eventId: string): Promise<void> {
	await redis.set(`mailer:event:${eventId}`, "PROCESSED", {
		EX: PROCESSED_TTL_SECONDS
	});
}

export async function releaseEvent(eventId: string): Promise<void> {
	await redis.del(`mailer:event:${eventId}`);
}
