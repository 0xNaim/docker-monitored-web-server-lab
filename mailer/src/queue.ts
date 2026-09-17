import { createClient } from "redis";

export const redis = createClient({
	url: "redis://redis:6379"
});

redis.on("error", (error) => {
	console.error("[Redis] Error:", error);
});

export async function connectRedis(): Promise<void> {
	await redis.connect();

	console.log("[Redis] Mailer connected");
}

export async function claimAlert(
	queueName: string,
	processingQueueName: string
): Promise<string | null> {
	return redis.rPopLPush(queueName, processingQueueName);
}

export async function acknowledgeAlert(processingQueueName: string, event: string): Promise<void> {
	await redis.lRem(processingQueueName, 1, event);
}
