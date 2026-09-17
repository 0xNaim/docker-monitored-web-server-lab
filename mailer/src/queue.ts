import { createClient } from "redis";

export const ALERT_QUEUE = "alert:queue";
export const PROCESSING_QUEUE = "alert:processing";
export const PROCESSING_METADATA = "alert:processing:metadata";
export const DLQ_QUEUE = "alert:dlq";

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
	const event = await redis.rPopLPush(queueName, processingQueueName);

	if (!event) {
		return null;
	}

	const parsed = JSON.parse(event) as { eventId: string };

	await redis.hSet(PROCESSING_METADATA, parsed.eventId, Date.now().toString());

	return event;
}

export async function acknowledgeAlert(processingQueueName: string, event: string): Promise<void> {
	await redis.lRem(processingQueueName, 1, event);

	const parsed = JSON.parse(event) as { eventId: string };

	await redis.hDel(PROCESSING_METADATA, parsed.eventId);
}

export async function requeueAlert(
	processingQueueName: string,
	queueName: string
): Promise<string | null> {
	return redis.lMove(processingQueueName, queueName, "LEFT", "LEFT");
}

export async function moveToDeadLetterQueue(
	processingQueueName: string,
	dlqName: string,
	processingEvent: string,
	failedEvent: string
): Promise<void> {
	await redis.lRem(processingQueueName, 1, processingEvent);

	await redis.lPush(dlqName, failedEvent);
}

export async function saveJob(eventId: string, event: string): Promise<void> {
	await redis.hSet(`alert:job:${eventId}`, "payload", event);
}

export async function getJob(eventId: string): Promise<string | null> {
	return redis.hGet(`alert:job:${eventId}`, "payload");
}

export async function deleteJob(eventId: string): Promise<void> {
	await redis.del(`alert:job:${eventId}`);
}
