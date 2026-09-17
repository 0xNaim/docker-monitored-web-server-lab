import { createClient } from "redis";

export const redis = createClient({
	url: "redis://redis:6379"
});

redis.on("error", (error) => {
	console.error("Redis error: ", error);
});

export async function connectRedis(): Promise<void> {
	await redis.connect();
	console.log("Redis connected");
}

export async function publishAlert(event: unknown): Promise<void> {
	const payload = JSON.stringify(event);

	const length = await redis.lPush("alert:queue", payload);

	console.log(`[Redis] Alert published. Queue length: ${length}`);
}
