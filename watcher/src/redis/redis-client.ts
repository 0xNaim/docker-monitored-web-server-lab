import { createClient } from "redis";

export const redis = createClient({
	url: "redis://redis:6379"
});

redis.on("error", (error) => {
	console.error("Redis error: ", error);
});

export async function connectRedis() {
	await redis.connect();
	console.log("Redis connected");
}
