import http from "node:http";

const PORT = 3000;

let rabbitMQHealthy = false;

export function setRabbitMQHealth(healthy: boolean): void {
	rabbitMQHealthy = healthy;
}

export function startHealthServer(): void {
	const server = http.createServer((req, res) => {
		if (req.method !== "GET" || req.url !== "/health") {
			res.statusCode = 404;

			res.end("Not Found");

			return;
		}

		const healthy = rabbitMQHealthy;

		res.statusCode = healthy ? 200 : 503;

		res.setHeader("Content-Type", "application/json");

		res.end(
			JSON.stringify({
				status: healthy ? "ok" : "unhealthy",
				service: "mailer",
				rabbitmq: rabbitMQHealthy ? "up" : "down"
			})
		);
	});

	server.listen(PORT, () => {
		console.log(`[Health] Server listening on ${PORT}`);
	});
}
