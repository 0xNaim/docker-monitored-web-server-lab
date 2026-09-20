import http from "node:http";
import { getMetrics } from "../metrics/metrics";

const PORT = 3000;

let rabbitMQHealthy = false;

export function setRabbitMQHealth(healthy: boolean): void {
	rabbitMQHealthy = healthy;
}

export function startHealthServer(): void {
	const server = http.createServer((req, res) => {
		if (req.method !== "GET") {
			res.statusCode = 404;
			res.end("Not Found");
			return;
		}

		if (req.url === "/health") {
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

			return;
		}

		if (req.url === "/metrics") {
			const metrics = getMetrics();

			res.statusCode = 200;

			res.setHeader("Content-Type", "application/json");

			res.end(JSON.stringify(metrics));

			return;
		}

		res.statusCode = 404;
		res.end("Not Found");
	});

	server.listen(PORT, () => {
		console.log(`[Health] Server listening on ${PORT}`);
	});
}
