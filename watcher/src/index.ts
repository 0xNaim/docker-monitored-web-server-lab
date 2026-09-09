import checkHealth = require("./health-checker");

const TARGET_URL = "http://nginx/health";

const CHECK_INTERVAL_MS = 5000;
const TIMEOUT_MS = 3000;

const FAILURE_THRESHOLD = 3;
const RECOVERY_THRESHOLD = 2;

let consecutiveFailures = 0;
let consecutiveSuccesses = 0;

let isDown = false;

async function monitor() {
	const result = await checkHealth(TARGET_URL, TIMEOUT_MS);

	const timestamp = new Date().toISOString();

	if (result.healthy) {
		consecutiveFailures = 0;
		consecutiveSuccesses++;

		console.log(`[${timestamp}] UP | status=${result.statusCode} latency=${result.latencyMs}ms`);

		if (isDown && consecutiveSuccesses >= RECOVERY_THRESHOLD) {
			isDown = false;

			console.log(`[${timestamp}] RECOVERED | service is healthy again`);

			consecutiveSuccesses = 0;
		}

		return;
	}

	consecutiveSuccesses = 0;
	consecutiveFailures++;

	console.log(`[${timestamp}] DOWN | failure=${consecutiveFailures}/${FAILURE_THRESHOLD}`);

	if (!isDown && consecutiveFailures >= FAILURE_THRESHOLD) {
		isDown = true;

		console.log(`[${timestamp}] CONFIRMED DOWN`);

		consecutiveFailures = 0;
	}
}

setInterval(monitor, CHECK_INTERVAL_MS);

monitor();
