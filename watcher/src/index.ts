import { AlertManager } from "./alert/alert-manager";
import { checkHealth } from "./monitor/health-checker";

const TARGET_URL = "http://nginx/health";

const CHECK_INTERVAL_MS = 5000;
const TIMEOUT_MS = 3000;

const FAILURE_THRESHOLD = 3;
const RECOVERY_THRESHOLD = 2;

let consecutiveFailures = 0;
let consecutiveSuccesses = 0;

const alertManager = new AlertManager("nginx");

async function monitor() {
	const result = await checkHealth(TARGET_URL, TIMEOUT_MS);

	const timestamp = new Date().toISOString();

	if (result.healthy) {
		consecutiveFailures = 0;
		consecutiveSuccesses++;

		console.log(`[${timestamp}] UP | status=${result.statusCode} latency=${result.latencyMs}ms`);

		if (consecutiveSuccesses >= RECOVERY_THRESHOLD) {
			const event = alertManager.handleRecovery();

			if (event) {
				console.log("RECOVERY ALERT:", event);
			}

			consecutiveSuccesses = 0;
		}

		return;
	}

	consecutiveSuccesses = 0;
	consecutiveFailures++;

	console.log(`[${timestamp}] DOWN | failure=${consecutiveFailures}/${FAILURE_THRESHOLD}`);

	if (consecutiveFailures >= FAILURE_THRESHOLD) {
		const event = alertManager.handleDown();

		if (event) {
			console.log("DOWN ALERT:", event);
		}

		consecutiveFailures = 0;
	}
}

setInterval(monitor, CHECK_INTERVAL_MS);

monitor();
