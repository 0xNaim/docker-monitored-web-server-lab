import axios = require("axios");

interface HealthCheckResult {
	healthy: boolean;
	latencyMs: number;
	statusCode?: number;
	error?: string;
}

async function checkHealth(url: string, timeoutMs: number): Promise<HealthCheckResult> {
	const startedAt = Date.now();

	try {
		const response = await axios.get(url, {
			timeout: timeoutMs,
			validateStatus: () => true
		});

		return {
			healthy: response.status >= 200 && response.status < 300,
			latencyMs: Date.now() - startedAt,
			statusCode: response.status
		};
	} catch (error) {
		return {
			healthy: false,
			latencyMs: Date.now() - startedAt,
			error: error instanceof Error ? error.message : "Unknown error"
		};
	}
}

export = checkHealth;

