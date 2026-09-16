export interface RetryOptions {
	maxAttempts: number;
	baseDelayMs: number;
}

function sleep(ms: number): Promise<void> {
	return new Promise((resolve) => {
		setTimeout(resolve, ms);
	});
}

export async function withRetry<T>(operation: () => Promise<T>, options: RetryOptions): Promise<T> {
	let lastError: unknown;

	for (let attempt = 1; attempt <= options.maxAttempts; attempt++) {
		try {
			return await operation();
		} catch (error) {
			lastError = error;

			if (attempt === options.maxAttempts) {
				break;
			}

			const delay = options.baseDelayMs * 2 ** (attempt - 1);

			console.error(`Attempt ${attempt} failed. Retrying in ${delay} ms...`);

			await sleep(delay);
		}
	}

	throw lastError;
}
