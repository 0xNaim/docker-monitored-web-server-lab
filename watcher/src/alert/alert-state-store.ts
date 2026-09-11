import { redis } from "../redis/redis-client";
import { AlertState } from "./alert-state";

export class AlertStateStore {
	constructor(private readonly serviceName: string) {}

	private get key(): string {
		return `alert:state:${this.serviceName}`;
	}

	async getState(): Promise<AlertState> {
		const state = await redis.get(this.key);

		if (!state) {
			return AlertState.NOT_ALERTED;
		}

		return state as AlertState;
	}

	async setState(state: AlertState): Promise<void> {
		await redis.set(this.key, state);
	}
}
