import { AlertState } from "./alert-state";
import { AlertStateStore } from "./alert-state-store";

export interface AlertEvent {
	eventId: string;
	service: string;
	status: "DOWN" | "RECOVERED";
	timestamp: string;
	message: string;
	retryCount: number;
}

export class AlertManager {
	private readonly stateStore: AlertStateStore;

	constructor(public readonly serviceName: string) {
		this.stateStore = new AlertStateStore(serviceName);
	}

	async handleDown(): Promise<AlertEvent | null> {
		const state = await this.stateStore.getState();

		if (state === AlertState.ALERTED) {
			return null;
		}

		await this.stateStore.setState(AlertState.ALERTED);

		return {
			eventId: crypto.randomUUID(),
			service: this.serviceName,
			status: "DOWN",
			timestamp: new Date().toISOString(),
			message: `${this.serviceName} is down`,
			retryCount: 0
		};
	}

	async handleRecovery(): Promise<AlertEvent | null> {
		const state = await this.stateStore.getState();

		if (state !== AlertState.ALERTED) {
			return null;
		}

		await this.stateStore.setState(AlertState.NOT_ALERTED);

		return {
			eventId: crypto.randomUUID(),
			service: this.serviceName,
			status: "RECOVERED",
			timestamp: new Date().toISOString(),
			message: `${this.serviceName} has recovered`,
			retryCount: 0
		};
	}
}
