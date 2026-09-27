export type SseConnectionStatus = 'connecting' | 'connected' | 'reconnecting' | 'disconnected';

export interface StreamExecutionEvent {
  eventType: string;
  machineName?: string;
  machineId?: string;
  timestamp: string;
  stateName?: string;
  sourceState?: string;
  targetState?: string;
  correlationKey?: string;
  signalName?: string;
  expectedSignal?: string;
  actionName?: string;
  finalStateName?: string;
  failedStateName?: string;
  errorMessage?: string;
  errorType?: string;
  duration?: string;
  durationMillis?: number;
  rawJson?: string;
}
