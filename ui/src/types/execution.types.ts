export type ExecutionStatus =
  | 'INITIALIZING'
  | 'RUNNING'
  | 'SUSPENDED'
  | 'COMPENSATING'
  | 'COMPLETED'
  | 'COMPENSATED'
  | 'FAILED'
  | 'CANCELLED';

export interface ExecutionSummary {
  executionId: string;
  machineName: string;
  currentState: string;
  status: ExecutionStatus;
  startTime: string;
  endTime?: string | null;
  correlationKey?: string | null;
  suspendedSignal?: string | null;
  transitionsCount: number;
  lastUpdated: string;
  errorMessage?: string | null;
}

export interface ListExecutionsFilter {
  machine?: string;
  status?: string;
  limit?: number;
  customBase?: string;
}

export interface SignalRequest {
  machineName: string;
  correlationKey: string;
  signalName: string;
  payload?: Record<string, unknown> | null;
}

export interface SignalDeliveryResult {
  delivered: boolean;
  message: string;
  machineName: string;
  correlationKey: string;
  signalName: string;
  completed: boolean;
  suspended: boolean;
  resultingState?: string | null;
  errorMessage?: string | null;
}
