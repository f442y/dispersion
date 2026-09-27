import { request } from './client';
import type {
  ExecutionSummary,
  ListExecutionsFilter,
  SignalRequest,
  SignalDeliveryResult,
  StreamExecutionEvent,
} from '../types';

export const executionsApi = {
  async listExecutions(params?: ListExecutionsFilter): Promise<ExecutionSummary[]> {
    const query = new URLSearchParams();
    if (params?.machine) query.set('machine', params.machine);
    if (params?.status) query.set('status', params.status);
    if (params?.limit) query.set('limit', String(params.limit));

    const qs = query.toString();
    const path = `/executions${qs ? `?${qs}` : ''}`;
    return request<ExecutionSummary[]>(path, { method: 'GET' }, params?.customBase);
  },

  async getExecution(id: string, customBase?: string): Promise<ExecutionSummary> {
    return request<ExecutionSummary>(
      `/executions/${encodeURIComponent(id)}`,
      { method: 'GET' },
      customBase
    );
  },

  async getExecutionTimeline(id: string, customBase?: string): Promise<StreamExecutionEvent[]> {
    return request<StreamExecutionEvent[]>(
      `/executions/${encodeURIComponent(id)}/timeline`,
      { method: 'GET' },
      customBase
    );
  },

  async sendSignal(req: SignalRequest, customBase?: string): Promise<SignalDeliveryResult> {
    return request<SignalDeliveryResult>(
      '/executions/signal',
      {
        method: 'POST',
        body: JSON.stringify(req),
      },
      customBase
    );
  },
};
