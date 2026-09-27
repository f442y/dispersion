import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { executionsApi } from '../api/executions.api';
import { executionKeys } from './queryKeys';
import type {
  ExecutionSummary,
  ListExecutionsFilter,
  StreamExecutionEvent,
  SignalRequest,
} from '../types';

export function useExecutionsQuery(
  nodeId?: string,
  filter?: ListExecutionsFilter,
  enabled: boolean = true,
  refetchInterval: number | false = 3000
) {
  return useQuery<ExecutionSummary[]>({
    queryKey: executionKeys.list(nodeId, filter),
    queryFn: () => executionsApi.listExecutions(filter),
    enabled,
    refetchInterval,
  });
}

export function useExecutionQuery(id?: string, enabled: boolean = true) {
  return useQuery<ExecutionSummary>({
    queryKey: executionKeys.detail(id ?? ''),
    queryFn: () => (id ? executionsApi.getExecution(id) : Promise.reject('No ID')),
    enabled: enabled && !!id,
  });
}

export function useExecutionTimelineQuery(
  id?: string,
  enabled: boolean = true,
  refetchInterval: number | false = false
) {
  return useQuery<StreamExecutionEvent[]>({
    queryKey: executionKeys.timeline(id ?? ''),
    queryFn: () => (id ? executionsApi.getExecutionTimeline(id) : Promise.resolve([])),
    enabled: enabled && !!id,
    refetchInterval,
  });
}

export function useSendSignalMutation() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (req: SignalRequest) => executionsApi.sendSignal(req),
    onSuccess: () => {
      // Invalidate executions and any timeline queries
      queryClient.invalidateQueries({ queryKey: executionKeys.all });
    },
  });
}
