import { useEffect, useRef, useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { getEventStreamUrl } from '../api/client';
import { executionKeys } from './queryKeys';
import type { StreamExecutionEvent, SseConnectionStatus } from '../types';

export interface UseEventStreamOptions {
  machineName?: string;
  baseUrl?: string;
  maxEvents?: number;
  enabled?: boolean;
}

export function useEventStream(options: UseEventStreamOptions = {}) {
  const { machineName, baseUrl, maxEvents = 40, enabled = true } = options;
  const queryClient = useQueryClient();
  const [events, setEvents] = useState<StreamExecutionEvent[]>([]);
  const [status, setStatus] = useState<SseConnectionStatus>('connecting');
  const [lastEventTime, setLastEventTime] = useState<Date | null>(null);
  const eventSourceRef = useRef<EventSource | null>(null);

  useEffect(() => {
    if (!enabled) {
      setStatus('disconnected');
      return;
    }

    setStatus('connecting');
    const url = getEventStreamUrl(machineName, baseUrl);
    const es = new EventSource(url);
    eventSourceRef.current = es;

    es.onopen = () => {
      setStatus('connected');
    };

    es.onerror = () => {
      if (es.readyState === EventSource.CONNECTING) {
        setStatus('reconnecting');
      } else {
        setStatus('disconnected');
      }
    };

    // All 28 polymorphic event types emitted by Dispersion Control Plane
    const eventTypes = [
      'message',
      'StateEnteredEvent',
      'StateExitedEvent',
      'TurnStartedEvent',
      'TurnCompletedEvent',
      'TurnSuspendedEvent',
      'TurnCompensatedEvent',
      'TurnFailedEvent',
      'TransitionEvaluatedEvent',
      'ActionExecutedEvent',
      'SignalAwaitedEvent',
      'SignalDeliveredEvent',
      'SignalDiscardedEvent',
      'SignalTimedOutEvent',
      'ExecutionPausedEvent',
      'ExecutionResumedEvent',
      'ExecutionCancelledEvent',
      'ChildMachineSpawnedEvent',
      'ChildMachineCompletedEvent',
      'CompensationStepStartedEvent',
      'CompensationStepCompletedEvent',
      'CompensationStepFailedEvent',
      'RetryAttemptedEvent',
      'RetryExhaustedEvent',
      'CircuitBreakerTrippedEvent',
      'StateVisitLimitExceededEvent',
      'ParallelForkStartedEvent',
      'ParallelBranchCompletedEvent',
      'ParallelJoinCompletedEvent',
    ];

    const handleEvent = (event: MessageEvent) => {
      try {
        const raw = JSON.parse(event.data);
        const parsed: StreamExecutionEvent = {
          eventType: event.type === 'message' ? raw.eventType ?? 'Unknown' : event.type,
          machineName: raw.machineName,
          machineId: raw.machineId ?? raw.executionId,
          timestamp: raw.timestamp ? String(raw.timestamp) : new Date().toISOString(),
          stateName: raw.stateName ?? raw.toState ?? raw.state,
          sourceState: raw.sourceState ?? raw.fromState,
          targetState: raw.targetState ?? raw.toState,
          correlationKey: raw.correlationKey,
          signalName: raw.signalName,
          expectedSignal: raw.expectedSignal,
          actionName: raw.actionName,
          finalStateName: raw.finalStateName,
          failedStateName: raw.failedStateName,
          errorMessage: raw.errorMessage,
          errorType: raw.errorType,
          durationMillis: raw.durationMillis,
          rawJson: JSON.stringify(raw),
        };

        setEvents((prev) => [parsed, ...prev.slice(0, maxEvents - 1)]);
        setLastEventTime(new Date());

        // Instant cache invalidation to trigger live UI update across all active queries
        queryClient.invalidateQueries({ queryKey: executionKeys.all });
      } catch (err) {
        console.warn('Failed to parse SSE payload:', err);
      }
    };

    for (const t of eventTypes) {
      es.addEventListener(t, handleEvent);
    }

    return () => {
      es.close();
      eventSourceRef.current = null;
    };
  }, [machineName, baseUrl, enabled, maxEvents, queryClient]);

  return {
    events,
    status,
    lastEventTime,
    clearEvents: () => setEvents([]),
  };
}
