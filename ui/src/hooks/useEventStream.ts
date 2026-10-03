import { useEffect, useRef, useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { getEventStreamUrl } from '../api/client';
import { executionKeys } from './queryKeys';
import type { StreamExecutionEvent, SseConnectionStatus, EventTier } from '../types';

export interface UseEventStreamOptions {
  machineName?: string;
  executionId?: string;
  tier?: 'lifecycle' | 'all';
  baseUrl?: string;
  maxEvents?: number;
  enabled?: boolean;
  onEvent?: (event: StreamExecutionEvent) => void;
}

export function useEventStream(options: UseEventStreamOptions = {}) {
  const { machineName, executionId, tier, baseUrl, maxEvents = 40, enabled = true, onEvent } = options;
  const queryClient = useQueryClient();
  const [events, setEvents] = useState<StreamExecutionEvent[]>([]);
  const [status, setStatus] = useState<SseConnectionStatus>('connecting');
  const [lastEventTime, setLastEventTime] = useState<Date | null>(null);
  const eventSourceRef = useRef<EventSource | null>(null);
  const onEventRef = useRef(onEvent);
  onEventRef.current = onEvent;

  useEffect(() => {
    if (!enabled) {
      setStatus('disconnected');
      return;
    }

    setStatus('connecting');
    const url = getEventStreamUrl({ machineName, executionId, tier }, baseUrl);
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

    const handleEvent = (event: MessageEvent) => {
      try {
        const raw = JSON.parse(event.data);
        const parsed: StreamExecutionEvent = {
          eventType: raw.eventType ?? (event.type === 'message' ? 'Unknown' : event.type),
          tier: (raw.tier as EventTier) ?? undefined,
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

        if (onEventRef.current) {
          onEventRef.current(parsed);
        }

        // Cache invalidation: targeted when executionId is provided, global otherwise
        if (executionId) {
          queryClient.invalidateQueries({ queryKey: executionKeys.detail(executionId) });
          queryClient.invalidateQueries({ queryKey: executionKeys.timeline(executionId) });
        } else {
          queryClient.invalidateQueries({ queryKey: executionKeys.all });
        }
      } catch (err) {
        console.warn('Failed to parse SSE payload:', err);
      }
    };

    es.onmessage = handleEvent;

    return () => {
      es.close();
      eventSourceRef.current = null;
    };
  }, [machineName, executionId, tier, baseUrl, enabled, maxEvents, queryClient]);

  return {
    events,
    status,
    lastEventTime,
    clearEvents: () => setEvents([]),
  };
}
