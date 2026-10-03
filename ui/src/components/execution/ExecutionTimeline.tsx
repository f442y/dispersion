import { useState, useEffect, useMemo } from 'react';
import { Radio, RefreshCw, Activity, Eye, EyeOff } from 'lucide-react';
import { useExecutionTimelineQuery, useEventStream } from '../../hooks';
import { formatTime } from '../../utils';
import type { StreamExecutionEvent } from '../../types';

interface ExecutionTimelineProps {
  executionId: string;
  isLive: boolean;
}

const MAX_LIVE_EVENTS = 200;

function getEventKey(evt: StreamExecutionEvent): string {
  return `${evt.timestamp}_${evt.eventType}_${evt.stateName ?? ''}_${evt.actionName ?? ''}_${evt.signalName ?? ''}_${evt.sourceState ?? ''}_${evt.targetState ?? ''}_${evt.durationMillis ?? ''}`;
}

export function ExecutionTimeline({ executionId, isLive }: ExecutionTimelineProps) {
  const [watchLive, setWatchLive] = useState(isLive);
  const [liveEvents, setLiveEvents] = useState<StreamExecutionEvent[]>([]);

  // Historical timeline query
  const {
    data: historicalEvents,
    isLoading,
    refetch,
    isFetching,
  } = useExecutionTimelineQuery(executionId, true, !watchLive && isLive ? 3000 : false);

  // Targeted live SSE stream with dynamic tap lease on Java control plane
  const { status: sseStatus, lastEventTime } = useEventStream({
    executionId,
    tier: 'all',
    enabled: watchLive,
    onEvent: (event) => {
      setLiveEvents((prev) => {
        const next = [...prev, event];
        return next.length > MAX_LIVE_EVENTS ? next.slice(next.length - MAX_LIVE_EVENTS) : next;
      });
    },
  });

  // Reset live events buffer when executionId changes
  useEffect(() => {
    setLiveEvents([]);
    setWatchLive(isLive);
  }, [executionId, isLive]);

  // Combine and deduplicate historical and live-streamed events chronologically
  const allEvents = useMemo(() => {
    const list: StreamExecutionEvent[] = [...(historicalEvents || [])];
    const seen = new Set(list.map(getEventKey));

    for (const evt of liveEvents) {
      const key = getEventKey(evt);
      if (!seen.has(key)) {
        seen.add(key);
        list.push(evt);
      }
    }
    return list;
  }, [historicalEvents, liveEvents]);

  const handleRefresh = () => {
    setLiveEvents([]);
    refetch();
  };

  const granularCount = allEvents.filter(
    (e) => e.tier === 'GRANULAR' || (!e.eventType.startsWith('Turn') && !e.eventType.startsWith('Execution'))
  ).length;

  return (
    <div className="rounded-md border border-zinc-800/80 bg-zinc-900/20 p-3.5 space-y-3">
      {/* Header with Title and Watch Live Controls */}
      <div className="flex items-center justify-between pb-2 border-b border-zinc-800/60 flex-wrap gap-2">
        <div className="flex items-center gap-2">
          <Radio className="h-3.5 w-3.5 text-zinc-400" />
          <h4 className="text-xs font-semibold text-zinc-200 uppercase tracking-wider font-mono">
            Execution Timeline
          </h4>
          {allEvents.length > 0 && (
            <span className="text-[10px] font-mono text-zinc-500 bg-zinc-800/60 px-1.5 py-0.5 rounded border border-zinc-800">
              {allEvents.length} events {granularCount > 0 && `(${granularCount} granular)`}
              {liveEvents.length >= MAX_LIVE_EVENTS && (
                <span className="ml-1 text-amber-400 font-semibold" title={`Live buffer capped at latest ${MAX_LIVE_EVENTS} events`}>
                  (capped {MAX_LIVE_EVENTS})
                </span>
              )}
            </span>
          )}
        </div>

        <div className="flex items-center gap-2">
          {/* Watch Live Toggle (activates Dynamic Tap for this executionId) */}
          <button
            type="button"
            onClick={() => setWatchLive((prev) => !prev)}
            className={`px-2 py-1 rounded text-[11px] font-mono flex items-center gap-1.5 transition cursor-pointer border ${
              watchLive
                ? 'bg-emerald-950/40 text-emerald-300 border-emerald-800/50 hover:bg-emerald-900/40'
                : 'bg-zinc-800/60 text-zinc-400 border-zinc-700/60 hover:text-zinc-200'
            }`}
            title={watchLive ? 'Live tap active: receiving all lifecycle & granular events' : 'Enable live tap subscription'}
          >
            {watchLive ? (
              <>
                <span className="relative flex h-2 w-2">
                  <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75" />
                  <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500" />
                </span>
                <span>Live Tap Active</span>
                <Eye className="h-3 w-3 ml-0.5 text-emerald-400" />
              </>
            ) : (
              <>
                <EyeOff className="h-3 w-3 text-zinc-500" />
                <span>Watch Live</span>
              </>
            )}
          </button>

          {/* Manual Refresh */}
          <button
            type="button"
            onClick={handleRefresh}
            className="text-zinc-500 hover:text-zinc-300 transition p-1 text-[11px] flex items-center gap-1 font-mono cursor-pointer"
            title="Refresh historical timeline"
          >
            <RefreshCw className={`h-3 w-3 ${isFetching ? 'animate-spin' : ''}`} />
          </button>
        </div>
      </div>

      {/* Live Tap Connection Sub-Bar */}
      {watchLive && (
        <div className="flex items-center justify-between text-[10px] font-mono px-2 py-1 rounded bg-zinc-950/60 border border-zinc-800/60 text-zinc-400">
          <div className="flex items-center gap-1.5">
            <Activity className="h-3 w-3 text-emerald-400" />
            <span>SSE Feed: <strong className="text-zinc-300 capitalize">{sseStatus}</strong> (tier=all)</span>
          </div>
          {lastEventTime && (
            <span className="text-zinc-500">
              Latest tick: {formatTime(lastEventTime.toISOString())}
            </span>
          )}
        </div>
      )}

      {/* Timeline Content */}
      {isLoading ? (
        <div className="py-5 text-center text-xs text-zinc-500 font-mono">
          Loading timeline...
        </div>
      ) : allEvents.length === 0 ? (
        <div className="py-5 text-center text-xs text-zinc-500 font-mono border border-dashed border-zinc-800 rounded">
          No recorded transition events for this instance yet.
        </div>
      ) : (
        <div className="relative pl-4 space-y-3.5 before:absolute before:left-1.5 before:top-2 before:bottom-2 before:w-px before:bg-zinc-800">
          {allEvents.map((evt) => {
            const isTurnCompleted = evt.eventType === 'TurnCompletedEvent';
            const isSuspendedEvt = evt.eventType === 'TurnSuspendedEvent';
            const isFailed =
              evt.eventType === 'TurnFailedEvent' || evt.eventType === 'TurnCompensatedEvent';
            const isGranular =
              evt.tier === 'GRANULAR' ||
              (!evt.eventType.startsWith('Turn') && !evt.eventType.startsWith('Execution'));
            const key = getEventKey(evt);

            return (
              <div key={key} className="relative group text-xs font-mono">
                {/* Timeline dot */}
                <div
                  className={`absolute -left-[14px] top-1 h-2 w-2 rounded-full border border-zinc-950 ${
                    isFailed
                      ? 'bg-rose-500'
                      : isSuspendedEvt
                      ? 'bg-amber-400'
                      : isTurnCompleted
                      ? 'bg-emerald-500'
                      : isGranular
                      ? 'bg-indigo-400'
                      : 'bg-zinc-400'
                  }`}
                />

                <div className="flex items-center justify-between gap-2 flex-wrap">
                  <div className="flex items-center gap-1.5">
                    <span className="font-medium text-zinc-200">{evt.eventType}</span>
                    <span
                      className={`text-[9px] px-1 py-0.2 rounded font-mono ${
                        isGranular
                          ? 'bg-indigo-950/40 text-indigo-400 border border-indigo-900/40'
                          : 'bg-cyan-950/40 text-cyan-400 border border-cyan-900/40'
                      }`}
                    >
                      {isGranular ? 'GRANULAR' : 'LIFECYCLE' }
                    </span>
                  </div>
                  <span className="text-[10px] text-zinc-500">{formatTime(evt.timestamp)}</span>
                </div>

                <div className="text-[11px] text-zinc-400 mt-0.5 flex flex-wrap gap-x-3 gap-y-0.5">
                  {evt.sourceState && (
                    <span>
                      from: <span className="text-zinc-300">{evt.sourceState}</span>
                    </span>
                  )}
                  {evt.targetState && (
                    <span>
                      to: <span className="text-zinc-300">{evt.targetState}</span>
                    </span>
                  )}
                  {evt.stateName && !evt.sourceState && !evt.targetState && (
                    <span>
                      state: <span className="text-zinc-300">{evt.stateName}</span>
                    </span>
                  )}
                  {evt.signalName && (
                    <span className="text-amber-400/90">signal: {evt.signalName}</span>
                  )}
                  {evt.actionName && (
                    <span className="text-violet-400/90">action: {evt.actionName}</span>
                  )}
                  {evt.durationMillis != null && (
                    <span className="text-zinc-500">{evt.durationMillis}ms</span>
                  )}
                  {evt.errorMessage && (
                    <span className="text-rose-400">error: {evt.errorMessage}</span>
                  )}
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
