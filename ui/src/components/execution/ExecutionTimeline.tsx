import { Radio, RefreshCw } from 'lucide-react';
import { useExecutionTimelineQuery } from '../../hooks';
import { formatTime } from '../../utils';

interface ExecutionTimelineProps {
  executionId: string;
  isLive: boolean;
}

export function ExecutionTimeline({ executionId, isLive }: ExecutionTimelineProps) {
  const {
    data: timelineEvents,
    isLoading,
    refetch,
    isFetching,
  } = useExecutionTimelineQuery(executionId, true, isLive ? 2000 : false);

  return (
    <div className="rounded-md border border-zinc-800/80 bg-zinc-900/20 p-3.5 space-y-3">
      <div className="flex items-center justify-between pb-2 border-b border-zinc-800/60">
        <div className="flex items-center gap-1.5">
          <Radio className="h-3.5 w-3.5 text-zinc-400" />
          <h4 className="text-xs font-semibold text-zinc-200 uppercase tracking-wider font-mono">
            Execution Timeline
          </h4>
        </div>
        <button
          type="button"
          onClick={() => refetch()}
          className="text-zinc-500 hover:text-zinc-300 transition p-0.5 text-[11px] flex items-center gap-1 font-mono cursor-pointer"
        >
          <RefreshCw className={`h-2.5 w-2.5 ${isFetching ? 'animate-spin' : ''}`} />
          <span>Refresh</span>
        </button>
      </div>

      {isLoading ? (
        <div className="py-5 text-center text-xs text-zinc-500 font-mono">
          Loading timeline...
        </div>
      ) : !timelineEvents || timelineEvents.length === 0 ? (
        <div className="py-5 text-center text-xs text-zinc-500 font-mono border border-dashed border-zinc-800 rounded">
          No recorded transition events for this instance yet.
        </div>
      ) : (
        <div className="relative pl-4 space-y-3.5 before:absolute before:left-1.5 before:top-2 before:bottom-2 before:w-px before:bg-zinc-800">
          {timelineEvents.map((evt, idx) => {
            const isTurnCompleted = evt.eventType === 'TurnCompletedEvent';
            const isSuspendedEvt = evt.eventType === 'TurnSuspendedEvent';
            const isFailed =
              evt.eventType === 'TurnFailedEvent' || evt.eventType === 'TurnCompensatedEvent';

            return (
              <div key={idx} className="relative group text-xs font-mono">
                {/* Timeline dot */}
                <div
                  className={`absolute -left-[14px] top-1 h-2 w-2 rounded-full border border-zinc-950 ${
                    isFailed
                      ? 'bg-rose-500'
                      : isSuspendedEvt
                      ? 'bg-amber-400'
                      : isTurnCompleted
                      ? 'bg-emerald-500'
                      : 'bg-zinc-400'
                  }`}
                />

                <div className="flex items-center justify-between">
                  <span className="font-medium text-zinc-200">{evt.eventType}</span>
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
                  {evt.signalName && (
                    <span className="text-amber-400/90">signal: {evt.signalName}</span>
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
