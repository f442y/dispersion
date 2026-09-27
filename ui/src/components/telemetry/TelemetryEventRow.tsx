import type { StreamExecutionEvent } from '../../types';
import { formatTime } from '../../utils';

interface TelemetryEventRowProps {
  event: StreamExecutionEvent;
}

export function TelemetryEventRow({ event }: TelemetryEventRowProps) {
  const isFailed =
    event.eventType.includes('Failed') || event.eventType.includes('Compensat');
  const isSuspended = event.eventType.includes('Suspended');
  const isCompleted = event.eventType.includes('Completed');

  return (
    <div className="flex items-center justify-between p-2 rounded bg-zinc-900/40 border border-zinc-800/60 text-xs font-mono">
      <div className="flex items-center gap-2">
        <span
          className={`h-1.5 w-1.5 rounded-full shrink-0 ${
            isFailed
              ? 'bg-rose-500'
              : isSuspended
              ? 'bg-amber-400'
              : isCompleted
              ? 'bg-emerald-500'
              : 'bg-zinc-400'
          }`}
        />
        <span className="font-medium text-zinc-200">{event.eventType}</span>
        {event.stateName && (
          <span className="text-zinc-500">
            state: <code className="text-zinc-300">{event.stateName}</code>
          </span>
        )}
        {event.targetState && (
          <span className="text-zinc-500">
            &rarr; <code className="text-zinc-300">{event.targetState}</code>
          </span>
        )}
        {event.signalName && (
          <span className="text-zinc-500">
            sig: <code className="text-amber-300">{event.signalName}</code>
          </span>
        )}
      </div>
      <div className="text-[11px] text-zinc-500 shrink-0">
        {formatTime(event.timestamp)}
      </div>
    </div>
  );
}
