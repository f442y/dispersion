import { Radio, Circle } from 'lucide-react';
import type { StreamExecutionEvent } from '../../types';
import { Card, CardHeader, CardTitle } from '../common/Card';
import { TelemetryEventRow } from './TelemetryEventRow';

interface LiveTelemetryStreamProps {
  flowName: string;
  events: StreamExecutionEvent[];
}

export function LiveTelemetryStream({ flowName, events }: LiveTelemetryStreamProps) {
  return (
    <Card>
      <CardHeader>
        <div className="flex items-center gap-2">
          <Radio className="h-3.5 w-3.5 text-zinc-400" />
          <CardTitle>Telemetry Stream</CardTitle>
          <span className="inline-flex items-center gap-1 px-1.5 py-0.2 rounded text-[10px] font-mono bg-zinc-800/80 text-zinc-400 border border-zinc-700/60">
            <Circle className="h-1.5 w-1.5 fill-emerald-500 text-emerald-500" />
            <span>SSE Active</span>
          </span>
        </div>
        <span className="text-xs font-mono text-zinc-500">
          {events.length} captured
        </span>
      </CardHeader>

      <div className="mt-3">
        {events.length === 0 ? (
          <div className="py-6 text-center text-xs text-zinc-500 font-mono border border-dashed border-zinc-800 rounded-md">
            Waiting for transitions in {flowName}...
          </div>
        ) : (
          <div className="space-y-1.5 max-h-60 overflow-y-auto pr-1">
            {events.map((evt, idx) => (
              <TelemetryEventRow key={`${evt.timestamp}-${idx}`} event={evt} />
            ))}
          </div>
        )}
      </div>
    </Card>
  );
}
