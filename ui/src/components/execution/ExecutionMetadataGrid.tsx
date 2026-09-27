import { Circle } from 'lucide-react';
import type { ExecutionSummary } from '../../types';
import { formatTime } from '../../utils';

interface ExecutionMetadataGridProps {
  execution: ExecutionSummary;
}

export function ExecutionMetadataGrid({ execution }: ExecutionMetadataGridProps) {
  const isSuspended = execution.status === 'SUSPENDED';
  const isRunning = execution.status === 'RUNNING' || execution.status === 'INITIALIZING';

  return (
    <div className="grid grid-cols-2 gap-2 text-xs font-mono">
      <div className="p-2.5 rounded-md bg-zinc-900/30 border border-zinc-800/80">
        <div className="text-[10px] text-zinc-500 uppercase">Workflow</div>
        <div className="text-zinc-200 font-medium mt-0.5">{execution.machineName}</div>
      </div>

      <div className="p-2.5 rounded-md bg-zinc-900/30 border border-zinc-800/80">
        <div className="text-[10px] text-zinc-500 uppercase">Current State</div>
        <div className="text-zinc-200 font-medium mt-0.5 flex items-center gap-1.5">
          {isSuspended && <Circle className="h-1.5 w-1.5 fill-amber-400 text-amber-400" />}
          {isRunning && <Circle className="h-1.5 w-1.5 fill-emerald-500 text-emerald-500" />}
          <span>{execution.currentState}</span>
        </div>
      </div>

      <div className="p-2.5 rounded-md bg-zinc-900/30 border border-zinc-800/80">
        <div className="text-[10px] text-zinc-500 uppercase">Correlation Key</div>
        <div className="text-zinc-300 font-medium mt-0.5">
          {execution.correlationKey ?? 'None'}
        </div>
      </div>

      <div className="p-2.5 rounded-md bg-zinc-900/30 border border-zinc-800/80">
        <div className="text-[10px] text-zinc-500 uppercase">Turns Executed</div>
        <div className="text-zinc-200 font-medium mt-0.5">{execution.transitionsCount}</div>
      </div>

      <div className="p-2.5 rounded-md bg-zinc-900/30 border border-zinc-800/80">
        <div className="text-[10px] text-zinc-500 uppercase">Start Time</div>
        <div className="text-zinc-400 mt-0.5">{formatTime(execution.startTime)}</div>
      </div>

      <div className="p-2.5 rounded-md bg-zinc-900/30 border border-zinc-800/80">
        <div className="text-[10px] text-zinc-500 uppercase">Last Updated</div>
        <div className="text-zinc-400 mt-0.5">{formatTime(execution.lastUpdated)}</div>
      </div>
    </div>
  );
}
