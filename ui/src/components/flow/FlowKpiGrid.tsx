import { Circle } from 'lucide-react';
import type { ExecutionSummary } from '../../types';

interface FlowKpiGridProps {
  executions: ExecutionSummary[];
}

export function FlowKpiGrid({ executions }: FlowKpiGridProps) {
  const activeCount = executions.filter(
    (e) => e.status === 'RUNNING' || e.status === 'INITIALIZING'
  ).length;
  const suspendedCount = executions.filter((e) => e.status === 'SUSPENDED').length;
  const completedCount = executions.filter((e) => e.status === 'COMPLETED').length;
  const compOrFailedCount = executions.filter(
    (e) => e.status === 'COMPENSATED' || e.status === 'FAILED'
  ).length;

  return (
    <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 text-xs font-mono">
      <div className="p-3 rounded-lg bg-zinc-900/30 border border-zinc-800/80">
        <div className="flex items-center justify-between text-[11px] text-zinc-500">
          <span>Active</span>
          <Circle className="h-1.5 w-1.5 fill-emerald-500 text-emerald-500" />
        </div>
        <div className="text-xl font-bold font-mono text-zinc-100 mt-1">{activeCount}</div>
      </div>

      <div className="p-3 rounded-lg bg-zinc-900/30 border border-zinc-800/80">
        <div className="flex items-center justify-between text-[11px] text-zinc-500">
          <span>Suspended</span>
          <Circle className="h-1.5 w-1.5 fill-amber-400 text-amber-400" />
        </div>
        <div className="text-xl font-bold font-mono text-zinc-100 mt-1">{suspendedCount}</div>
      </div>

      <div className="p-3 rounded-lg bg-zinc-900/30 border border-zinc-800/80">
        <div className="flex items-center justify-between text-[11px] text-zinc-500">
          <span>Completed</span>
          <Circle className="h-1.5 w-1.5 fill-zinc-500 text-zinc-500" />
        </div>
        <div className="text-xl font-bold font-mono text-zinc-100 mt-1">{completedCount}</div>
      </div>

      <div className="p-3 rounded-lg bg-zinc-900/30 border border-zinc-800/80">
        <div className="flex items-center justify-between text-[11px] text-zinc-500">
          <span>Compensations</span>
          <Circle className="h-1.5 w-1.5 fill-rose-500 text-rose-500" />
        </div>
        <div className="text-xl font-bold font-mono text-zinc-100 mt-1">{compOrFailedCount}</div>
      </div>
    </div>
  );
}
