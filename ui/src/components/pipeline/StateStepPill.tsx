import { ArrowRight, Circle } from 'lucide-react';
import type { ExecutionSummary } from '../../types';

interface StateStepPillProps {
  stateName: string;
  isInitial: boolean;
  isEnd: boolean;
  isLast: boolean;
  isSelected: boolean;
  stateExecutions: ExecutionSummary[];
  onSelectState: (stateName: string) => void;
}

export function StateStepPill({
  stateName,
  isInitial,
  isEnd,
  isLast,
  isSelected,
  stateExecutions,
  onSelectState,
}: StateStepPillProps) {
  const running = stateExecutions.filter(
    (e) => e.status === 'RUNNING' || e.status === 'INITIALIZING'
  ).length;
  const waiting = stateExecutions.filter((e) => e.status === 'SUSPENDED').length;
  const completed = stateExecutions.filter((e) => e.status === 'COMPLETED').length;

  return (
    <div className="flex items-center gap-2">
      <button
        type="button"
        onClick={() => onSelectState(stateName)}
        className={`px-3 py-2 rounded-lg border text-left transition-all cursor-pointer ${
          isSelected
            ? 'bg-zinc-800 border-zinc-500 shadow-xs'
            : 'bg-zinc-900/60 border-zinc-800 hover:bg-zinc-800/60 hover:border-zinc-700'
        }`}
      >
        <div className="flex items-center gap-1.5 font-mono text-xs">
          {isInitial && <Circle className="h-2 w-2 fill-zinc-400 text-zinc-400" />}
          {isEnd && <Circle className="h-2 w-2 fill-emerald-500 text-emerald-500" />}
          <span className={`font-semibold ${isSelected ? 'text-white' : 'text-zinc-200'}`}>
            {stateName}
          </span>
        </div>

        {/* Real-time turn counts */}
        <div className="mt-1 flex items-center gap-1.5 text-[10px] font-mono">
          {running > 0 && (
            <span className="flex items-center gap-1 px-1.5 py-0.2 rounded bg-zinc-800 text-emerald-400 border border-emerald-900/40">
              <span className="h-1 w-1 rounded-full bg-emerald-400 animate-pulse" />
              <span>{running} run</span>
            </span>
          )}
          {waiting > 0 && (
            <span className="flex items-center gap-1 px-1.5 py-0.2 rounded bg-zinc-800 text-amber-300 border border-amber-900/40">
              <span className="h-1 w-1 rounded-full bg-amber-400" />
              <span>{waiting} wait</span>
            </span>
          )}
          {completed > 0 && (
            <span className="px-1.5 py-0.2 rounded bg-zinc-800/80 text-zinc-400">
              {completed} done
            </span>
          )}
          {running === 0 && waiting === 0 && completed === 0 && (
            <span className="text-zinc-600 text-[10px]">idle</span>
          )}
        </div>
      </button>

      {!isLast && <ArrowRight className="h-3.5 w-3.5 text-zinc-600 shrink-0" />}
    </div>
  );
}
