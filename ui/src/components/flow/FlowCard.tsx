import React from 'react';
import { Workflow, Box, GitFork, Play, RefreshCw } from 'lucide-react';
import type { MachineDescriptor, ExecutionSummary } from '../../types';
import { useDispatchMachineMutation } from '../../hooks';
import { toast } from 'sonner';

interface FlowCardProps {
  machine: MachineDescriptor;
  isSelected: boolean;
  executions: ExecutionSummary[];
  onSelect: (name: string) => void;
}

export function FlowCard({ machine, isSelected, executions, onSelect }: FlowCardProps) {
  const dispatchMutation = useDispatchMachineMutation();
  const suspended = executions.filter((e) => e.status === 'SUSPENDED').length;

  const getIcon = () => {
    switch (machine.type) {
      case 'TIER_2_DISCRETE_SAGA':
        return <Workflow className="h-3.5 w-3.5 text-zinc-400" />;
      case 'TIER_3_BATCH':
        return <Box className="h-3.5 w-3.5 text-zinc-400" />;
      case 'TIER_1_ATOMIC_FSM':
      default:
        return <GitFork className="h-3.5 w-3.5 text-zinc-400" />;
    }
  };

  const getTypeLabel = () => {
    switch (machine.type) {
      case 'TIER_2_DISCRETE_SAGA':
        return 'Orchestration';
      case 'TIER_3_BATCH':
        return 'Batch';
      case 'TIER_1_ATOMIC_FSM':
      default:
        return 'FSM';
    }
  };

  const handleQuickRun = (e: React.MouseEvent) => {
    e.stopPropagation();
    dispatchMutation.mutate(
      { name: machine.name },
      {
        onSuccess: (res) => {
          toast.success(`Dispatched ${machine.name}`, {
            description: `Status: ${res.status}. Instance started.`,
          });
        },
        onError: (err) => {
          const msg = err instanceof Error ? err.message : String(err);
          toast.error(`Dispatch Failed: ${machine.name}`, { description: msg });
        },
      }
    );
  };

  return (
    <div
      onClick={() => onSelect(machine.name)}
      className={`text-left p-3 rounded-lg border transition-all cursor-pointer ${
        isSelected
          ? 'bg-zinc-900 border-zinc-500/90 shadow-xs ring-1 ring-zinc-500/20'
          : 'bg-zinc-900/40 border-zinc-800/80 hover:bg-zinc-900/80 hover:border-zinc-700'
      }`}
    >
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          {getIcon()}
          <span className="font-mono text-xs font-semibold text-zinc-200">{machine.name}</span>
        </div>
        <div className="flex items-center gap-1.5">
          <button
            type="button"
            disabled={dispatchMutation.isPending}
            onClick={handleQuickRun}
            className="inline-flex items-center gap-1 px-2 py-0.5 rounded bg-zinc-800 hover:bg-zinc-700 text-zinc-300 border border-zinc-700/60 text-[10px] font-mono transition cursor-pointer disabled:opacity-50"
            title={`Run ${machine.name}`}
          >
            {dispatchMutation.isPending ? (
              <RefreshCw className="h-2.5 w-2.5 animate-spin" />
            ) : (
              <Play className="h-2.5 w-2.5 fill-current" />
            )}
            <span>Run</span>
          </button>
          <span className="text-[10px] font-mono px-1.5 py-0.5 rounded bg-zinc-800/70 text-zinc-400 border border-zinc-700/50">
            {getTypeLabel()}
          </span>
        </div>
      </div>
      <div className="mt-2.5 flex items-center justify-between text-[11px] text-zinc-400 font-mono">
        <span>{machine.allStates.length} States</span>
        <span className="flex items-center gap-1.5">
          {suspended > 0 && (
            <span className="px-1.5 py-0.2 rounded bg-amber-950/40 text-amber-300 border border-amber-800/50 text-[10px]">
              {suspended} suspended
            </span>
          )}
          <span>{executions.length} runs</span>
        </span>
      </div>
    </div>
  );
}
