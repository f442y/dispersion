import { Play } from 'lucide-react';
import type { ExecutionSummary } from '../../types';
import { Card, CardHeader, CardTitle } from '../common/Card';
import { ExecutionRow } from './ExecutionRow';

interface ExecutionsTableProps {
  executions: ExecutionSummary[];
  isLoading: boolean;
  selectedStateFilter: string | null;
  signalingId: string | null;
  onSelectExecution: (execution: ExecutionSummary) => void;
  onDeliverSignal: (execution: ExecutionSummary) => void;
  onQuickDispatch: () => void;
  isDispatching: boolean;
}

export function ExecutionsTable({
  executions,
  isLoading,
  selectedStateFilter,
  signalingId,
  onSelectExecution,
  onDeliverSignal,
  onQuickDispatch,
  isDispatching,
}: ExecutionsTableProps) {
  const filteredExecutions = selectedStateFilter
    ? executions.filter((e) => e.currentState === selectedStateFilter)
    : executions;

  return (
    <Card>
      <CardHeader>
        <div>
          <CardTitle>Executions</CardTitle>
          <p className="text-xs text-zinc-500 mt-0.5">
            {selectedStateFilter
              ? `Filtered by state: ${selectedStateFilter}`
              : 'All historical and in-flight runs'}
          </p>
        </div>

        <div className="flex items-center gap-2">
          <span className="text-xs font-mono text-zinc-500">
            {filteredExecutions.length} runs
          </span>
          <button
            type="button"
            disabled={isDispatching}
            onClick={onQuickDispatch}
            className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded bg-zinc-800 hover:bg-zinc-700 text-zinc-200 border border-zinc-700 text-xs font-medium transition cursor-pointer"
          >
            <Play className="h-3 w-3 fill-current" />
            <span>New Run</span>
          </button>
        </div>
      </CardHeader>

      <div className="mt-3">
        {isLoading ? (
          <div className="py-6 text-center text-xs text-zinc-500 font-mono">
            Loading instances...
          </div>
        ) : filteredExecutions.length === 0 ? (
          <div className="py-6 text-center text-xs text-zinc-500 font-mono border border-dashed border-zinc-800 rounded-md space-y-2">
            <div>
              {selectedStateFilter
                ? `No instances currently in state '${selectedStateFilter}'.`
                : 'No instances recorded for this workflow.'}
            </div>
            <div>
              <button
                type="button"
                onClick={onQuickDispatch}
                className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded bg-zinc-800 hover:bg-zinc-700 text-zinc-200 border border-zinc-700 text-xs font-medium transition cursor-pointer"
              >
                <Play className="h-3 w-3 fill-current" />
                <span>Dispatch Run</span>
              </button>
            </div>
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-left text-xs font-mono">
              <thead className="border-b border-zinc-800 text-zinc-500">
                <tr>
                  <th className="pb-2 font-medium">ID</th>
                  <th className="pb-2 font-medium">Correlation Key</th>
                  <th className="pb-2 font-medium">Current State</th>
                  <th className="pb-2 font-medium">Status</th>
                  <th className="pb-2 font-medium">Turns</th>
                  <th className="pb-2 font-medium text-right">Actions</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-zinc-800/40">
                {filteredExecutions.map((exec) => (
                  <ExecutionRow
                    key={exec.executionId}
                    execution={exec}
                    isSignaling={signalingId === exec.executionId}
                    onSelect={onSelectExecution}
                    onDeliverSignal={onDeliverSignal}
                  />
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </Card>
  );
}
