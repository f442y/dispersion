import type { MachineDescriptor, ExecutionSummary } from '../../types';
import { Card, CardHeader, CardTitle } from '../common/Card';
import { FlowCard } from './FlowCard';

interface FlowCatalogProps {
  machines: MachineDescriptor[] | undefined;
  isLoading: boolean;
  activeFlowName: string | null;
  allExecutions: ExecutionSummary[] | undefined;
  onSelectFlow: (name: string) => void;
  activeNodeId?: string;
}

export function FlowCatalog({
  machines,
  isLoading,
  activeFlowName,
  allExecutions,
  onSelectFlow,
  activeNodeId,
}: FlowCatalogProps) {
  return (
    <Card>
      <CardHeader>
        <div>
          <CardTitle>Registered Flows</CardTitle>
          <p className="text-xs text-zinc-500 mt-0.5">
            Select a workflow to inspect execution states, trigger signals, and observe live telemetry
          </p>
        </div>
        <span className="text-xs font-mono text-zinc-400">
          {(machines?.length ?? 0)} flows
        </span>
      </CardHeader>

      {isLoading ? (
        <div className="py-6 text-center text-xs text-zinc-500 font-mono">
          Loading flow registry...
        </div>
      ) : !machines || machines.length === 0 ? (
        <div className="py-8 text-center text-xs text-zinc-500 font-mono border border-dashed border-zinc-800 rounded-md mt-3">
          No flows registered on node {activeNodeId ?? 'local'}.
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-3 gap-2.5 mt-3">
          {machines.map((m) => {
            const isSelected = activeFlowName === m.name;
            const flowExecs = allExecutions?.filter((e) => e.machineName === m.name) ?? [];

            return (
              <FlowCard
                key={m.name}
                machine={m}
                isSelected={isSelected}
                executions={flowExecs}
                onSelect={onSelectFlow}
              />
            );
          })}
        </div>
      )}
    </Card>
  );
}
