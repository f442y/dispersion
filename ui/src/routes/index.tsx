import { createFileRoute } from '@tanstack/react-router';
import { useState } from 'react';
import { Terminal, AlertCircle, RefreshCw } from 'lucide-react';
import {
  useNodeCluster,
  useMachinesQuery,
  useExecutionsQuery,
  useEventStream,
  DEV_NODE_HOST,
  DEV_NODE_PORT,
} from '../hooks';
import { ConnectedNodeBar, FlowCatalog, FlowInspector } from '../components';

export const Route = createFileRoute('/')({
  component: DashboardComponent,
});

function DashboardComponent() {
  const cluster = useNodeCluster();
  const [selectedFlowName, setSelectedFlowName] = useState<string | null>(null);

  const activeNode = cluster.activeNode;

  // Fetch registered machines / flows for active node using TanStack Query
  const { data: machines, isLoading: machinesLoading } = useMachinesQuery(
    activeNode?.id,
    cluster.isConnected
  );

  const activeFlow =
    (machines && machines.find((m) => m.name === selectedFlowName)) ||
    (machines && machines.length > 0 ? machines[0] : null);

  // Fetch executions specifically for the active flow using TanStack Query
  const {
    data: flowExecutions,
    isLoading: flowExecutionsLoading,
    refetch: refetchFlowExecutions,
  } = useExecutionsQuery(
    activeNode?.id,
    activeFlow ? { machine: activeFlow.name } : undefined,
    cluster.isConnected && !!activeFlow,
    cluster.isConnected ? 3000 : false
  );

  // Fetch all executions count summary across all flows for catalog badge indicators
  const { data: allExecutions, refetch: refetchAllExecutions } = useExecutionsQuery(
    activeNode?.id,
    undefined,
    cluster.isConnected,
    cluster.isConnected ? 5000 : false
  );

  // Flow-scoped live SSE event stream
  const { events, status: sseStatus } = useEventStream({
    machineName: activeFlow?.name,
    baseUrl: activeNode?.url,
    enabled: cluster.isConnected,
    maxEvents: 40,
  });

  const handleRefreshExecutions = () => {
    refetchFlowExecutions();
    refetchAllExecutions();
  };

  return (
    <div className="space-y-6 max-w-7xl mx-auto pb-12">
      {/* Connected Node Information Bar */}
      <ConnectedNodeBar cluster={cluster} sseStatus={sseStatus} />

      {/* Disconnected Placeholder Hero */}
      {!cluster.isConnected ? (
        <div className="rounded-lg border border-zinc-800 bg-zinc-900/20 p-8 text-center space-y-4">
          <div className="flex h-10 w-10 mx-auto items-center justify-center rounded-md bg-zinc-800 text-zinc-400 border border-zinc-700/60">
            <AlertCircle className="h-5 w-5" />
          </div>

          <div className="space-y-1.5 max-w-lg mx-auto">
            <h3 className="text-sm font-semibold text-zinc-200">
              Waiting for Dispersion Control Plane Node
            </h3>
            <p className="text-xs text-zinc-400 font-mono leading-relaxed">
              Target endpoint:{' '}
              <code className="text-zinc-300">
                http://{DEV_NODE_HOST}:{DEV_NODE_PORT}/api/v1
              </code>
              <br />
              <span className="text-zinc-500">Auto-retrying in {cluster.retryCountdown}s</span>
            </p>
          </div>

          <div className="max-w-xl mx-auto bg-zinc-900/90 border border-zinc-800/80 rounded-md p-3 text-left font-mono text-xs space-y-1.5">
            <div className="flex items-center gap-2 text-zinc-400 border-b border-zinc-800 pb-1.5 mb-1.5">
              <Terminal className="h-3 w-3 text-zinc-400" />
              <span className="text-[11px] text-zinc-400">Launch Demo App:</span>
            </div>
            <code className="text-[11px] text-zinc-300 block select-all break-all">
              .\mvnw compile exec:java -pl examples -Dexec.mainClass="com.github.f442y.dispersion.examples.DispersionDemoApp"
            </code>
          </div>

          <div className="flex items-center justify-center gap-3 font-mono text-xs pt-2">
            <button
              type="button"
              disabled={cluster.isChecking}
              onClick={() => cluster.triggerCheck()}
              className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-md bg-zinc-800 hover:bg-zinc-700 text-zinc-200 border border-zinc-700 text-xs font-medium transition disabled:opacity-50 cursor-pointer"
            >
              <RefreshCw className={`h-3 w-3 ${cluster.isChecking ? 'animate-spin' : ''}`} />
              <span>Retry Connection</span>
            </button>
          </div>
        </div>
      ) : (
        <>
          {/* Flow Selection Deck / Catalog */}
          <FlowCatalog
            machines={machines}
            isLoading={machinesLoading}
            activeFlowName={activeFlow?.name ?? null}
            allExecutions={allExecutions}
            onSelectFlow={setSelectedFlowName}
            activeNodeId={activeNode?.id}
          />

          {/* Selected Flow Inspector with Its Respective Executions & Telemetry */}
          {activeFlow && (
            <FlowInspector
              flow={activeFlow}
              executions={flowExecutions ?? []}
              isLoadingExecutions={flowExecutionsLoading}
              events={events}
              onRefreshExecutions={handleRefreshExecutions}
            />
          )}
        </>
      )}
    </div>
  );
}
