import { useState } from 'react';
import type { MachineDescriptor, ExecutionSummary, StreamExecutionEvent } from '../../types';
import { FlowHeader } from './FlowHeader';
import { FlowKpiGrid } from './FlowKpiGrid';
import { DispatchModal } from './DispatchModal';
import { SimpleStatePipeline } from '../pipeline/SimpleStatePipeline';
import { MermaidDiagram } from '../pipeline/MermaidDiagram';
import { ExecutionsTable } from '../execution/ExecutionsTable';
import { ExecutionDetailDrawer } from '../execution/ExecutionDetailDrawer';
import { LiveTelemetryStream } from '../telemetry/LiveTelemetryStream';
import { useDispatchMachineMutation, useSendSignalMutation } from '../../hooks';
import { toast } from 'sonner';

interface FlowInspectorProps {
  flow: MachineDescriptor;
  executions: ExecutionSummary[];
  isLoadingExecutions: boolean;
  events: StreamExecutionEvent[];
  onRefreshExecutions?: () => void;
}

export function FlowInspector({
  flow,
  executions,
  isLoadingExecutions,
  events,
  onRefreshExecutions,
}: FlowInspectorProps) {
  const [viewMode, setViewMode] = useState<'pipeline' | 'mermaid'>('pipeline');
  const [selectedStateFilter, setSelectedStateFilter] = useState<string | null>(null);
  const [selectedExecution, setSelectedExecution] = useState<ExecutionSummary | null>(null);
  const [showDispatchModal, setShowDispatchModal] = useState(false);
  const [signalingId, setSignalingId] = useState<string | null>(null);

  const dispatchMutation = useDispatchMachineMutation();
  const sendSignalMutation = useSendSignalMutation();

  const handleQuickDispatch = (payload?: unknown) => {
    dispatchMutation.mutate(
      { name: flow.name, payload },
      {
        onSuccess: (res) => {
          toast.success(`Dispatched ${flow.name}`, {
            description: `Workflow run initialized with status ${res.status}`,
          });
          setShowDispatchModal(false);
          if (onRefreshExecutions) onRefreshExecutions();
        },
        onError: (err) => {
          const msg = err instanceof Error ? err.message : String(err);
          toast.error(`Dispatch Failed: ${flow.name}`, { description: msg });
        },
      }
    );
  };

  const handleDeliverSignal = (exec: ExecutionSummary) => {
    const key = exec.correlationKey || 'ORDER-DEMO-99';
    const sig = exec.suspendedSignal || 'PaymentSignal';
    setSignalingId(exec.executionId);

    sendSignalMutation.mutate(
      {
        machineName: exec.machineName,
        correlationKey: key,
        signalName: sig,
        payload: {
          paymentMethod: 'CREDIT_CARD',
          amountCents: 9995,
          authCode: 'AUTH-' + Math.floor(1000 + Math.random() * 9000),
        },
      },
      {
        onSuccess: (res) => {
          if (res.delivered) {
            toast.success(`Signal [${sig}] Delivered`, {
              description: `Execution resumed -> ${res.resultingState ?? 'Next Step'}`,
            });
            if (onRefreshExecutions) onRefreshExecutions();
          } else {
            toast.warning(res.message || 'Signal not delivered', {
              description: res.errorMessage ?? 'Workflow turn could not accept signal.',
            });
          }
        },
        onError: (err) => {
          const msg = err instanceof Error ? err.message : String(err);
          toast.error('Signal delivery failed', { description: msg });
        },
        onSettled: () => {
          setSignalingId(null);
        },
      }
    );
  };

  return (
    <div className="space-y-4">
      {/* Flow Header */}
      <FlowHeader
        flow={flow}
        viewMode={viewMode}
        onViewModeChange={setViewMode}
        isDispatching={dispatchMutation.isPending}
        onQuickDispatch={() => handleQuickDispatch()}
        onOpenCustomDispatch={() => setShowDispatchModal(true)}
        hasFilter={!!selectedStateFilter}
        onClearFilter={() => setSelectedStateFilter(null)}
      />

      {/* KPI Counters */}
      <FlowKpiGrid executions={executions} />

      {/* Visual State Pipeline or Mermaid Graph */}
      {viewMode === 'pipeline' ? (
        <SimpleStatePipeline
          machine={flow}
          executions={executions}
          selectedStateFilter={selectedStateFilter}
          onSelectStateFilter={setSelectedStateFilter}
        />
      ) : (
        <MermaidDiagram chart={flow.mermaidGraph} />
      )}

      {/* Executions Table */}
      <ExecutionsTable
        executions={executions}
        isLoading={isLoadingExecutions}
        selectedStateFilter={selectedStateFilter}
        signalingId={signalingId}
        onSelectExecution={setSelectedExecution}
        onDeliverSignal={handleDeliverSignal}
        onQuickDispatch={() => handleQuickDispatch()}
        isDispatching={dispatchMutation.isPending}
      />

      {/* Live SSE Telemetry Feed */}
      <LiveTelemetryStream flowName={flow.name} events={events} />

      {/* Instance Detail Slide-out Drawer */}
      <ExecutionDetailDrawer
        execution={selectedExecution}
        onClose={() => setSelectedExecution(null)}
        onSignalDelivered={() => {
          if (onRefreshExecutions) onRefreshExecutions();
        }}
      />

      {/* Custom JSON Dispatch Modal */}
      <DispatchModal
        isOpen={showDispatchModal}
        onClose={() => setShowDispatchModal(false)}
        flowName={flow.name}
        isDispatching={dispatchMutation.isPending}
        onDispatch={(payload) => handleQuickDispatch(payload)}
      />
    </div>
  );
}
