import { Server, Wifi, RefreshCw, AlertCircle, Circle } from 'lucide-react';
import type { useNodeCluster } from '../../hooks/useNodeCluster';
import type { SseConnectionStatus } from '../../types';
import { NodeTelemetryBadges } from './NodeTelemetryBadges';
import { Button } from '../common/Button';

interface ConnectedNodeBarProps {
  cluster: ReturnType<typeof useNodeCluster>;
  sseStatus: SseConnectionStatus;
}

export function ConnectedNodeBar({ cluster, sseStatus }: ConnectedNodeBarProps) {
  const {
    activeNode,
    isConnected,
    isChecking,
    retryCountdown,
    triggerCheck,
  } = cluster;

  const getSseBadge = () => {
    switch (sseStatus) {
      case 'connected':
        return {
          label: 'Live Stream',
          dot: 'bg-emerald-500',
        };
      case 'connecting':
      case 'reconnecting':
        return {
          label: 'Reconnecting',
          dot: 'bg-amber-400 animate-pulse',
        };
      case 'disconnected':
      default:
        return {
          label: 'Standby',
          dot: 'bg-zinc-600',
        };
    }
  };

  const sseInfo = getSseBadge();

  if (!isConnected || !activeNode) {
    return (
      <div className="rounded-lg border border-zinc-800 bg-zinc-900/30 p-4 transition-all">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
          <div className="flex items-start gap-3">
            <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-md bg-zinc-800/80 text-zinc-400 border border-zinc-700/60">
              <AlertCircle className="h-4 w-4 text-amber-400" />
            </div>
            <div>
              <div className="flex items-center gap-2">
                <span className="font-mono text-xs font-semibold text-zinc-200">
                  Node Offline
                </span>
                <span className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded text-[10px] font-mono text-zinc-400 bg-zinc-800/60 border border-zinc-700/60">
                  <span className="h-1.5 w-1.5 rounded-full bg-zinc-500" />
                  Disconnected
                </span>
              </div>
              <p className="text-xs text-zinc-400 font-mono mt-1">
                Endpoint: <code className="text-zinc-300">127.0.0.1:8080/api/v1</code>
              </p>
              <p className="text-[11px] text-zinc-500 font-mono mt-0.5">
                Auto-retrying in {retryCountdown}s
              </p>
            </div>
          </div>

          <div className="flex items-center gap-2 font-mono text-xs">
            <Button
              variant="secondary"
              size="sm"
              disabled={isChecking}
              onClick={() => triggerCheck()}
            >
              <RefreshCw className={`h-3 w-3 ${isChecking ? 'animate-spin' : ''}`} />
              <span>Retry</span>
            </Button>
          </div>
        </div>
      </div>
    );
  }

  const nodeInfo = activeNode.nodeInfo;

  return (
    <div className="rounded-lg border border-zinc-800/80 bg-zinc-900/30 p-3.5 space-y-3">
      {/* Top row: Identity & Primary Status */}
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex items-center gap-3">
          <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-md bg-zinc-900 border border-zinc-800 text-zinc-400">
            <Server className="h-4 w-4 text-zinc-300" />
          </div>
          <div>
            <div className="flex items-center gap-2">
              <span className="font-mono text-xs font-semibold text-zinc-100">
                {activeNode.id}
              </span>
              <span className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded text-[10px] font-mono text-zinc-300 bg-zinc-800/60 border border-zinc-700/50">
                <Circle className="h-1.5 w-1.5 fill-emerald-500 text-emerald-500" />
                <span>{activeNode.pingMs}ms</span>
              </span>
            </div>

            <div className="flex flex-wrap items-center gap-2 text-[11px] text-zinc-500 font-mono mt-0.5">
              <span className="text-zinc-400">{activeNode.url}</span>
              <span>&bull;</span>
              <span>{nodeInfo.runtime}</span>
            </div>
          </div>
        </div>

        <div className="flex items-center gap-2 font-mono text-xs">
          <div className="flex items-center gap-1.5 px-2.5 py-1 rounded-md bg-zinc-900/80 border border-zinc-800 text-[11px] text-zinc-400">
            <Wifi className="h-3 w-3 text-zinc-400" />
            <span className={`h-1.5 w-1.5 rounded-full ${sseInfo.dot}`} />
            <span>{sseInfo.label}</span>
          </div>

          <Button
            variant="secondary"
            size="sm"
            disabled={isChecking}
            onClick={() => triggerCheck()}
            title="Check node health"
          >
            <RefreshCw className={`h-3 w-3 text-zinc-400 ${isChecking ? 'animate-spin text-zinc-200' : ''}`} />
            <span>{isChecking ? 'Checking...' : 'Refresh'}</span>
          </Button>
        </div>
      </div>

      {/* Bottom row: Telemetry Badges */}
      <NodeTelemetryBadges nodeInfo={nodeInfo} />
    </div>
  );
}
