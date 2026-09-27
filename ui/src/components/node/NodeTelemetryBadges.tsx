import { Cpu, Clock, Activity, Shield } from 'lucide-react';
import type { NodeInfo } from '../../types';
import { formatUptime } from '../../utils';

interface NodeTelemetryBadgesProps {
  nodeInfo: NodeInfo;
}

export function NodeTelemetryBadges({ nodeInfo }: NodeTelemetryBadgesProps) {
  return (
    <div className="flex flex-wrap items-center gap-2 text-[11px] font-mono pt-2 border-t border-zinc-800/60">
      <div className="flex items-center gap-1.5 px-2 py-0.5 rounded bg-zinc-900/60 border border-zinc-800/80 text-zinc-400">
        <Shield className="h-3 w-3 text-zinc-400" />
        <span>Virtual Threads:</span>
        <span className="text-zinc-200 font-medium">Java 25</span>
      </div>

      {nodeInfo.availableProcessors && (
        <div className="flex items-center gap-1.5 px-2 py-0.5 rounded bg-zinc-900/60 border border-zinc-800/80 text-zinc-400">
          <Cpu className="h-3 w-3 text-zinc-400" />
          <span>CPUs:</span>
          <span className="text-zinc-200 font-medium">{nodeInfo.availableProcessors} Cores</span>
        </div>
      )}

      {nodeInfo.uptimeSeconds !== undefined && (
        <div className="flex items-center gap-1.5 px-2 py-0.5 rounded bg-zinc-900/60 border border-zinc-800/80 text-zinc-400">
          <Clock className="h-3 w-3 text-zinc-400" />
          <span>Uptime:</span>
          <span className="text-zinc-200 font-medium">{formatUptime(nodeInfo.uptimeSeconds)}</span>
        </div>
      )}

      {nodeInfo.activeMachines !== undefined && (
        <div className="flex items-center gap-1.5 px-2 py-0.5 rounded bg-zinc-900/60 border border-zinc-800/80 text-zinc-400">
          <Activity className="h-3 w-3 text-zinc-400" />
          <span>Active Flows:</span>
          <span className="text-zinc-200 font-medium">{nodeInfo.activeMachines}</span>
        </div>
      )}
    </div>
  );
}
