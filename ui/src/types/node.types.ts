export interface NodeInfo {
  nodeId: string;
  environment?: string;
  clusterId?: string;
  role?: string;
  host: string;
  port: number;
  basePath: string;
  runtime: string;
  virtualThreadsEnabled: boolean;
  uptimeSeconds: number;
  availableProcessors: number;
  activeMachines: number;
  status: 'HEALTHY' | 'DEGRADED' | 'OFFLINE';
}

export interface DiscoveredNode {
  id: string;
  url: string;
  host: string;
  port: number;
  basePath: string;
  nodeInfo: NodeInfo;
  pingMs: number;
  lastSeen: Date;
}

export interface NodeProbeResult {
  nodeInfo: NodeInfo;
  pingMs: number;
}
