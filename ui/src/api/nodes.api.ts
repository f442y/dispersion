import { request } from './client';
import type { NodeInfo, NodeProbeResult } from '../types';

export const nodesApi = {
  async probeNode(baseUrl: string, timeoutMs: number = 2000): Promise<NodeProbeResult | null> {
    const cleanUrl = baseUrl.endsWith('/') ? baseUrl.slice(0, -1) : baseUrl;
    const start = performance.now();
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const res = await fetch(`${cleanUrl}/node`, {
        signal: controller.signal,
        headers: { Accept: 'application/json' },
      });
      clearTimeout(timer);
      if (!res.ok) return null;
      const nodeInfo = (await res.json()) as NodeInfo;
      const pingMs = Math.round(performance.now() - start);
      return { nodeInfo, pingMs };
    } catch {
      clearTimeout(timer);
      return null;
    }
  },

  async getNodeInfo(customBase?: string): Promise<NodeInfo> {
    return request<NodeInfo>('/node', { method: 'GET' }, customBase);
  },
};
