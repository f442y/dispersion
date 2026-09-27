import { useState, useEffect, useCallback, useRef } from 'react';
import { nodesApi } from '../api/nodes.api';
import { setActiveNodeBaseUrl } from '../api/client';
import type { DiscoveredNode } from '../types';

export const DEV_NODE_HOST = '127.0.0.1';
export const DEV_NODE_PORT = 8080;
export const DEFAULT_NODE_URL = `http://${DEV_NODE_HOST}:${DEV_NODE_PORT}/api/v1`;

export const RETRY_INTERVAL_SECONDS = 10;

export function useNodeCluster() {
  const [activeNode, setActiveNode] = useState<DiscoveredNode | null>(null);
  const [isChecking, setIsChecking] = useState<boolean>(false);
  const [retryCountdown, setRetryCountdown] = useState<number>(RETRY_INTERVAL_SECONDS);
  const [lastCheckTime, setLastCheckTime] = useState<Date | null>(null);

  const isCheckingRef = useRef(false);
  const activeNodeRef = useRef<DiscoveredNode | null>(null);
  activeNodeRef.current = activeNode;

  // Check connection to local Dispersion node
  const checkConnection = useCallback(async () => {
    if (isCheckingRef.current) return;
    isCheckingRef.current = true;
    setIsChecking(true);

    const candidates = [DEFAULT_NODE_URL, '/api/v1'];
    let connectedNode: DiscoveredNode | null = null;

    for (const url of candidates) {
      try {
        const res = await nodesApi.probeNode(url, 2000);
        if (res && res.nodeInfo && res.nodeInfo.status === 'HEALTHY') {
          connectedNode = {
            id: res.nodeInfo.nodeId || `node-${res.nodeInfo.port}`,
            url,
            host: res.nodeInfo.host,
            port: res.nodeInfo.port,
            basePath: res.nodeInfo.basePath,
            nodeInfo: res.nodeInfo,
            pingMs: res.pingMs,
            lastSeen: new Date(),
          };
          break;
        }
      } catch {
        // unreachable
      }
    }

    if (connectedNode) {
      setActiveNode(connectedNode);
      setActiveNodeBaseUrl(connectedNode.url);
      setRetryCountdown(RETRY_INTERVAL_SECONDS);
    } else {
      setActiveNode(null);
      setActiveNodeBaseUrl(DEFAULT_NODE_URL);
    }

    setLastCheckTime(new Date());
    isCheckingRef.current = false;
    setIsChecking(false);
  }, []);

  // Initial check on mount
  useEffect(() => {
    checkConnection();
  }, [checkConnection]);

  // Auto-reconnect countdown timer when disconnected
  useEffect(() => {
    const interval = setInterval(() => {
      if (!activeNodeRef.current) {
        setRetryCountdown((prev) => {
          if (prev <= 1) {
            checkConnection();
            return RETRY_INTERVAL_SECONDS;
          }
          return prev - 1;
        });
      } else {
        setRetryCountdown(RETRY_INTERVAL_SECONDS);
      }
    }, 1000);

    return () => clearInterval(interval);
  }, [checkConnection]);

  const isConnected = !!activeNode;

  return {
    activeNode,
    connectedNodes: activeNode ? [activeNode] : [],
    isConnected,
    isChecking,
    retryCountdown,
    lastCheckTime,
    triggerCheck: checkConnection,
  };
}
