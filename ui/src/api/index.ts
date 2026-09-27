import { machinesApi } from './machines.api';
import { executionsApi } from './executions.api';
import { nodesApi } from './nodes.api';
import { getEventStreamUrl, setActiveNodeBaseUrl, getActiveNodeBaseUrl } from './client';

export * from './client';
export * from './machines.api';
export * from './executions.api';
export * from './nodes.api';

// Unified API facade for clean, single-point access
export const dispersionApi = {
  ...machinesApi,
  ...executionsApi,
  ...nodesApi,
  getEventStreamUrl,
  setActiveNodeBaseUrl,
  getActiveNodeBaseUrl,
};
