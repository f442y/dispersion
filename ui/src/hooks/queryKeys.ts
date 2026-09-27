import type { ListExecutionsFilter } from '../types';

export const machineKeys = {
  all: ['machines'] as const,
  lists: () => [...machineKeys.all, 'list'] as const,
  list: (nodeId?: string) => [...machineKeys.lists(), { nodeId }] as const,
  details: () => [...machineKeys.all, 'detail'] as const,
  detail: (nodeId?: string, name?: string) => [...machineKeys.details(), { nodeId, name }] as const,
};

export const executionKeys = {
  all: ['executions'] as const,
  lists: () => [...executionKeys.all, 'list'] as const,
  list: (nodeId?: string, filter?: ListExecutionsFilter) =>
    [...executionKeys.lists(), { nodeId, ...filter }] as const,
  details: () => [...executionKeys.all, 'detail'] as const,
  detail: (id: string) => [...executionKeys.details(), id] as const,
  timelines: () => [...executionKeys.all, 'timeline'] as const,
  timeline: (id: string) => [...executionKeys.timelines(), id] as const,
};

export const nodeKeys = {
  all: ['nodes'] as const,
  probe: (url: string) => [...nodeKeys.all, 'probe', url] as const,
  info: (url?: string) => [...nodeKeys.all, 'info', url] as const,
};
