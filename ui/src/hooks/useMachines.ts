import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { machinesApi } from '../api/machines.api';
import { machineKeys, executionKeys } from './queryKeys';
import type { MachineDescriptor } from '../types';

export function useMachinesQuery(nodeId?: string, enabled: boolean = true) {
  return useQuery<MachineDescriptor[]>({
    queryKey: machineKeys.list(nodeId),
    queryFn: () => machinesApi.listMachines(),
    enabled,
    retry: 1,
  });
}

export function useMachineQuery(name?: string, nodeId?: string, enabled: boolean = true) {
  return useQuery<MachineDescriptor>({
    queryKey: machineKeys.detail(nodeId, name),
    queryFn: () => (name ? machinesApi.getMachine(name) : Promise.reject('No machine name')),
    enabled: enabled && !!name,
  });
}

export function useDispatchMachineMutation() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({ name, payload }: { name: string; payload?: unknown }) =>
      machinesApi.dispatchMachine(name, payload),
    onSuccess: () => {
      // Invalidate execution queries so new instance appears immediately
      queryClient.invalidateQueries({ queryKey: executionKeys.all });
    },
  });
}
