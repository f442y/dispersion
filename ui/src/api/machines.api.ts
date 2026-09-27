import { request } from './client';
import type { MachineDescriptor, DispatchMachineResponse } from '../types';

export const machinesApi = {
  async listMachines(customBase?: string): Promise<MachineDescriptor[]> {
    return request<MachineDescriptor[]>('/machines', { method: 'GET' }, customBase);
  },

  async getMachine(name: string, customBase?: string): Promise<MachineDescriptor> {
    return request<MachineDescriptor>(
      `/machines/${encodeURIComponent(name)}`,
      { method: 'GET' },
      customBase
    );
  },

  async dispatchMachine(
    name: string,
    payload?: unknown,
    customBase?: string
  ): Promise<DispatchMachineResponse> {
    return request<DispatchMachineResponse>(
      `/machines/${encodeURIComponent(name)}/dispatch`,
      {
        method: 'POST',
        body: JSON.stringify(payload ?? {}),
      },
      customBase
    );
  },
};
