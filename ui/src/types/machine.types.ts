export type MachineType = 'TIER_1_ATOMIC_FSM' | 'TIER_2_DISCRETE_SAGA' | 'TIER_3_BATCH';

export interface MachineDescriptor {
  name: string;
  type: MachineType;
  initialState: string;
  endStates: string[];
  allStates: string[];
  mermaidGraph: string;
}

export interface DispatchMachineResponse {
  status: string;
  machineName: string;
}
