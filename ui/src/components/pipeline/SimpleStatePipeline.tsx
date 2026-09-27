import type { MachineDescriptor, ExecutionSummary } from '../../types';
import { StateStepPill } from './StateStepPill';

interface SimpleStatePipelineProps {
  machine: MachineDescriptor;
  executions: ExecutionSummary[];
  selectedStateFilter: string | null;
  onSelectStateFilter: (stateName: string | null) => void;
}

export function SimpleStatePipeline({
  machine,
  executions,
  selectedStateFilter,
  onSelectStateFilter,
}: SimpleStatePipelineProps) {
  const states = machine.allStates && machine.allStates.length > 0
    ? machine.allStates
    : [machine.initialState, ...machine.endStates];

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between text-xs text-zinc-500 font-mono">
        <span>Execution Pipeline</span>
        {selectedStateFilter && (
          <button
            type="button"
            onClick={() => onSelectStateFilter(null)}
            className="text-zinc-400 hover:text-zinc-200 underline cursor-pointer"
          >
            Clear state filter: {selectedStateFilter}
          </button>
        )}
      </div>

      <div className="flex items-center gap-2 overflow-x-auto py-2 pr-2">
        {states.map((st, idx) => {
          const isInitial = st === machine.initialState;
          const isEnd = machine.endStates.includes(st);
          const isLast = idx === states.length - 1;
          const isSelected = selectedStateFilter === st;
          const stateExecs = executions.filter((e) => e.currentState === st);

          return (
            <StateStepPill
              key={st}
              stateName={st}
              isInitial={isInitial}
              isEnd={isEnd}
              isLast={isLast}
              isSelected={isSelected}
              stateExecutions={stateExecs}
              onSelectState={(name) => {
                if (selectedStateFilter === name) {
                  onSelectStateFilter(null);
                } else {
                  onSelectStateFilter(name);
                }
              }}
            />
          );
        })}
      </div>
    </div>
  );
}
