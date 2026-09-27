import { useState, useRef, useEffect } from 'react';
import {
  Workflow,
  Box,
  GitFork,
  Play,
  RefreshCw,
  ChevronDown,
  Layers,
  Network,
  RotateCcw,
} from 'lucide-react';
import type { MachineDescriptor } from '../../types';
import { Button } from '../common/Button';

interface FlowHeaderProps {
  flow: MachineDescriptor;
  viewMode: 'pipeline' | 'mermaid';
  onViewModeChange: (mode: 'pipeline' | 'mermaid') => void;
  isDispatching: boolean;
  onQuickDispatch: () => void;
  onOpenCustomDispatch: () => void;
  onClearFilter?: () => void;
  hasFilter?: boolean;
}

export function FlowHeader({
  flow,
  viewMode,
  onViewModeChange,
  isDispatching,
  onQuickDispatch,
  onOpenCustomDispatch,
  onClearFilter,
  hasFilter,
}: FlowHeaderProps) {
  const [dropdownOpen, setDropdownOpen] = useState(false);
  const dropdownRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handleOutsideClick = (e: MouseEvent) => {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target as Node)) {
        setDropdownOpen(false);
      }
    };
    document.addEventListener('mousedown', handleOutsideClick);
    return () => document.removeEventListener('mousedown', handleOutsideClick);
  }, []);

  const getIcon = () => {
    switch (flow.type) {
      case 'TIER_2_DISCRETE_SAGA':
        return <Workflow className="h-4 w-4 text-zinc-300" />;
      case 'TIER_3_BATCH':
        return <Box className="h-4 w-4 text-zinc-300" />;
      case 'TIER_1_ATOMIC_FSM':
      default:
        return <GitFork className="h-4 w-4 text-zinc-300" />;
    }
  };

  const getTypeLabel = () => {
    switch (flow.type) {
      case 'TIER_2_DISCRETE_SAGA':
        return 'Saga Orchestration';
      case 'TIER_3_BATCH':
        return 'Batch Flow';
      case 'TIER_1_ATOMIC_FSM':
      default:
        return 'Atomic FSM';
    }
  };

  return (
    <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 pb-4 border-b border-zinc-800/80">
      <div className="space-y-1">
        <div className="flex items-center gap-2">
          {getIcon()}
          <h2 className="text-base font-bold font-mono text-zinc-100">{flow.name}</h2>
          <span className="text-[10px] font-mono px-2 py-0.5 rounded bg-zinc-800 text-zinc-300 border border-zinc-700/60">
            {getTypeLabel()}
          </span>
        </div>
        <div className="flex flex-wrap items-center gap-3 text-xs text-zinc-500 font-mono">
          <span>Initial: <code className="text-zinc-300">{flow.initialState}</code></span>
          <span>&bull;</span>
          <span>End States: <code className="text-zinc-300">{flow.endStates.join(', ')}</code></span>
          <span>&bull;</span>
          <span>{flow.allStates.length} defined states</span>
        </div>
      </div>

      <div className="flex items-center gap-2">
        {hasFilter && onClearFilter && (
          <Button
            variant="ghost"
            size="sm"
            onClick={onClearFilter}
            title="Reset state filter"
          >
            <RotateCcw className="h-3 w-3" />
            <span>Reset Filter</span>
          </Button>
        )}

        {/* View Mode Toggle */}
        <div className="flex items-center rounded-md border border-zinc-800 bg-zinc-900/60 p-0.5 text-xs font-mono">
          <button
            type="button"
            onClick={() => onViewModeChange('pipeline')}
            className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded text-xs transition cursor-pointer ${
              viewMode === 'pipeline'
                ? 'bg-zinc-800 text-zinc-100 shadow-xs'
                : 'text-zinc-400 hover:text-zinc-200'
            }`}
          >
            <Layers className="h-3 w-3" />
            <span>Pipeline</span>
          </button>
          <button
            type="button"
            onClick={() => onViewModeChange('mermaid')}
            className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded text-xs transition cursor-pointer ${
              viewMode === 'mermaid'
                ? 'bg-zinc-800 text-zinc-100 shadow-xs'
                : 'text-zinc-400 hover:text-zinc-200'
            }`}
          >
            <Network className="h-3 w-3" />
            <span>Topology</span>
          </button>
        </div>

        {/* Run Flow Action Button with Split Dropdown */}
        <div className="relative inline-flex" ref={dropdownRef}>
          <button
            type="button"
            disabled={isDispatching}
            onClick={onQuickDispatch}
            className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-l-md bg-zinc-100 hover:bg-white text-zinc-950 text-xs font-medium transition cursor-pointer disabled:opacity-50"
          >
            {isDispatching ? (
              <RefreshCw className="h-3 w-3 animate-spin" />
            ) : (
              <Play className="h-3 w-3 fill-current" />
            )}
            <span>Run Flow</span>
          </button>

          <button
            type="button"
            disabled={isDispatching}
            onClick={() => setDropdownOpen(!dropdownOpen)}
            className="px-1.5 py-1.5 rounded-r-md bg-zinc-200 hover:bg-white text-zinc-950 border-l border-zinc-300 text-xs transition cursor-pointer disabled:opacity-50"
            title="Additional dispatch options"
          >
            <ChevronDown className="h-3 w-3" />
          </button>

          {dropdownOpen && (
            <div className="absolute right-0 top-full mt-1 w-48 rounded-md bg-zinc-900 border border-zinc-800 py-1 shadow-xl z-20 text-xs font-mono">
              <button
                type="button"
                onClick={() => {
                  setDropdownOpen(false);
                  onOpenCustomDispatch();
                }}
                className="w-full text-left px-3 py-1.5 hover:bg-zinc-800 text-zinc-300 hover:text-white transition cursor-pointer"
              >
                Run with Custom JSON...
              </button>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
