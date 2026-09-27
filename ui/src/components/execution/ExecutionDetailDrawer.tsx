import { useState } from 'react';
import { X, AlertTriangle, Copy, Check } from 'lucide-react';
import type { ExecutionSummary } from '../../types';
import { ExecutionMetadataGrid } from './ExecutionMetadataGrid';
import { ExecutionSignalConsole } from './ExecutionSignalConsole';
import { ExecutionTimeline } from './ExecutionTimeline';

interface ExecutionDetailDrawerProps {
  execution: ExecutionSummary | null;
  onClose: () => void;
  onSignalDelivered?: () => void;
}

export function ExecutionDetailDrawer({
  execution,
  onClose,
  onSignalDelivered,
}: ExecutionDetailDrawerProps) {
  const [copied, setCopied] = useState(false);

  if (!execution) return null;

  const handleCopyId = () => {
    navigator.clipboard.writeText(execution.executionId);
    setCopied(true);
    setTimeout(() => setCopied(false), 1500);
  };

  const isSuspended = execution.status === 'SUSPENDED';
  const isRunning = execution.status === 'RUNNING' || execution.status === 'INITIALIZING';

  return (
    <div className="fixed inset-0 z-50 overflow-hidden bg-black/70 backdrop-blur-xs flex justify-end">
      <div className="w-full max-w-xl bg-[#0c0c0e] border-l border-zinc-800 h-full flex flex-col shadow-2xl animate-in slide-in-from-right duration-150">
        {/* Drawer Header */}
        <div className="px-5 py-3.5 border-b border-zinc-800 flex items-center justify-between bg-zinc-950/60">
          <div>
            <div className="flex items-center gap-2">
              <span className="text-xs font-semibold text-zinc-300 uppercase tracking-wider font-mono">
                Instance Detail
              </span>
              <span
                className={`inline-flex items-center gap-1 px-1.5 py-0.2 rounded text-[10px] font-mono ${
                  isRunning
                    ? 'bg-zinc-800 text-emerald-400 border border-emerald-900/40'
                    : isSuspended
                    ? 'bg-zinc-800 text-amber-300 border border-amber-900/40'
                    : execution.status === 'COMPLETED'
                    ? 'bg-zinc-800 text-zinc-400 border border-zinc-700/60'
                    : 'bg-zinc-800 text-rose-400 border border-rose-900/40'
                }`}
              >
                {isRunning && <span className="h-1 w-1 rounded-full bg-emerald-400 animate-pulse" />}
                {isSuspended && <span className="h-1 w-1 rounded-full bg-amber-400" />}
                <span>{execution.status}</span>
              </span>
            </div>
            <div className="flex items-center gap-1.5 mt-1">
              <span className="font-mono text-xs text-zinc-400 select-all">
                {execution.executionId}
              </span>
              <button
                type="button"
                onClick={handleCopyId}
                className="text-zinc-500 hover:text-zinc-300 transition p-0.5 cursor-pointer"
                title="Copy instance ID"
              >
                {copied ? <Check className="h-3 w-3 text-emerald-400" /> : <Copy className="h-3 w-3" />}
              </button>
            </div>
          </div>

          <button
            type="button"
            onClick={onClose}
            className="p-1 rounded text-zinc-400 hover:text-zinc-200 hover:bg-zinc-800 transition cursor-pointer"
          >
            <X className="h-4 w-4" />
          </button>
        </div>

        {/* Drawer Scrollable Content */}
        <div className="flex-1 overflow-y-auto px-5 py-4 space-y-5">
          {/* Metadata Grid */}
          <ExecutionMetadataGrid execution={execution} />

          {/* Error Banner if any */}
          {execution.errorMessage && (
            <div className="p-3 rounded-md bg-rose-950/20 border border-rose-900/40 text-xs font-mono space-y-1">
              <div className="flex items-center gap-1.5 text-rose-400 font-medium">
                <AlertTriangle className="h-3.5 w-3.5" />
                <span>Execution Error / Compensation Triggered</span>
              </div>
              <div className="text-rose-300 select-all pl-5">{execution.errorMessage}</div>
            </div>
          )}

          {/* Interactive Signal Console */}
          <ExecutionSignalConsole
            execution={execution}
            onSignalDelivered={onSignalDelivered}
          />

          {/* Chronological Event Timeline / Audit Trail */}
          <ExecutionTimeline
            executionId={execution.executionId}
            isLive={isRunning || isSuspended}
          />
        </div>
      </div>
    </div>
  );
}
