import React from 'react';
import { Eye, Send, RefreshCw, Circle } from 'lucide-react';
import type { ExecutionSummary } from '../../types';
import { truncateId } from '../../utils';

interface ExecutionRowProps {
  execution: ExecutionSummary;
  isSignaling: boolean;
  onSelect: (execution: ExecutionSummary) => void;
  onDeliverSignal: (execution: ExecutionSummary) => void;
}

export function ExecutionRow({
  execution,
  isSignaling,
  onSelect,
  onDeliverSignal,
}: ExecutionRowProps) {
  const isSuspended = execution.status === 'SUSPENDED';
  const isRunning = execution.status === 'RUNNING' || execution.status === 'INITIALIZING';
  const signalToDeliver = execution.suspendedSignal || 'PaymentSignal';

  return (
    <tr
      onClick={() => onSelect(execution)}
      className="hover:bg-zinc-900/60 transition cursor-pointer group"
    >
      <td className="py-2.5 text-zinc-200 group-hover:text-white font-medium flex items-center gap-1.5">
        <span>{truncateId(execution.executionId, 8)}</span>
        <Eye className="h-3 w-3 text-zinc-600 group-hover:text-zinc-400" />
      </td>

      <td className="py-2.5 text-zinc-300">
        {execution.correlationKey ? (
          <span className="px-1.5 py-0.2 rounded bg-zinc-900 border border-zinc-800 text-zinc-400 text-[11px]">
            {execution.correlationKey}
          </span>
        ) : (
          <span className="text-zinc-600">-</span>
        )}
      </td>

      <td className="py-2.5 text-zinc-300">
        <span className="inline-flex items-center gap-1.5">
          {isSuspended && <Circle className="h-1.5 w-1.5 fill-amber-400 text-amber-400" />}
          {isRunning && <Circle className="h-1.5 w-1.5 fill-emerald-500 text-emerald-500" />}
          <span>{execution.currentState}</span>
        </span>
      </td>

      <td className="py-2.5">
        <span
          className={`inline-flex items-center gap-1 px-1.5 py-0.2 rounded text-[10px] font-mono ${
            isRunning
              ? 'bg-zinc-800 text-emerald-400 border border-emerald-900/40'
              : isSuspended
              ? 'bg-zinc-800 text-amber-300 border border-amber-900/40'
              : execution.status === 'COMPLETED'
              ? 'bg-zinc-800/80 text-zinc-400 border border-zinc-700/60'
              : 'bg-zinc-800 text-rose-400 border border-rose-900/40'
          }`}
        >
          {isRunning && <span className="h-1 w-1 rounded-full bg-emerald-400 animate-pulse" />}
          {isSuspended && <span className="h-1 w-1 rounded-full bg-amber-400" />}
          <span>{execution.status}</span>
        </span>
      </td>

      <td className="py-2.5 text-zinc-400">{execution.transitionsCount}</td>

      <td className="py-2.5 text-right" onClick={(e: React.MouseEvent) => e.stopPropagation()}>
        <div className="inline-flex items-center gap-1.5">
          <button
            type="button"
            onClick={() => onSelect(execution)}
            className="inline-flex items-center gap-1 px-2 py-0.5 rounded bg-zinc-800 hover:bg-zinc-700 text-zinc-300 text-[11px] transition cursor-pointer"
          >
            <Eye className="h-3 w-3" />
            <span>Inspect</span>
          </button>

          {isSuspended && (
            <button
              type="button"
              disabled={isSignaling}
              onClick={() => onDeliverSignal(execution)}
              className="inline-flex items-center gap-1 px-2 py-0.5 rounded bg-zinc-800 hover:bg-zinc-700 text-amber-300 border border-zinc-700 text-[11px] transition disabled:opacity-50 cursor-pointer"
              title={`Resume with ${signalToDeliver}`}
            >
              {isSignaling ? (
                <RefreshCw className="h-3 w-3 animate-spin" />
              ) : (
                <Send className="h-3 w-3" />
              )}
              <span>Resume ({signalToDeliver})</span>
            </button>
          )}
        </div>
      </td>
    </tr>
  );
}
