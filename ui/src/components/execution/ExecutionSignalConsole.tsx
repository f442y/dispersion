import { useState } from 'react';
import { Send, RefreshCw } from 'lucide-react';
import type { ExecutionSummary } from '../../types';
import { useSendSignalMutation } from '../../hooks';
import { toast } from 'sonner';

interface ExecutionSignalConsoleProps {
  execution: ExecutionSummary;
  onSignalDelivered?: () => void;
}

export function ExecutionSignalConsole({
  execution,
  onSignalDelivered,
}: ExecutionSignalConsoleProps) {
  const [signalName, setSignalName] = useState<string>('');
  const [payloadText, setPayloadText] = useState<string>(
    JSON.stringify(
      {
        paymentMethod: 'CREDIT_CARD',
        amountCents: 9995,
        authCode: 'AUTH-' + Math.floor(1000 + Math.random() * 9000),
      },
      null,
      2
    )
  );

  const sendSignalMutation = useSendSignalMutation();

  const isSuspended = execution.status === 'SUSPENDED';
  const effectiveSignalName =
    signalName || execution.suspendedSignal || 'PaymentSignal';

  const handleSendSignal = (e: React.FormEvent) => {
    e.preventDefault();

    let parsedPayload: Record<string, unknown> | null = null;
    try {
      if (payloadText.trim()) {
        parsedPayload = JSON.parse(payloadText);
      }
    } catch {
      toast.error('Invalid JSON payload');
      return;
    }

    const key = execution.correlationKey || 'ORDER-DEMO-99';

    sendSignalMutation.mutate(
      {
        machineName: execution.machineName,
        correlationKey: key,
        signalName: effectiveSignalName,
        payload: parsedPayload,
      },
      {
        onSuccess: (res) => {
          if (res.delivered) {
            toast.success(`Signal [${effectiveSignalName}] Delivered`, {
              description: `Resulting state: ${res.resultingState ?? 'Next Step'}`,
            });
            if (onSignalDelivered) onSignalDelivered();
          } else {
            toast.warning(res.message || 'Signal not delivered', {
              description: res.errorMessage ?? 'Execution was not in an awaitable state.',
            });
          }
        },
        onError: (err) => {
          const msg = err instanceof Error ? err.message : String(err);
          toast.error('Signal delivery error', { description: msg });
        },
      }
    );
  };

  return (
    <div className="rounded-md border border-zinc-800/80 bg-zinc-900/20 p-3.5 space-y-3">
      <div className="flex items-center justify-between pb-2 border-b border-zinc-800/60">
        <div className="flex items-center gap-1.5">
          <Send className="h-3.5 w-3.5 text-zinc-400" />
          <h4 className="text-xs font-semibold text-zinc-200 uppercase tracking-wider font-mono">
            Signal Injection
          </h4>
        </div>
        {isSuspended && (
          <span className="text-[10px] font-mono px-1.5 py-0.2 rounded bg-zinc-800 text-amber-300 border border-amber-900/50">
            Awaiting: {execution.suspendedSignal || 'PaymentSignal'}
          </span>
        )}
      </div>

      <p className="text-xs text-zinc-400">
        Send an external event/signal to resume this suspended workflow turn.
      </p>

      <form onSubmit={handleSendSignal} className="space-y-2.5 pt-0.5">
        <div>
          <label className="block text-[11px] font-mono text-zinc-400 mb-1">
            Signal Name
          </label>
          <input
            type="text"
            value={effectiveSignalName}
            onChange={(e) => setSignalName(e.target.value)}
            placeholder="e.g. PaymentSignal, CancelOrderCommand"
            className="w-full px-2.5 py-1.5 rounded bg-zinc-900 border border-zinc-800 text-zinc-200 font-mono text-xs focus:outline-hidden focus:border-zinc-500"
          />
        </div>

        <div>
          <label className="block text-[11px] font-mono text-zinc-400 mb-1">
            Payload (JSON)
          </label>
          <textarea
            rows={4}
            value={payloadText}
            onChange={(e) => setPayloadText(e.target.value)}
            className="w-full px-2.5 py-1.5 rounded bg-zinc-900 border border-zinc-800 text-zinc-300 font-mono text-xs focus:outline-hidden focus:border-zinc-500"
          />
        </div>

        <div className="flex justify-end pt-1">
          <button
            type="submit"
            disabled={sendSignalMutation.isPending}
            className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded bg-zinc-100 hover:bg-white text-zinc-900 font-medium text-xs transition disabled:opacity-50 cursor-pointer"
          >
            {sendSignalMutation.isPending ? (
              <RefreshCw className="h-3 w-3 animate-spin" />
            ) : (
              <Send className="h-3 w-3" />
            )}
            <span>Deliver Signal</span>
          </button>
        </div>
      </form>
    </div>
  );
}
