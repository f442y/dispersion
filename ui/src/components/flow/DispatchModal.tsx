import { useState } from 'react';
import { Play, RefreshCw } from 'lucide-react';
import { Modal } from '../common/Modal';
import { Button } from '../common/Button';
import { toast } from 'sonner';

interface DispatchModalProps {
  isOpen: boolean;
  onClose: () => void;
  flowName: string;
  isDispatching: boolean;
  onDispatch: (payload: unknown) => void;
}

export function DispatchModal({
  isOpen,
  onClose,
  flowName,
  isDispatching,
  onDispatch,
}: DispatchModalProps) {
  const [customInputJson, setCustomInputJson] = useState<string>('{}');

  const handleConfirm = () => {
    try {
      const parsed = JSON.parse(customInputJson);
      onDispatch(parsed);
    } catch {
      toast.error('Invalid JSON payload');
    }
  };

  return (
    <Modal
      isOpen={isOpen}
      onClose={onClose}
      title={
        <>
          <Play className="h-3.5 w-3.5 text-zinc-300 fill-current" />
          <span>Dispatch Run: {flowName}</span>
        </>
      }
    >
      <p className="text-xs text-zinc-400 leading-relaxed">
        Provide optional JSON payload parameters for this execution. Leave as{' '}
        <code className="text-zinc-200">&#123;&#125;</code> to use defaults.
      </p>

      <div>
        <label className="block text-[11px] font-mono text-zinc-400 mb-1">
          Payload (JSON)
        </label>
        <textarea
          rows={5}
          value={customInputJson}
          onChange={(e) => setCustomInputJson(e.target.value)}
          className="w-full px-3 py-2 rounded bg-zinc-900 border border-zinc-800 text-zinc-200 font-mono text-xs focus:outline-hidden focus:border-zinc-500"
        />
      </div>

      <div className="flex items-center justify-end gap-2 pt-1">
        <Button variant="ghost" size="sm" onClick={onClose}>
          Cancel
        </Button>
        <Button
          variant="primary"
          size="sm"
          disabled={isDispatching}
          onClick={handleConfirm}
        >
          {isDispatching ? (
            <RefreshCw className="h-3 w-3 animate-spin" />
          ) : (
            <Play className="h-3 w-3 fill-current" />
          )}
          <span>Dispatch</span>
        </Button>
      </div>
    </Modal>
  );
}
