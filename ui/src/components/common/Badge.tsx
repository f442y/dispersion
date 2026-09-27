import React from 'react';
import { cn } from '../../utils';

export interface BadgeProps extends React.HTMLAttributes<HTMLSpanElement> {
  variant?: 'default' | 'neutral' | 'success' | 'warning' | 'danger' | 'outline';
}

export function Badge({
  className,
  variant = 'default',
  children,
  ...props
}: BadgeProps) {
  const variantStyles = {
    default: 'bg-zinc-800 text-zinc-300 border-zinc-700/60',
    neutral: 'bg-zinc-900 text-zinc-400 border-zinc-800',
    success: 'bg-zinc-900 text-emerald-400 border-emerald-900/40',
    warning: 'bg-zinc-900 text-amber-300 border-amber-900/40',
    danger: 'bg-zinc-900 text-rose-400 border-rose-900/40',
    outline: 'bg-transparent text-zinc-400 border-zinc-800',
  }[variant];

  return (
    <span
      className={cn(
        'inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-mono font-medium border transition-colors',
        variantStyles,
        className
      )}
      {...props}
    >
      {children}
    </span>
  );
}
