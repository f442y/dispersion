import React from 'react';
import { cn } from '../../utils';

export interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: 'primary' | 'secondary' | 'ghost' | 'outline';
  size?: 'sm' | 'md' | 'icon';
}

export const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(
  ({ className, variant = 'secondary', size = 'md', disabled, children, ...props }, ref) => {
    const variantStyles = {
      primary:
        'bg-zinc-100 hover:bg-white text-zinc-950 font-medium shadow-xs border-transparent',
      secondary:
        'bg-zinc-900 hover:bg-zinc-800 text-zinc-200 border-zinc-800 hover:border-zinc-700',
      ghost:
        'bg-transparent hover:bg-zinc-800/60 text-zinc-400 hover:text-zinc-200 border-transparent',
      outline:
        'bg-transparent hover:bg-zinc-900 text-zinc-300 border-zinc-800 hover:border-zinc-700',
    }[variant];

    const sizeStyles = {
      sm: 'px-2 py-0.5 text-xs',
      md: 'px-3 py-1.5 text-xs',
      icon: 'p-1.5 text-xs',
    }[size];

    return (
      <button
        ref={ref}
        disabled={disabled}
        className={cn(
          'inline-flex items-center justify-center gap-1.5 rounded-md border font-sans transition-colors cursor-pointer disabled:opacity-50 disabled:cursor-not-allowed',
          variantStyles,
          sizeStyles,
          className
        )}
        {...props}
      >
        {children}
      </button>
    );
  }
);

Button.displayName = 'Button';
