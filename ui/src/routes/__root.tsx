import { createRootRoute, Outlet } from '@tanstack/react-router';
import { Cpu, Circle } from 'lucide-react';
import { Toaster } from 'sonner';

export const Route = createRootRoute({
  component: RootComponent,
});

function RootComponent() {
  return (
    <div className="min-h-screen w-full bg-[#09090b] text-zinc-100 font-sans flex flex-col selection:bg-zinc-800 selection:text-white">
      {/* Refined minimalist header */}
      <header className="h-13 border-b border-zinc-800/80 bg-[#09090b]/90 backdrop-blur sticky top-0 z-40 px-6 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="h-7 w-7 rounded-md bg-zinc-900 border border-zinc-700/60 flex items-center justify-center text-zinc-200">
            <Cpu className="h-3.5 w-3.5" />
          </div>
          <div className="flex items-center gap-2">
            <span className="font-semibold text-xs tracking-wider text-zinc-100 font-mono uppercase">
              Dispersion
            </span>
            <span className="text-[10px] font-mono text-zinc-500 border-l border-zinc-800 pl-2">
              Control Plane
            </span>
          </div>
        </div>

        <div className="flex items-center gap-3 text-xs font-mono text-zinc-400">
          <div className="flex items-center gap-1.5 text-zinc-400">
            <Circle className="h-2 w-2 fill-emerald-500 text-emerald-500" />
            <span className="text-zinc-400 text-[11px]">System Online</span>
          </div>
        </div>
      </header>

      {/* Main Content Viewport */}
      <main className="flex-1 w-full max-w-7xl mx-auto p-4 sm:p-6 lg:p-8">
        <Outlet />
      </main>

      <Toaster position="bottom-right" theme="dark" />
    </div>
  );
}
