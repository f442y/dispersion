let activeBaseUrl = '/api/v1';

export function setActiveNodeBaseUrl(url: string): void {
  activeBaseUrl = url.endsWith('/') ? url.slice(0, -1) : url;
}

export function getActiveNodeBaseUrl(): string {
  return activeBaseUrl;
}

export async function request<T>(
  path: string,
  options: RequestInit = {},
  customBase?: string
): Promise<T> {
  const base = customBase ?? activeBaseUrl;
  const cleanPath = path.startsWith('/') ? path : `/${path}`;
  const url = `${base}${cleanPath}`;

  const res = await fetch(url, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/json',
      ...options.headers,
    },
  });

  if (!res.ok) {
    const errorText = await res.text().catch(() => res.statusText);
    throw new Error(`API Error (${res.status}): ${errorText}`);
  }

  return res.json() as Promise<T>;
}

export interface EventStreamUrlOptions {
  machineName?: string;
  executionId?: string;
  tier?: 'lifecycle' | 'all';
}

export function getEventStreamUrl(
  optionsOrMachineName?: string | EventStreamUrlOptions,
  customBase?: string
): string {
  const base = customBase ?? activeBaseUrl;
  const query = new URLSearchParams();

  if (typeof optionsOrMachineName === 'string') {
    if (optionsOrMachineName) query.set('machine', optionsOrMachineName);
  } else if (optionsOrMachineName) {
    if (optionsOrMachineName.machineName) query.set('machine', optionsOrMachineName.machineName);
    if (optionsOrMachineName.executionId) query.set('executionId', optionsOrMachineName.executionId);
    if (optionsOrMachineName.tier) query.set('tier', optionsOrMachineName.tier);
  }

  const qs = query.toString();
  return `${base}/events/stream${qs ? `?${qs}` : ''}`;
}
