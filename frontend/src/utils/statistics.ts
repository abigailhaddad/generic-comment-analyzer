export interface StatColors {
  bg: string;
  text: string;
  border: string;
}

export function getStatColors(key: string): StatColors {
  switch (key) {
    case 'for':
      return { bg: 'bg-emerald-50', text: 'text-emerald-700', border: 'border-emerald-200' };
    case 'against':
      return { bg: 'bg-rose-50', text: 'text-rose-700', border: 'border-rose-200' };
    case 'neutral':
      return { bg: 'bg-slate-50', text: 'text-slate-700', border: 'border-slate-200' };
    default:
      // Default for total and other stats
      return { bg: 'bg-slate-50', text: 'text-slate-700', border: 'border-slate-200' };
  }
}

export interface StanceColors {
  [key: string]: string;
}

export const stanceColors: StanceColors = {
  For: '#10b981',      // Softer emerald
  Against: '#f43f5e',  // Softer rose
  'Neutral/Unclear': '#64748b', // Slate
};

export function getStanceBadgeClasses(stance: string): string {
  switch (stance) {
    case 'For':
      return 'bg-emerald-100 text-emerald-700';
    case 'Against':
      return 'bg-rose-100 text-rose-700';
    case 'Neutral/Unclear':
    default:
      return 'bg-slate-100 text-slate-700';
  }
}