import { StanceData } from '@/lib/types/timeSeries';

export type { StanceData } from '@/lib/types/timeSeries';

export interface ChartProps {
  data: StanceData[];
  className?: string;
}

export type ChartType = 'svg' | 'nivo';