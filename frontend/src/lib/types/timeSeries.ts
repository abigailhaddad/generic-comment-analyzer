// src/lib/types/timeSeries.ts
export interface StanceData {
    date: string; // ISO format YYYY-MM-DD
    For: number;
    Against: number;
    'Neutral/Unclear': number;
  }
  
  export interface TimeSeriesResponse {
    success: boolean;
    data?: StanceData[];
    error?: string;
  }