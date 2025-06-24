// frontend/src/components/StanceOverTime/StanceOverTimeClient.tsx
'use client';

import { useState, useMemo } from 'react';
import dynamic from 'next/dynamic';
import Card from '@/components/ui/Card';
import LoadingSkeleton from './LoadingSkeleton';
import { useServerDataContext } from '@/contexts/ServerDataContext';
import { StanceData } from './types';

// Dynamically import the Nivo chart to avoid SSR issues
const TimeSeriesChart = dynamic(() => import('./NivoTimeSeriesChart'), {
  ssr: false,
  loading: () => <LoadingSkeleton />
});

export default function StanceOverTimeClient() {
  const { stanceTimeSeriesData, loading: contextLoading } = useServerDataContext();

  const [selectedDateType, setSelectedDateType] = useState<'posted_date' | 'received_date'>('posted_date');
  const [includeDuplicates, setIncludeDuplicates] = useState(true); // New state

  const chartData = useMemo((): StanceData[] => {
    if (!stanceTimeSeriesData) return [];
    
    // Select the appropriate data based on both toggles
    if (includeDuplicates) {
      return selectedDateType === 'posted_date' 
        ? stanceTimeSeriesData.posted_date 
        : stanceTimeSeriesData.received_date;
    } else {
      return selectedDateType === 'posted_date'
        ? stanceTimeSeriesData.posted_date_no_duplicates
        : stanceTimeSeriesData.received_date_no_duplicates;
    }
  }, [stanceTimeSeriesData, selectedDateType, includeDuplicates]);

  if (contextLoading && !stanceTimeSeriesData) {
    return <LoadingSkeleton />;
  }

  if (stanceTimeSeriesData?.error) {
    return (
      <Card className="border-red-200" collapsible={false}>
        <Card.Header className="bg-gradient-to-r from-red-500 to-red-600 flex justify-between items-center">
          <h5 className="text-lg font-bold text-white flex items-center">
            <span className="mr-2">⚠️</span>
            Chart Error
          </h5>
        </Card.Header>
        <Card.Body className="text-center py-8">
          <p className="text-red-500">Error loading chart data: {stanceTimeSeriesData.error}</p>
        </Card.Body>
      </Card>
    );
  }
  
  if (!stanceTimeSeriesData) {
    return <LoadingSkeleton />;
  }

  return (
    <Card collapsible={true} initiallyCollapsed={false} className="shadow-sm">
      <Card.Header className="bg-gradient-to-r from-slate-50 to-blue-50 border-b border-slate-200">
        <div className="flex justify-between items-center w-full">
          <h5 className="text-lg font-semibold text-slate-800 flex items-center">
            <span className="mr-2 opacity-60">📈</span>
            Comments Over Time
          </h5>
          <div className="flex flex-col md:flex-row items-end md:items-center gap-3">
            {/* Duplicate toggle */}
            <label className="flex items-center gap-2 text-slate-700 text-sm cursor-pointer">
              <input
                type="checkbox"
                checked={includeDuplicates}
                onChange={(e) => setIncludeDuplicates(e.target.checked)}
                className="w-4 h-4 rounded border-slate-300 text-slate-600 focus:ring-slate-500 focus:ring-opacity-50"
                aria-label="Include duplicate comments in the chart"
              />
              <span className="select-none">Include Duplicates</span>
            </label>
            
            {/* Date selector */}
            <select
              value={selectedDateType}
              onChange={(e) => setSelectedDateType(e.target.value as 'posted_date' | 'received_date')}
              className="bg-white text-slate-700 text-sm px-3 py-1 rounded-md focus:outline-none focus:ring-2 focus:ring-slate-400 focus:ring-opacity-50 border border-slate-200"
              aria-label="Select the appropriate date that you would like to see for the chart"
            >
              <option value="posted_date" className="text-gray-700">By Posted Date</option>
              <option value="received_date" className="text-gray-700">By Received Date</option>
            </select>
          </div>
        </div>
      </Card.Header>
      <Card.Body className="p-4">
        <TimeSeriesChart data={chartData} />
        {!includeDuplicates && (
          <p className="text-sm text-gray-500 mt-2 text-center">
            Showing unique comments only (duplicates excluded)
          </p>
        )}
      </Card.Body>
    </Card>
  );
}