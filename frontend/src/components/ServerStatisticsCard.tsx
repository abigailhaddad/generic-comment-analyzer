// components/ServerStatisticsCard.tsx
'use client';

import { useServerDataContext } from '@/contexts/ServerDataContext';
import { useState, useEffect } from 'react';
import Card from '@/components/ui/Card';
import Skeleton from '@/components/ui/Skeleton';
import { getStatColors } from '@/utils/statistics';

export default function ServerStatisticsCard() {
  const { stats, loading } = useServerDataContext();
  
  // Create a state to hold the displayed values
  const [displayedStats, setDisplayedStats] = useState(stats);
  // State to track if it's the very first load sequence
  const [isInitialLoad, setIsInitialLoad] = useState(true);
  
  // Update displayed stats when new stats arrive and loading is complete
  useEffect(() => {
    if (!loading) {
      setDisplayedStats(stats);
      // Once data is loaded (or attempted to load and loading is false),
      // it's no longer the "initial load" for placeholder purposes.
      if (isInitialLoad) {
        setIsInitialLoad(false);
      }
    }
  }, [stats, loading, isInitialLoad]);
  
  
  // Create statistics data with labels and values
  const statisticsData = [
    { key: 'total', label: 'Total Comments', value: displayedStats.total },
    { key: 'for', label: 'For', value: displayedStats.for, match: 'For' },
    { key: 'against', label: 'Against', value: displayedStats.against, match: 'Against' },
    { key: 'neutral', label: 'Neutral/Unclear', value: displayedStats.neutral, match: 'Neutral/Unclear' }
  ];

  // Placeholder component for individual stat
  const StatPlaceholder = () => (
    <div className="p-4 rounded-lg border shadow-sm bg-gray-100">
      <Skeleton variant="text" width="75%" className="mb-2" />
      <Skeleton variant="text" width="50%" height={32} className="mb-1" />
      <Skeleton variant="text" width="25%" className="mt-2" />
    </div>
  );

  // Loading state display - show during initial load OR when data is updating
  if (loading) {
    return (
      <Card className="overflow-hidden shadow-sm" collapsible={true} initiallyCollapsed={false}>
        <Card.Header className="px-6 py-4 bg-gradient-to-r from-white to-slate-50 border-b border-slate-200">
          <h5 className="text-lg font-semibold text-slate-800 flex items-center">
            <span className="mr-2 opacity-60">📊</span>
            Statistics Overview
          </h5>
        </Card.Header>
        <Card.Body className="p-6">
          <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4">
            {[...Array(4)].map((_, index) => (
              <StatPlaceholder key={index} />
            ))}
          </div>
        </Card.Body>
      </Card>
    );
  }
  
  // Main display of statistics
  return (
    <Card className="overflow-hidden shadow-sm" collapsible={true} initiallyCollapsed={false}>
      <Card.Header className="px-6 py-4 bg-gradient-to-r from-white to-slate-50 border-b border-slate-200">
        <h5 className="text-lg font-semibold text-slate-800 flex items-center">
          <span className="mr-2 opacity-60">📊</span>
          Statistics Overview
        </h5>
      </Card.Header>
      <Card.Body className="p-6">
        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4">
          {statisticsData.map((stat) => {
            const colors = getStatColors(stat.key);
            return (
              <div 
                key={stat.key} 
                className={`p-4 rounded-lg border shadow-sm ${colors.bg} ${colors.border}`}
              >
                <p className={`text-sm uppercase font-semibold mb-1 ${colors.text} ${stat.key === 'neutral' ? 'break-words text-xs' : ''}`}>
                  {stat.key === 'neutral' ? (
                    <>NEUTRAL/<wbr />UNCLEAR</>
                  ) : (
                    stat.label
                  )}
                </p>
                <h3 className={`text-3xl font-bold mb-1 ${colors.text}`}>
                  {(typeof stat.value === 'number' ? stat.value.toLocaleString() : '0')}
                </h3>
                {stat.match && (
                  <div className="mt-2 flex items-center">
                    <span className={`px-2 py-1 rounded-full text-xs font-medium ${colors.bg} ${colors.text} border ${colors.border} ${stat.key === 'neutral' ? 'break-words' : ''}`}>
                      {stat.key === 'neutral' ? (
                        <>Neutral/<wbr />Unclear</>
                      ) : (
                        stat.match
                      )}
                    </span>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </Card.Body>
    </Card>
  );
}