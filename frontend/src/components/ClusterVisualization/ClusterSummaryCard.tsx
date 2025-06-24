import React from 'react';
import { useRouter } from 'next/navigation';
import { SimpleTooltip } from '@/components/ui/Tooltip';

interface ClusterSummaryCardProps {
  clusterId: string;
  label: string;
  total: number;
  forCount: number;
  againstCount: number;
  neutralCount: number;
  forPercentage: number;
  againstPercentage: number;
  neutralPercentage?: number;
  dominantStance: string;
  clusterTitle?: string;
  clusterDescription?: string;
}

export default function ClusterSummaryCard({
  clusterId,
  label,
  total,
  forPercentage,
  againstPercentage,
  neutralPercentage,
  dominantStance,
  clusterTitle,
  clusterDescription
}: ClusterSummaryCardProps) {
  const router = useRouter();

  const handleClick = () => {
    router.push(`/clusters/${clusterId}`);
  };

  return (
    <div
      onClick={handleClick}
      className="bg-white border border-gray-200 rounded-lg p-4 hover:shadow-md transition-shadow cursor-pointer"
    >
      <div className="flex justify-between items-start mb-2">
        <div className="flex-1 pr-2">
          <h4 className="font-semibold text-sm text-gray-900 flex items-center gap-1">
            Cluster {clusterId}
            {clusterDescription && (
              <SimpleTooltip content={clusterDescription} className="!w-64">
                <svg className="w-4 h-4 text-gray-400 hover:text-gray-600 cursor-help" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                </svg>
              </SimpleTooltip>
            )}
          </h4>
          <p className="text-xs text-gray-500">{label}</p>
          {clusterTitle && (
            <p className="text-xs text-gray-600 italic mt-1">{clusterTitle}</p>
          )}
        </div>
        <span className="text-xs bg-gray-100 px-2 py-1 rounded whitespace-nowrap">
          {total} comments
        </span>
      </div>
      
      <div className="space-y-2">
        <div className="flex items-center justify-between text-xs">
          <span className="text-green-600">For:</span>
          <div className="flex items-center gap-2">
            <div className="w-20 bg-gray-200 rounded-full h-2 overflow-hidden">
              <div
                className="bg-green-500 h-full"
                style={{ width: `${forPercentage}%` }}
              />
            </div>
            <span className="text-gray-700 w-12 text-right">
              {forPercentage.toFixed(0)}%
            </span>
          </div>
        </div>
        
        <div className="flex items-center justify-between text-xs">
          <span className="text-red-600">Against:</span>
          <div className="flex items-center gap-2">
            <div className="w-20 bg-gray-200 rounded-full h-2 overflow-hidden">
              <div
                className="bg-red-500 h-full"
                style={{ width: `${againstPercentage}%` }}
              />
            </div>
            <span className="text-gray-700 w-12 text-right">
              {againstPercentage.toFixed(0)}%
            </span>
          </div>
        </div>
        
        <div className="flex items-center justify-between text-xs">
          <span className="text-gray-600">Neutral:</span>
          <div className="flex items-center gap-2">
            <div className="w-20 bg-gray-200 rounded-full h-2 overflow-hidden">
              <div
                className="bg-gray-500 h-full"
                style={{ width: `${neutralPercentage ?? Math.max(0, 100 - forPercentage - againstPercentage)}%` }}
              />
            </div>
            <span className="text-gray-700 w-12 text-right">
              {Math.max(0, neutralPercentage ?? (100 - forPercentage - againstPercentage)).toFixed(0)}%
            </span>
          </div>
        </div>
      </div>
      
      <div className="mt-3 pt-3 border-t border-gray-100">
        <p className="text-xs text-gray-600">
          Dominant stance: <span className={
            dominantStance === 'For' ? 'text-green-600 font-medium' : 
            dominantStance === 'Against' ? 'text-red-600 font-medium' : 
            'text-gray-600 font-medium'
          }>{dominantStance}</span>
        </p>
      </div>
    </div>
  );
}