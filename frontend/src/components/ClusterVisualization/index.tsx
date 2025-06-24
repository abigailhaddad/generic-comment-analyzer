// src/components/ClusterVisualization/index.tsx
"use client";

import React, { useState, useMemo, useEffect } from 'react';
import { ClusterData } from "@/lib/actions/clusters";
import ClusterChart from './ClusterChart';
import ClusterControls from "./ClusterControls";
import ClusterSummaryCard from './ClusterSummaryCard';
import Card from "@/components/ui/Card";
import LoadingSpinner from '@/components/ui/LoadingSpinner';
import { usePathname } from 'next/navigation';

interface ClusterVisualizationProps {
  initialData: ClusterData | null;
  isLoading: boolean;
  error: string | null;
  initialSelectedCluster?: string | null;
}

const ClusterVisualization: React.FC<ClusterVisualizationProps> = ({ initialData, isLoading, error, initialSelectedCluster }) => {
  const pathname = usePathname();
  const [clusterData, setClusterData] = useState<ClusterData | null>(initialData);
  const [selectedClusterId, setSelectedClusterId] = useState<string | null>(initialSelectedCluster || null);

  // Update selected cluster based on URL changes
  useEffect(() => {
    const pathParts = pathname.split('/');
    const clusterFromUrl = pathParts.length > 2 && pathParts[1] === 'clusters' ? pathParts[2] : null;
    setSelectedClusterId(clusterFromUrl);
  }, [pathname]);


  React.useEffect(() => {
    setClusterData(initialData);
  }, [initialData]);

  // Data for the chart, formatted as Nivo expects series.
  const chartSeriesData = useMemo(() => {
    if (!clusterData) return [];
    return clusterData.clusters.map(([id, points]) => ({
      id: id,
      data: points.map(p => ({ ...p, x: p.pcaX, y: p.pcaY })),
    }));
  }, [clusterData]);

  
  // Data for ClusterChart component: if a cluster is selected, only pass that series
  const chartDisplayData = useMemo(() => {
    if (!selectedClusterId || !chartSeriesData) return chartSeriesData;
    return chartSeriesData.filter(series => series.id === selectedClusterId);
  }, [chartSeriesData, selectedClusterId]);

  // Get cluster-specific information
  const selectedClusterInfo = useMemo(() => {
    if (!selectedClusterId || !clusterData) return null;
    const clusterPoints = clusterData.clusters.find(([id]) => id === selectedClusterId)?.[1];
    if (!clusterPoints) return null;

    // Calculate statistics for the selected cluster
    const stanceCounts = clusterPoints.reduce((acc, point) => {
      acc[point.stance || 'Neutral/Unclear'] = (acc[point.stance || 'Neutral/Unclear'] || 0) + 1;
      return acc;
    }, {} as Record<string, number>);

    return {
      totalComments: clusterPoints.length,
      stanceCounts,
      sampleComments: clusterPoints.slice(0, 5) // First 5 comments as samples
    };
  }, [selectedClusterId, clusterData]);

  // Calculate interesting cluster statistics when no cluster is selected
  const clusterSummaries = useMemo(() => {
    if (!clusterData || selectedClusterId) return null;

    const summaries = clusterData.clusters.map(([clusterId, points]) => {
      const stanceCounts = points.reduce((acc, point) => {
        acc[point.stance || 'Neutral/Unclear'] = (acc[point.stance || 'Neutral/Unclear'] || 0) + 1;
        return acc;
      }, {} as Record<string, number>);

      const forCount = stanceCounts['For'] || 0;
      const againstCount = stanceCounts['Against'] || 0;
      const neutralCount = stanceCounts['Neutral/Unclear'] || 0;
      const total = points.length;

      // Get cluster title and description from first point
      const firstPoint = points[0];
      const clusterTitle = firstPoint?.clusterTitle || null;
      const clusterDescription = firstPoint?.clusterDescription || null;

      return {
        clusterId,
        total,
        forCount,
        againstCount,
        neutralCount,
        forPercentage: total > 0 ? (forCount / total) * 100 : 0,
        againstPercentage: total > 0 ? (againstCount / total) * 100 : 0,
        neutralPercentage: total > 0 ? (neutralCount / total) * 100 : 0,
        dominantStance: forCount > againstCount && forCount > neutralCount ? 'For' :
                        againstCount > forCount && againstCount > neutralCount ? 'Against' : 
                        'Neutral/Unclear',
        clusterTitle,
        clusterDescription
      };
    });

    // Sort to find interesting clusters
    const highestFor = [...summaries].sort((a, b) => b.forPercentage - a.forPercentage)[0];
    const highestAgainst = [...summaries].sort((a, b) => b.againstPercentage - a.againstPercentage)[0];
    const mostBalanced = [...summaries].sort((a, b) => {
      const aBalance = Math.abs(a.forPercentage - a.againstPercentage);
      const bBalance = Math.abs(b.forPercentage - b.againstPercentage);
      return aBalance - bBalance;
    }).find(s => s.forPercentage > 10 && s.againstPercentage > 10); // Only consider clusters with meaningful splits
    const largest = [...summaries].sort((a, b) => b.total - a.total)[0];

    return {
      highestFor,
      highestAgainst,
      mostBalanced,
      largest,
      all: summaries
    };
  }, [clusterData, selectedClusterId]);

  if (isLoading) {
    return <div className="flex justify-center items-center h-64"><LoadingSpinner /></div>;
  }

  if (error) {
    return <div className="text-red-500 text-center p-4">Error loading cluster data: {error}</div>;
  }

  if (!clusterData || clusterData.clusters.length === 0) {
    return <div className="text-center p-4">No cluster data available.</div>;
  }

  return (
    <div className="h-full flex flex-col p-4">
      {/* Controls Section */}
      <Card className="mb-4 shadow-lg" collapsible={true} >
        <Card.Header>
          <h2 className="text-lg font-bold">Cluster Controls</h2>
        </Card.Header>
        <Card.Body>
          <ClusterControls
            clusters={clusterData.clusters}
            selectedCluster={selectedClusterId}
          />
        </Card.Body>
      </Card>

      {/* Main content area with chart and info */}
      <div className="flex-1 grid grid-cols-1 lg:grid-cols-3 gap-4">
        {/* Chart Section - spans 2 columns on large screens */}
        <Card className="lg:col-span-2 shadow-lg h-full" collapsible={true}>
          <Card.Header>
            <h2 className="text-lg font-bold">
              Cluster Scatter Plot
            </h2>
          </Card.Header>
          <Card.Body className="p-0 relative flex-1">
            <div className="h-full">
              <ClusterChart
                data={chartDisplayData}
                bounds={clusterData.bounds}
              />
            </div>
          </Card.Body>
        </Card>

        {/* Cluster Information Section */}
        <Card className="shadow-lg h-full overflow-hidden" collapsible={true}>
          <Card.Header>
            <h2 className="text-lg font-bold">
              {selectedClusterId ? `Cluster ${selectedClusterId} Details` : 'Cluster Information'}
            </h2>
          </Card.Header>
          <Card.Body className="overflow-y-auto">
            {selectedClusterInfo ? (
              <div className="space-y-4">
                <div>
                  <h3 className="font-semibold text-sm text-gray-700">Total Comments</h3>
                  <p className="text-2xl font-bold">{selectedClusterInfo.totalComments}</p>
                </div>
                
                <div>
                  <h3 className="font-semibold text-sm text-gray-700 mb-2">Stance Distribution</h3>
                  <div className="space-y-1">
                    {Object.entries(selectedClusterInfo.stanceCounts).map(([stance, count]) => (
                      <div key={stance} className="flex justify-between items-center">
                        <span className={`text-sm ${
                          stance === 'For' ? 'text-green-600' : 
                          stance === 'Against' ? 'text-red-600' : 
                          'text-gray-600'
                        }`}>{stance}:</span>
                        <span className="font-medium">{count}</span>
                      </div>
                    ))}
                  </div>
                </div>

                <div>
                  <h3 className="font-semibold text-sm text-gray-700 mb-2">Sample Comments</h3>
                  <div className="space-y-2">
                    {selectedClusterInfo.sampleComments.map((comment) => (
                      <div key={comment.id} className="bg-gray-50 p-2 rounded text-xs">
                        <p className="text-gray-600 line-clamp-2">{comment.title}</p>
                        {comment.keyQuote && (
                          <p className="text-gray-500 italic mt-1 line-clamp-2">&ldquo;{comment.keyQuote}&rdquo;</p>
                        )}
                        <p className="text-gray-400 mt-1">
                          Stance: <span className={
                            comment.stance === 'For' ? 'text-green-600' : 
                            comment.stance === 'Against' ? 'text-red-600' : 
                            'text-gray-600'
                          }>{comment.stance || 'Neutral/Unclear'}</span>
                        </p>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            ) : (
              <div className="space-y-4">
                <div className="text-gray-500 text-center pb-4 border-b border-gray-200">
                  <p>Select a cluster to view detailed information</p>
                  <p className="text-sm mt-2">Click on a cluster in the controls above or on the chart</p>
                </div>
                
                {clusterSummaries && (
                  <div className="space-y-3">
                    <h3 className="font-semibold text-sm text-gray-700">Interesting Clusters</h3>
                    
                    {clusterSummaries.highestFor && (
                      <ClusterSummaryCard
                        {...clusterSummaries.highestFor}
                        clusterTitle={clusterSummaries.highestFor.clusterTitle ?? undefined}
                        clusterDescription={clusterSummaries.highestFor.clusterDescription ?? undefined}
                        label="Highest support"
                      />
                    )}
                    
                    {clusterSummaries.highestAgainst && 
                     clusterSummaries.highestAgainst.clusterId !== clusterSummaries.highestFor?.clusterId && (
                      <ClusterSummaryCard
                        {...clusterSummaries.highestAgainst}
                        clusterTitle={clusterSummaries.highestAgainst.clusterTitle ?? undefined}
                        clusterDescription={clusterSummaries.highestAgainst.clusterDescription ?? undefined}
                        label="Highest opposition"
                      />
                    )}
                    
                    {clusterSummaries.mostBalanced && 
                     clusterSummaries.mostBalanced.clusterId !== clusterSummaries.highestFor?.clusterId &&
                     clusterSummaries.mostBalanced.clusterId !== clusterSummaries.highestAgainst?.clusterId && (
                      <ClusterSummaryCard
                        {...clusterSummaries.mostBalanced}
                        clusterTitle={clusterSummaries.mostBalanced.clusterTitle ?? undefined}
                        clusterDescription={clusterSummaries.mostBalanced.clusterDescription ?? undefined}
                        label="Most balanced views"
                      />
                    )}
                    
                    {clusterSummaries.largest && 
                     clusterSummaries.largest.clusterId !== clusterSummaries.highestFor?.clusterId &&
                     clusterSummaries.largest.clusterId !== clusterSummaries.highestAgainst?.clusterId &&
                     clusterSummaries.largest.clusterId !== clusterSummaries.mostBalanced?.clusterId && (
                      <ClusterSummaryCard
                        {...clusterSummaries.largest}
                        clusterTitle={clusterSummaries.largest.clusterTitle ?? undefined}
                        clusterDescription={clusterSummaries.largest.clusterDescription ?? undefined}
                        label="Largest cluster"
                      />
                    )}
                  </div>
                )}
              </div>
            )}
          </Card.Body>
        </Card>
      </div>
    </div>
  );
};

export default ClusterVisualization;