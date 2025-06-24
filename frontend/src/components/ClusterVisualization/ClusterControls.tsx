'use client';

import { useMemo } from 'react';
import { useRouter } from 'next/navigation';
import { ClusterPoint } from '@/lib/actions/clusters';
import { SimpleTooltip } from '@/components/ui/Tooltip';

interface ClusterControlsProps {
  clusters: Array<[string, ClusterPoint[]]>;
  selectedCluster: string | null;
}

export default function ClusterControls({
  clusters,
  selectedCluster,
}: ClusterControlsProps) {
  const router = useRouter();

  // Extract cluster IDs and get descriptions
  const clusterData = useMemo(() => {
    return clusters.map(([clusterId, points]) => {
      const firstPoint = points[0];
      return {
        id: clusterId,
        title: firstPoint?.clusterTitle || null,
        description: firstPoint?.clusterDescription || null,
        count: points.length
      };
    });
  }, [clusters]);

  const clusterIds = useMemo(() => clusters.map(([clusterId]) => clusterId), [clusters]);

  // Generate cluster colors (same logic as in ClusterChart)
  const clusterColors = useMemo(() => {
    // Base colors for parent clusters (same as in ClusterChart)
    const baseColors = [
      "#6366f1", "#10b981", "#f59e0b", "#ec4899", "#06b6d4",
      "#8b5cf6", "#f97316", "#14b8a6", "#3b82f6", "#ef4444",
      "#a855f7", "#0ea5e9", "#84cc16", "#f43f5e", "#2563eb"
    ];
    
    const colors = new Map<string, string>();
    const parentMapping = new Map<string, number>();
    let nextParentIndex = 0;
    
    // First, identify parent clusters
    clusterIds.forEach(clusterId => {
      // Extract the numeric part at the beginning (parent cluster)
      const match = clusterId.match(/^(\d+)/);
      const parentCluster = match ? match[1] : clusterId;
      if (!parentMapping.has(parentCluster)) {
        parentMapping.set(parentCluster, nextParentIndex++);
      }
    });
    
    // Then, assign colors
    clusterIds.forEach(clusterId => {
      const match = clusterId.match(/^(\d+)(.*)/);
      const parentCluster = match ? match[1] : clusterId;
      const subPart = match ? match[2] : '';
      const parentIndex = parentMapping.get(parentCluster) || 0;
      
      const baseColor = baseColors[parentIndex % baseColors.length];
      let shadeMultiplier = 1;
      
      if (subPart) {
        // Create variation based on the sub-part (could be 'a', 'b', 'aa', 'ab', etc.)
        let variation = 0;
        for (let i = 0; i < subPart.length; i++) {
          variation += (subPart.charCodeAt(i) - 'a'.charCodeAt(0)) * Math.pow(26, subPart.length - 1 - i);
        }
        // Limit the variation to prevent colors from getting too dark
        variation = Math.min(variation, 15); // Match ClusterChart logic
        shadeMultiplier = Math.max(0.75, 1 - (variation * 0.02)); // Max 25% darkening only
      }
      
      const r = parseInt(baseColor.slice(1, 3), 16);
      const g = parseInt(baseColor.slice(3, 5), 16);
      const b = parseInt(baseColor.slice(5, 7), 16);
      
      // Apply shade but ensure minimum RGB values for visibility
      const minValue = 100; // Higher minimum RGB value to keep colors bright
      const newR = Math.max(minValue, Math.round(r * shadeMultiplier));
      const newG = Math.max(minValue, Math.round(g * shadeMultiplier));
      const newB = Math.max(minValue, Math.round(b * shadeMultiplier));
      
      colors.set(clusterId, `#${newR.toString(16).padStart(2, '0')}${newG.toString(16).padStart(2, '0')}${newB.toString(16).padStart(2, '0')}`);
    });
    
    return colors;
  }, [clusterIds]);

  // Group clusters by parent for better organization
  const groupedClusters = useMemo(() => {
    const groups = new Map<string, string[]>();
    clusterIds.forEach(clusterId => {
      // Extract the numeric part at the beginning (parent cluster)
      const match = clusterId.match(/^(\d+)/);
      const parent = match ? match[1] : clusterId;
      if (!groups.has(parent)) {
        groups.set(parent, []);
      }
      groups.get(parent)!.push(clusterId);
    });
    // Sort groups and their clusters
    return Array.from(groups.entries())
      .sort(([a], [b]) => a.localeCompare(b, undefined, { numeric: true, sensitivity: 'base' }))
      .map(([parent, subs]) => [parent, subs.sort((a, b) => {
        // Custom sort to handle mixed formats (0a, 0b, 0aa, 0ab, etc.)
        const aMatch = a.match(/^\d+(.*)/);
        const bMatch = b.match(/^\d+(.*)/);
        const aSuffix = aMatch ? aMatch[1] : '';
        const bSuffix = bMatch ? bMatch[1] : '';
        
        // Sort by length first (a, b, c before aa, ab, ac)
        if (aSuffix.length !== bSuffix.length) {
          return aSuffix.length - bSuffix.length;
        }
        // Then alphabetically
        return aSuffix.localeCompare(bSuffix);
      })] as const);
  }, [clusterIds]);

  return (
    <div className="space-y-4">
      {/* Filter Controls */}
      <div className="flex flex-wrap gap-4 items-center">
        <div className="flex items-center gap-2">
          <label htmlFor="cluster-select" className="text-sm font-medium text-gray-700">
            Filter by Cluster:
          </label>
          <select
            id="cluster-select"
            value={selectedCluster ?? ''}
            onChange={(e) => {
              const newCluster = e.target.value;
              // Only update the URL - the component will react to the URL change
              if (newCluster) {
                router.push(`/clusters/${newCluster}`);
              } else {
                router.push('/clusters');
              }
            }}
            className="px-3 py-1 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            <option value="">All Clusters</option>
            {clusterData.sort((a, b) => a.id.localeCompare(b.id, undefined, { numeric: true, sensitivity: 'base' })).map(cluster => (
              <option key={`cluster-option-${cluster.id}`} value={cluster.id}>
                {cluster.title ? `${cluster.title} (${cluster.id})` : `Cluster ${cluster.id}`}
              </option>
            ))}
          </select>
        </div>
      </div>

      {/* Color Legend */}
      <div>
        <h3 className="text-sm font-medium text-gray-700 mb-2">Cluster Legend</h3>
        <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 gap-2">
          {groupedClusters.map((group) => {
            const [parent, subClusters] = group;
            return (
              <div key={parent} className="space-y-1">
                <div className="text-xs font-medium text-gray-600">Group {parent}</div>
                {subClusters.map((clusterId: string) => {
                  const cluster = clusterData.find(c => c.id === clusterId);
                  return (
                    <div key={clusterId} className="flex items-center gap-1">
                      <div 
                        className="w-3 h-3 rounded-full flex-shrink-0" 
                        style={{ backgroundColor: clusterColors.get(clusterId) }}
                      />
                      <div className="flex flex-col">
                        <span className="text-xs text-gray-700">{clusterId}</span>
                        {cluster?.title && (
                          <SimpleTooltip
                            content={
                              <div className="max-w-xs">
                                <div className="font-semibold mb-1">{cluster.title}</div>
                                {cluster.description && (
                                  <div className="text-gray-300">{cluster.description}</div>
                                )}
                              </div>
                            }
                            className="!left-0 !-translate-x-0 !whitespace-normal !w-max !max-w-xs"
                          >
                            <span className="text-xs text-gray-500 italic cursor-help">
                              {cluster.title.length > 30 ? `${cluster.title.substring(0, 30)}...` : cluster.title}
                            </span>
                          </SimpleTooltip>
                        )}
                      </div>
                    </div>
                  );
                })}
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}