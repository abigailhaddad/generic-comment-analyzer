# Cluster Visualization Implementation Plan

## 1. Create Server Action for Cluster Data

File: `frontend/src/lib/actions/clusters.ts`

```typescript
'use server';

import { db, connectDb } from '@/lib/db';
import { comments } from '@/lib/db/schema';
import { sql } from 'drizzle-orm';
import { unstable_cache } from 'next/cache';
import { cacheConfig } from '../cache-config';

export interface ClusterPoint {
  id: string;
  title: string;
  stance: string | null;
  clusterId: number;
  pcaX: number;
  pcaY: number;
  keyQuote: string | null;
  themes: string | null;
}

export interface ClusterData {
  clusters: Map<number, ClusterPoint[]>;
  bounds: {
    minX: number;
    maxX: number;
    minY: number;
    maxY: number;
  };
}

export interface ClusterDataResponse {
  success: boolean;
  data?: ClusterData;
  error?: string;
}

export async function getClusterData(): Promise<ClusterDataResponse> {
  const fetchClusterData = async () => {
    try {
      const connection = await connectDb();
      if (!connection.success) {
        throw new Error("Failed to connect to database");
      }

      // Fetch all comments with cluster data
      const result = await db
        .select({
          id: comments.id,
          title: comments.title,
          stance: comments.stance,
          clusterId: comments.clusterId,
          pcaX: comments.pcaX,
          pcaY: comments.pcaY,
          keyQuote: comments.keyQuote,
          themes: comments.themes,
        })
        .from(comments)
        .where(sql`${comments.clusterId} IS NOT NULL AND ${comments.pcaX} IS NOT NULL AND ${comments.pcaY} IS NOT NULL`)
        .execute();

      // Group by cluster ID
      const clusters = new Map<number, ClusterPoint[]>();
      let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;

      result.forEach(row => {
        const point: ClusterPoint = {
          id: row.id,
          title: row.title || 'Untitled',
          stance: row.stance,
          clusterId: row.clusterId!,
          pcaX: row.pcaX!,
          pcaY: row.pcaY!,
          keyQuote: row.keyQuote,
          themes: row.themes,
        };

        if (!clusters.has(point.clusterId)) {
          clusters.set(point.clusterId, []);
        }
        clusters.get(point.clusterId)!.push(point);

        // Update bounds
        minX = Math.min(minX, point.pcaX);
        maxX = Math.max(maxX, point.pcaX);
        minY = Math.min(minY, point.pcaY);
        maxY = Math.max(maxY, point.pcaY);
      });

      return {
        success: true,
        data: {
          clusters,
          bounds: { minX, maxX, minY, maxY }
        }
      };
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      console.error("Error fetching cluster data:", errorMessage);
      return {
        success: false,
        error: `Failed to fetch cluster data: ${errorMessage}`
      };
    }
  };

  // Use caching in production
  const shouldSkipCache = process.env.NODE_ENV === 'development' && cacheConfig.disableCacheInDevelopment;
  
  if (shouldSkipCache) {
    return fetchClusterData();
  }

  const getCachedClusterData = unstable_cache(
    fetchClusterData,
    ['cluster-data'],
    {
      revalidate: 86400, // 24 hours
      tags: ['clusters']
    }
  );
  
  return getCachedClusterData();
}
```

## 2. Create Cluster Visualization Page

File: `frontend/src/app/clusters/page.tsx`

```typescript
import { Metadata } from 'next';
import { getClusterData } from '@/lib/actions/clusters';
import ClusterVisualization from '@/components/ClusterVisualization';
import Navbar from '@/components/Navbar';

export const metadata: Metadata = {
  title: 'Comment Clusters - Schedule F Analysis',
  description: 'Visualization of comment clusters using PCA analysis',
};

export const revalidate = 86400; // 24 hours

export default async function ClustersPage() {
  const clusterResponse = await getClusterData();

  if (!clusterResponse.success || !clusterResponse.data) {
    return (
      <main className="min-h-screen bg-gray-50">
        <Navbar />
        <div className="pt-24 pb-12">
          <div className="container mx-auto px-4">
            <div className="bg-red-50 border border-red-200 rounded-lg p-8 text-center">
              <h1 className="text-2xl font-bold text-red-800 mb-2">
                Error Loading Cluster Data
              </h1>
              <p className="text-red-600">
                {clusterResponse.error || 'Unable to load cluster visualization'}
              </p>
            </div>
          </div>
        </div>
      </main>
    );
  }

  return (
    <main className="min-h-screen bg-gray-50">
      <Navbar />
      <div className="pt-24 pb-12">
        <div className="container mx-auto px-4">
          <h1 className="text-3xl font-bold mb-8">Comment Cluster Analysis</h1>
          <ClusterVisualization data={clusterResponse.data} />
        </div>
      </div>
    </main>
  );
}
```

## 3. Create Cluster Visualization Component

File: `frontend/src/components/ClusterVisualization/index.tsx`

```typescript
'use client';

import { useState } from 'react';
import dynamic from 'next/dynamic';
import { ClusterData, ClusterPoint } from '@/lib/actions/clusters';
import Card from '@/components/ui/Card';
import ClusterControls from './ClusterControls';
import ClusterTooltip from './ClusterTooltip';
import { useRouter } from 'next/navigation';

// Dynamically import the chart to avoid SSR issues
const ClusterChart = dynamic(() => import('./ClusterChart'), {
  ssr: false,
  loading: () => (
    <div className="h-[600px] flex items-center justify-center">
      <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-blue-500" />
    </div>
  ),
});

interface ClusterVisualizationProps {
  data: ClusterData;
}

export default function ClusterVisualization({ data }: ClusterVisualizationProps) {
  const router = useRouter();
  const [selectedCluster, setSelectedCluster] = useState<number | null>(null);
  const [hoveredPoint, setHoveredPoint] = useState<ClusterPoint | null>(null);
  const [showStanceColors, setShowStanceColors] = useState(false);

  const handlePointClick = (point: ClusterPoint) => {
    router.push(`/comment/${point.id}`);
  };

  // Convert Map to array for chart
  const chartData = Array.from(data.clusters.entries()).map(([clusterId, points]) => ({
    id: `Cluster ${clusterId}`,
    data: points.map(point => ({
      x: point.pcaX,
      y: point.pcaY,
      ...point,
    })),
  }));

  const filteredData = selectedCluster !== null
    ? chartData.filter(series => series.id === `Cluster ${selectedCluster}`)
    : chartData;

  return (
    <div className="space-y-6">
      <Card collapsible={false}>
        <Card.Header className="bg-gradient-to-r from-purple-500 to-pink-500">
          <h2 className="text-lg font-bold text-white flex items-center">
            <span className="mr-2">🔮</span>
            Cluster Visualization
          </h2>
        </Card.Header>
        <Card.Body className="p-4">
          <ClusterControls
            clusters={Array.from(data.clusters.keys())}
            selectedCluster={selectedCluster}
            onClusterSelect={setSelectedCluster}
            showStanceColors={showStanceColors}
            onStanceColorsToggle={setShowStanceColors}
          />
          
          <div className="relative">
            <ClusterChart
              data={filteredData}
              bounds={data.bounds}
              showStanceColors={showStanceColors}
              onPointClick={handlePointClick}
              onPointHover={setHoveredPoint}
            />
            
            {hoveredPoint && (
              <ClusterTooltip point={hoveredPoint} />
            )}
          </div>
        </Card.Body>
      </Card>

      {/* Cluster Statistics */}
      <Card collapsible={true}>
        <Card.Header className="bg-gradient-to-r from-blue-500 to-blue-600">
          <h3 className="text-lg font-bold text-white">Cluster Statistics</h3>
        </Card.Header>
        <Card.Body className="p-4">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {Array.from(data.clusters.entries()).map(([clusterId, points]) => (
              <div key={clusterId} className="bg-gray-50 p-4 rounded-lg">
                <h4 className="font-semibold text-gray-700">Cluster {clusterId}</h4>
                <p className="text-2xl font-bold text-blue-600">{points.length}</p>
                <p className="text-sm text-gray-500">comments</p>
              </div>
            ))}
          </div>
        </Card.Body>
      </Card>
    </div>
  );
}
```

## 4. Create Cluster Chart Component

File: `frontend/src/components/ClusterVisualization/ClusterChart.tsx`

```typescript
'use client';

import { ResponsiveScatterPlot } from '@nivo/scatterplot';
import { ClusterPoint } from '@/lib/actions/clusters';

interface ClusterChartProps {
  data: Array<{
    id: string;
    data: Array<{
      x: number;
      y: number;
    } & ClusterPoint>;
  }>;
  bounds: {
    minX: number;
    maxX: number;
    minY: number;
    maxY: number;
  };
  showStanceColors: boolean;
  onPointClick: (point: ClusterPoint) => void;
  onPointHover: (point: ClusterPoint | null) => void;
}

export default function ClusterChart({
  data,
  bounds,
  showStanceColors,
  onPointClick,
  onPointHover,
}: ClusterChartProps) {
  // Color scheme for clusters
  const clusterColors = [
    '#e11d48', '#db2777', '#c026d3', '#9333ea', '#7c3aed',
    '#6366f1', '#3b82f6', '#0ea5e9', '#06b6d4', '#14b8a6',
    '#10b981', '#22c55e', '#84cc16', '#eab308', '#f59e0b',
    '#f97316', '#ef4444', '#dc2626', '#b91c1c', '#991b1b',
  ];

  // Color scheme for stances
  const stanceColors = {
    'For': '#10b981',
    'Against': '#ef4444',
    'Neutral/Unclear': '#64748b',
  };

  const getNodeColor = (node: any) => {
    if (showStanceColors && node.data.stance) {
      return stanceColors[node.data.stance as keyof typeof stanceColors] || '#64748b';
    }
    const clusterIndex = parseInt(node.serieId.replace('Cluster ', '')) % clusterColors.length;
    return clusterColors[clusterIndex];
  };

  return (
    <div style={{ height: 600 }} className="bg-white rounded-lg">
      <ResponsiveScatterPlot
        data={data}
        margin={{ top: 20, right: 20, bottom: 60, left: 60 }}
        xScale={{
          type: 'linear',
          min: bounds.minX * 1.1,
          max: bounds.maxX * 1.1,
        }}
        yScale={{
          type: 'linear',
          min: bounds.minY * 1.1,
          max: bounds.maxY * 1.1,
        }}
        blendMode="multiply"
        axisTop={null}
        axisRight={null}
        axisBottom={{
          tickSize: 5,
          tickPadding: 5,
          tickRotation: 0,
          legend: 'PCA Component 1',
          legendPosition: 'middle',
          legendOffset: 46,
        }}
        axisLeft={{
          tickSize: 5,
          tickPadding: 5,
          tickRotation: 0,
          legend: 'PCA Component 2',
          legendPosition: 'middle',
          legendOffset: -60,
        }}
        nodeSize={8}
        colors={getNodeColor}
        onClick={(node) => {
          if (node.data) {
            onPointClick(node.data as ClusterPoint);
          }
        }}
        onMouseEnter={(node) => {
          if (node.data) {
            onPointHover(node.data as ClusterPoint);
          }
        }}
        onMouseLeave={() => onPointHover(null)}
        tooltip={() => null} // We'll use our custom tooltip
        legends={[
          {
            anchor: 'top-right',
            direction: 'column',
            justify: false,
            translateX: -20,
            translateY: 20,
            itemsSpacing: 5,
            itemDirection: 'left-to-right',
            itemWidth: 80,
            itemHeight: 20,
            itemOpacity: 0.75,
            symbolSize: 12,
            symbolShape: 'circle',
            effects: [
              {
                on: 'hover',
                style: {
                  itemOpacity: 1,
                },
              },
            ],
          },
        ]}
      />
    </div>
  );
}
```

## 5. Create Cluster Controls Component

File: `frontend/src/components/ClusterVisualization/ClusterControls.tsx`

```typescript
'use client';

interface ClusterControlsProps {
  clusters: number[];
  selectedCluster: number | null;
  onClusterSelect: (cluster: number | null) => void;
  showStanceColors: boolean;
  onStanceColorsToggle: (show: boolean) => void;
}

export default function ClusterControls({
  clusters,
  selectedCluster,
  onClusterSelect,
  showStanceColors,
  onStanceColorsToggle,
}: ClusterControlsProps) {
  return (
    <div className="mb-4 flex flex-wrap gap-4 items-center">
      <div className="flex items-center gap-2">
        <label htmlFor="cluster-select" className="text-sm font-medium text-gray-700">
          Filter by Cluster:
        </label>
        <select
          id="cluster-select"
          value={selectedCluster ?? ''}
          onChange={(e) => onClusterSelect(e.target.value ? parseInt(e.target.value) : null)}
          className="px-3 py-1 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          <option value="">All Clusters</option>
          {clusters.sort((a, b) => a - b).map(clusterId => (
            <option key={clusterId} value={clusterId}>
              Cluster {clusterId}
            </option>
          ))}
        </select>
      </div>

      <label className="flex items-center gap-2 cursor-pointer">
        <input
          type="checkbox"
          checked={showStanceColors}
          onChange={(e) => onStanceColorsToggle(e.target.checked)}
          className="w-4 h-4 text-blue-600 rounded focus:ring-blue-500"
        />
        <span className="text-sm font-medium text-gray-700">Color by Stance</span>
      </label>
    </div>
  );
}
```

## 6. Create Cluster Tooltip Component

File: `frontend/src/components/ClusterVisualization/ClusterTooltip.tsx`

```typescript
'use client';

import { ClusterPoint } from '@/lib/actions/clusters';
import Badge from '@/components/ui/Badge';

interface ClusterTooltipProps {
  point: ClusterPoint;
}

export default function ClusterTooltip({ point }: ClusterTooltipProps) {
  const getBadgeType = (stance: string): 'success' | 'danger' | 'warning' => {
    if (stance === 'For') return 'success';
    if (stance === 'Against') return 'danger';
    return 'warning';
  };

  return (
    <div className="absolute z-50 pointer-events-none bg-white p-4 rounded-lg shadow-lg border border-gray-200 max-w-sm"
         style={{
           left: '50%',
           top: '50%',
           transform: 'translate(-50%, -50%)',
         }}>
      <h4 className="font-semibold text-gray-800 mb-2 line-clamp-2">
        {point.title}
      </h4>
      
      <div className="space-y-2 text-sm">
        <div className="flex items-center gap-2">
          <span className="text-gray-600">Cluster:</span>
          <span className="font-medium">{point.clusterId}</span>
        </div>
        
        {point.stance && (
          <div className="flex items-center gap-2">
            <span className="text-gray-600">Stance:</span>
            <Badge type={getBadgeType(point.stance)} label={point.stance} />
          </div>
        )}
        
        {point.keyQuote && (
          <div className="mt-2">
            <p className="text-gray-600 text-xs">Key Quote:</p>
            <p className="italic text-gray-700 line-clamp-2">"{point.keyQuote}"</p>
          </div>
        )}
        
        {point.themes && (
          <div className="mt-2">
            <p className="text-gray-600 text-xs">Themes:</p>
            <div className="flex flex-wrap gap-1 mt-1">
              {point.themes.split(',').slice(0, 3).map((theme, i) => (
                <span key={i} className="text-xs bg-gray-100 px-2 py-1 rounded">
                  {theme.trim()}
                </span>
              ))}
            </div>
          </div>
        )}
      </div>
      
      <p className="text-xs text-blue-600 mt-3">Click to view full comment</p>
    </div>
  );
}
```

## 7. Update Navigation

Add a link to the clusters page in your Navbar or create a dedicated navigation item.

File: Update `frontend/src/components/Navbar.tsx`

Add this to the navigation items:

```typescript
<Link
  href="/clusters"
  className="nav-link"
>
  Cluster Analysis
</Link>
```

## 8. Export the new action

Update `frontend/src/lib/actions/index.ts` to export the new cluster functions:

```typescript
// Add to exports
export { getClusterData } from './clusters';
export type { ClusterData, ClusterPoint, ClusterDataResponse } from './clusters';
```

## Next Steps

1. Install any missing dependencies if needed
2. Test the implementation
3. Consider adding:
   - Zoom and pan functionality
   - Export cluster data as CSV
   - Filter by themes or other attributes
   - 3D visualization option
   - Cluster summary statistics