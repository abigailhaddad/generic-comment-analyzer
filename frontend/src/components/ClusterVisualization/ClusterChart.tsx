"use client";

import { useRef, useState, useCallback, useEffect, useMemo, Suspense } from 'react';
import {
  ResponsiveScatterPlotCanvas
} from "@nivo/scatterplot";
import { ClusterPoint } from "@/lib/actions/clusters";
import { useRouter, useSearchParams } from 'next/navigation';
// import d3 from 'd3'; // d3 import seems unused
import ClusterTooltipOverlay from './ClusterTooltipOverlay';

// Base colors for parent clusters - moved outside component to ensure it's always available
const baseColors = [
  "#6366f1", "#10b981", "#f59e0b", "#ec4899", "#06b6d4",
  "#8b5cf6", "#f97316", "#14b8a6", "#3b82f6", "#ef4444",
  "#a855f7", "#0ea5e9", "#84cc16", "#f43f5e", "#2563eb"
];

interface ClusterChartProps {
  data: Array<{
    id: string; // This is the clusterId string
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
}

function ClusterChartContent({
  data,
  bounds,
}: ClusterChartProps) {
  const router = useRouter();
  const searchParams = useSearchParams();
  const chartRef = useRef<HTMLDivElement>(null);
  const [tooltipPosition, setTooltipPosition] = useState({ x: 0, y: 0 });
  const [hoveredPoint, setHoveredPoint] = useState<ClusterPoint | null>(null);
  const [clickedPoint, setClickedPoint] = useState<ClusterPoint | null>(null);
  const hoverTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  const lastHoverTimeRef = useRef<number>(0);
  const pendingHoverRef = useRef<ClusterPoint | null>(null);

  // Create a stable color mapping for cluster IDs
  const clusterIdToColorIndex = useMemo(() => {
    const mapping = new Map<string, number>();
    const parentMapping = new Map<string, number>();
    let nextParentIndex = 0;
    
    // Only proceed if we have data
    if (!data || data.length === 0) return mapping;
    
    // First, identify parent clusters and assign base colors
    data.forEach(series => {
      if (series && series.id && typeof series.id === 'string' && series.id.length > 0) {
        // Extract the numeric part at the beginning (parent cluster)
        const match = series.id.match(/^(\d+)/);
        const parentCluster = match ? match[1] : series.id;
        if (!parentMapping.has(parentCluster)) {
          parentMapping.set(parentCluster, nextParentIndex++);
        }
      }
    });
    
    // Then, assign colors to sub-clusters based on their parent
    data.forEach(series => {
      if (series && series.id && typeof series.id === 'string' && series.id.length > 0 && !mapping.has(series.id)) {
        const match = series.id.match(/^(\d+)(.*)/);
        const parentCluster = match ? match[1] : series.id;
        const subPart = match ? match[2] : '';
        const parentIndex = parentMapping.get(parentCluster) || 0;
        
        if (subPart) {
          // Create variation based on the sub-part (could be 'a', 'b', 'aa', 'ab', etc.)
          let variation = 0;
          for (let i = 0; i < subPart.length; i++) {
            variation += (subPart.charCodeAt(i) - 'a'.charCodeAt(0)) * Math.pow(26, subPart.length - 1 - i);
          }
          // Limit variation to prevent running out of distinguishable colors
          variation = Math.min(variation, 15);
          // Combine parent index with variation for unique but related colors
          mapping.set(series.id, parentIndex * 20 + variation);
        } else {
          // For simple clusters without sub-clusters, just use the parent index
          mapping.set(series.id, parentIndex * 20);
        }
      }
    });
    return mapping;
  }, [data]);

  // Check for pinned comment ID in URL on mount
  useEffect(() => {
    const pinnedId = searchParams.get('pinned');
    if (pinnedId && data && chartRef.current) {
      for (const series of data) {
        const point = series.data.find(p => p.id === pinnedId);
        if (point) {
          setClickedPoint(point);
          
          // Calculate the actual position of the point on the chart
          // Chart dimensions from the Nivo configuration
          const chartWidth = chartRef.current.offsetWidth;
          const chartHeight = chartRef.current.offsetHeight;
          const margin = { top: 20, right: 20, bottom: 60, left: 60 };
          
          // Calculate the drawing area dimensions
          const drawWidth = chartWidth - margin.left - margin.right;
          const drawHeight = chartHeight - margin.top - margin.bottom;
          
          // Scale the point coordinates to pixel positions
          const xRange = bounds.maxX * 1.1 - bounds.minX * 1.1;
          const yRange = bounds.maxY * 1.1 - bounds.minY * 1.1;
          
          const pixelX = margin.left + ((point.x - bounds.minX * 1.1) / xRange) * drawWidth;
          const pixelY = margin.top + drawHeight - ((point.y - bounds.minY * 1.1) / yRange) * drawHeight;
          
          setTooltipPosition({ x: pixelX, y: pixelY });
          break;
        }
      }
    }
  }, [searchParams, data, bounds]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (hoverTimeoutRef.current) {
        clearTimeout(hoverTimeoutRef.current);
      }
    };
  }, []);

  // const stanceColors = { // This was commented out by user, ensuring it's fully gone or remains commented
  //   For: "#10b981",
  //   Against: "#ef4444",
  //   "Neutral/Unclear": "#64748b",
  // };

  const handleMouseMove = useCallback((event: React.MouseEvent) => {
    // Don't update position if we have a clicked point (tooltip is frozen)
    if (clickedPoint) return;
    
    if (chartRef.current && hoveredPoint) {
      const rect = chartRef.current.getBoundingClientRect();
      const newX = event.clientX - rect.left;
      const newY = event.clientY - rect.top;
      
      // Only update if position has changed significantly (more than 2 pixels)
      setTooltipPosition(prev => {
        if (Math.abs(prev.x - newX) > 2 || Math.abs(prev.y - newY) > 2) {
          return { x: newX, y: newY };
        }
        return prev;
      });
    }
  }, [hoveredPoint, clickedPoint]);

  const handlePointHover = useCallback((point: ClusterPoint | null) => {
    // Don't update hover if we have a clicked point
    if (clickedPoint) return;
    
    // Throttle hover updates to prevent excessive re-renders
    const now = Date.now();
    const timeSinceLastHover = now - lastHoverTimeRef.current;
    
    // Clear any existing timeout
    if (hoverTimeoutRef.current) {
      clearTimeout(hoverTimeoutRef.current);
    }

    if (point) {
      // Store the pending hover point
      pendingHoverRef.current = point;
      
      // If we're hovering too quickly, delay the update
      if (timeSinceLastHover < 25) { // Reduced throttle for more responsiveness
        hoverTimeoutRef.current = setTimeout(() => {
          if (pendingHoverRef.current) {
            setHoveredPoint(pendingHoverRef.current);
            lastHoverTimeRef.current = Date.now();
          }
        }, 25 - timeSinceLastHover);
      } else {
        // Update immediately if enough time has passed
        setHoveredPoint(point);
        lastHoverTimeRef.current = now;
      }
    } else {
      // Clear pending hover
      pendingHoverRef.current = null;
      
      // Add a minimal delay before hiding tooltip to allow clicking
      hoverTimeoutRef.current = setTimeout(() => {
        setHoveredPoint(null);
      }, 50); // Reduced delay for more precise hover behavior
    }
  }, [clickedPoint]);

  const handlePointClick = useCallback((point: ClusterPoint) => {
    // If clicking the same point, toggle it off
    if (clickedPoint?.id === point.id) {
      setClickedPoint(null);
      setHoveredPoint(null);
    } else {
      setClickedPoint(point);
      setHoveredPoint(null);
      
      // Calculate and set the position of the clicked point
      if (chartRef.current) {
        const chartWidth = chartRef.current.offsetWidth;
        const chartHeight = chartRef.current.offsetHeight;
        const margin = { top: 20, right: 20, bottom: 60, left: 60 };
        
        const drawWidth = chartWidth - margin.left - margin.right;
        const drawHeight = chartHeight - margin.top - margin.bottom;
        
        const xRange = bounds.maxX * 1.1 - bounds.minX * 1.1;
        const yRange = bounds.maxY * 1.1 - bounds.minY * 1.1;
        
        const pixelX = margin.left + ((point.pcaX - bounds.minX * 1.1) / xRange) * drawWidth;
        const pixelY = margin.top + drawHeight - ((point.pcaY - bounds.minY * 1.1) / yRange) * drawHeight;
        
        setTooltipPosition({ x: pixelX, y: pixelY });
      }
    }
  }, [clickedPoint, bounds]);

  // Clear clicked point when clicking outside
  const handleChartClick = useCallback((event: React.MouseEvent) => {
    if (event.target === event.currentTarget) {
      setClickedPoint(null);
    }
  }, []);

  const handleMouseLeave = useCallback(() => {
    if (!clickedPoint) {
      handlePointHover(null);
    }
  }, [clickedPoint, handlePointHover]);

  const handleMouseEnter = useCallback((node: { data?: unknown }) => {
    if (node.data && !clickedPoint) {
      handlePointHover(node.data as ClusterPoint);
    }
  }, [clickedPoint, handlePointHover]);

  const handleClick = useCallback((node: { data?: unknown }) => {
    if (node.data) {
      handlePointClick(node.data as ClusterPoint);
    }
  }, [handlePointClick]);

  // Memoize the chart data to prevent unnecessary re-renders
  const chartData = useMemo(() => data || [], [data]);

  // Memoize event handlers to prevent re-creation on every render
  const memoizedHandlers = useMemo(() => ({
    onClick: handleClick,
    onMouseEnter: handleMouseEnter,
    onMouseLeave: handleMouseLeave,
  }), [handleClick, handleMouseEnter, handleMouseLeave]);

  return (
    <div 
      ref={chartRef}
      className="bg-white rounded-lg relative"
      style={{ height: '600px' }}
      onMouseMove={handleMouseMove}
      onMouseLeave={handleMouseLeave}
      onClick={handleChartClick}
    >
      <ResponsiveScatterPlotCanvas
        data={chartData}
        margin={{ top: 20, right: 20, bottom: 60, left: 60 }}
        xScale={{
          type: "linear",
          min: bounds.minX * 1.1,
          max: bounds.maxX * 1.1,
        }}
        yScale={{
          type: "linear",
          min: bounds.minY * 1.1,
          max: bounds.maxY * 1.1,
        }}
        axisTop={null}
        axisRight={null}
        axisBottom={{
          tickSize: 5,
          tickPadding: 5,
          tickRotation: 0,
          legend: "PCA Component 1",
          legendPosition: "middle",
          legendOffset: 46,
        }}
        axisLeft={{
          tickSize: 5,
          tickPadding: 5,
          tickRotation: 0,
          legend: "PCA Component 2",
          legendPosition: "middle",
          legendOffset: -60,
        }}
        nodeSize={10}
        // Add a smaller detection area for hover
        colors={(node) => {
          // Access the series id from the node
          const serieId = node.serieId;
          
          if (!serieId || typeof serieId !== 'string') {
            return baseColors[0]; // Fallback color
          }
          
          const colorIndex = clusterIdToColorIndex.get(serieId);
          if (colorIndex !== undefined && baseColors && baseColors.length > 0) {
            const parentIndex = Math.floor(colorIndex / 20);
            const variation = colorIndex % 20;
            const baseColor = baseColors[parentIndex % baseColors.length];
            
            // Additional safety check
            if (!baseColor || typeof baseColor !== 'string' || baseColor.length !== 7) {
              return baseColors[0] || '#3b82f6'; // Fallback with extra safety
            }
            
            // Create shade variations - keep colors bright and vibrant
            // Use a much smaller multiplier to maintain brightness
            const shadeMultiplier = Math.max(0.75, 1 - (variation * 0.02)); // Max 25% darkening only
            
            // Parse hex color and apply shade
            const r = parseInt(baseColor.slice(1, 3), 16);
            const g = parseInt(baseColor.slice(3, 5), 16);
            const b = parseInt(baseColor.slice(5, 7), 16);
            
            // Apply shade but ensure minimum RGB values for visibility
            const minValue = 100; // Higher minimum RGB value to keep colors bright
            const newR = Math.max(minValue, Math.round(r * shadeMultiplier));
            const newG = Math.max(minValue, Math.round(g * shadeMultiplier));
            const newB = Math.max(minValue, Math.round(b * shadeMultiplier));
            
            return `#${newR.toString(16).padStart(2, '0')}${newG.toString(16).padStart(2, '0')}${newB.toString(16).padStart(2, '0')}`;
          }
          return baseColors[0] || '#3b82f6'; // Fallback color with extra safety
        }}
        onClick={memoizedHandlers.onClick}
        onMouseEnter={memoizedHandlers.onMouseEnter}
        onMouseLeave={memoizedHandlers.onMouseLeave}
        enableGridX={false}
        enableGridY={false}
        // Disable Nivo's built-in tooltip
        tooltip={() => null}
      />
      
      {/* Show tooltip for either hovered or clicked point */}
      {(hoveredPoint || clickedPoint) && (
        <ClusterTooltipOverlay
          point={clickedPoint || hoveredPoint!}
          position={tooltipPosition}
          isFixed={!!clickedPoint}
          onClose={() => {
            setClickedPoint(null);
            setHoveredPoint(null);
          }}
          onNavigate={(id: string) => {
            const currentPath = window.location.pathname; // Base path e.g. /clusters
            const currentSearchParams = new URLSearchParams(window.location.search);
            
            // Remove existing 'pinned' param if any, then add new one
            currentSearchParams.delete('pinned');
            currentSearchParams.set('pinned', id);
            
            const returnUrl = encodeURIComponent(`${currentPath}?${currentSearchParams.toString()}`);
            router.push(`/comment/${id}?returnUrl=${returnUrl}`);
          }}
        />
      )}
      
      {/* Instructions */}
      <div className="absolute bottom-2 left-2 text-xs text-gray-500">
        Hover to preview • Click to freeze tooltip • {(data || []).length} clusters shown
      </div>
    </div>
  );
}

export default function ClusterChart(props: ClusterChartProps) {
  return (
    <Suspense fallback={<div className="h-full flex items-center justify-center">Loading chart...</div>}>
      <ClusterChartContent {...props} />
    </Suspense>
  );
}