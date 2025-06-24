"use client";

import { useRef, useState, useEffect, useMemo } from 'react';
import { ClusterPoint } from '@/lib/actions/clusters';
import Badge from '@/components/ui/Badge';

interface ClusterTooltipOverlayProps {
  point: ClusterPoint;
  position: { x: number; y: number };
  isFixed: boolean;
  onClose: () => void;
  onNavigate: (id: string) => void;
}

export default function ClusterTooltipOverlay({ 
  point, 
  position, 
  isFixed,
  onClose,
  onNavigate
}: ClusterTooltipOverlayProps) {
  const tooltipRef = useRef<HTMLDivElement>(null);
  const [tooltipDimensions, setTooltipDimensions] = useState({ width: 0, height: 0 });

  // Only measure tooltip dimensions when it changes or when point changes
  useEffect(() => {
    if (tooltipRef.current) {
      const rect = tooltipRef.current.getBoundingClientRect();
      setTooltipDimensions({ width: rect.width, height: rect.height });
    }
  }, [point.id]); // Only re-measure when the point changes

  // Calculate adjusted position using useMemo to prevent recalculation on every render
  const adjustedPosition = useMemo(() => {
    if (!tooltipRef.current) {
      return { x: position.x + 10, y: position.y + 10 };
    }

    const parentRect = tooltipRef.current.offsetParent?.getBoundingClientRect();
    if (!parentRect) {
      return { x: position.x + 10, y: position.y + 10 };
    }

    let newX = position.x + 10;
    let newY = position.y + 10;
    
    // Check if tooltip would go off the right edge
    if (newX + tooltipDimensions.width > parentRect.width - 20) {
      // Position to the left of the cursor instead
      newX = position.x - tooltipDimensions.width - 10;
    }
    
    // Check if tooltip would go off the bottom edge
    if (newY + tooltipDimensions.height > parentRect.height - 20) {
      // Position above the cursor instead
      newY = position.y - tooltipDimensions.height - 10;
    }
    
    // Ensure tooltip doesn't go off the left edge
    if (newX < 10) {
      newX = 10;
    }
    
    // Ensure tooltip doesn't go off the top edge
    if (newY < 10) {
      newY = 10;
    }
    
    return { x: newX, y: newY };
  }, [position.x, position.y, tooltipDimensions.width, tooltipDimensions.height]);

  // Use adjusted position for tooltip
  const tooltipStyle: React.CSSProperties = {
    position: 'absolute',
    left: adjustedPosition.x,
    top: adjustedPosition.y,
    zIndex: 50,
    pointerEvents: 'auto', // Always allow interaction
  };

  return (
    <div 
      ref={tooltipRef}
      className={`bg-white p-4 rounded-lg shadow-lg border ${
        isFixed ? 'border-blue-400 ring-2 ring-blue-200' : 'border-gray-200'
      } max-w-sm transition-all`}
      style={tooltipStyle}
      onMouseEnter={(e) => e.stopPropagation()}
    >
      {isFixed && (
        <button
          onClick={onClose}
          className="absolute top-2 right-2 text-gray-400 hover:text-gray-600"
          aria-label="Close tooltip"
        >
          <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
          </svg>
        </button>
      )}
      
      <h4 className="font-semibold text-gray-800 mb-2 line-clamp-2 pr-4">
        {point.title}
      </h4>
      
      <div className="space-y-2 text-sm">
        <div className="flex items-start gap-2">
          <span className="text-gray-600">Cluster:</span>
          <div className="flex-1">
            <span className="font-medium">{point.clusterId}</span>
            {point.clusterTitle && (
              <p className="text-xs text-gray-500 italic mt-1">{point.clusterTitle}</p>
            )}
          </div>
        </div>
        
        {point.stance && (
          <div className="flex items-center gap-2">
            <span className="text-gray-600">Stance:</span>
            <Badge 
              type={point.stance === 'For' ? 'success' : point.stance === 'Against' ? 'danger' : 'default'}
              label={point.stance}
            />
          </div>
        )}
        
        {point.keyQuote && (
          <div className="mt-2">
            <p className="text-gray-600 text-xs">Key Quote:</p>
            <p className="italic text-gray-700 line-clamp-2">&quot;{point.keyQuote}&quot;</p>
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
      
      {isFixed && (
        <button
          onClick={() => onNavigate(point.id)}
          className="w-full text-sm bg-blue-600 hover:bg-blue-700 text-white py-2 px-4 rounded mt-3 font-medium transition-colors"
        >
          View Full Comment →
        </button>
      )}
      
      {!isFixed && (
        <p className="text-xs text-gray-500 mt-3 italic">Click to pin this tooltip</p>
      )}
    </div>
  );
} 