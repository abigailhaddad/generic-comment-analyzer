'use client';

import React from 'react';

interface SkeletonProps {
  variant?: 'text' | 'circular' | 'rectangular' | 'rounded';
  width?: number | string;
  height?: number | string;
  className?: string;
  animation?: 'pulse' | 'wave' | 'none';
}

export default function Skeleton({
  variant = 'text',
  width,
  height,
  className = '',
  animation = 'pulse'
}: SkeletonProps) {
  const getVariantClasses = () => {
    switch (variant) {
      case 'text':
        return 'h-4 rounded';
      case 'circular':
        return 'rounded-full';
      case 'rectangular':
        return 'rounded-none';
      case 'rounded':
        return 'rounded-lg';
      default:
        return 'rounded';
    }
  };

  const getAnimationClasses = () => {
    switch (animation) {
      case 'pulse':
        return 'animate-pulse';
      case 'wave':
        return 'animate-shimmer';
      case 'none':
        return '';
      default:
        return 'animate-pulse';
    }
  };

  const style: React.CSSProperties = {};
  if (width) style.width = typeof width === 'number' ? `${width}px` : width;
  if (height) style.height = typeof height === 'number' ? `${height}px` : height;

  return (
    <div
      className={`bg-gray-200 ${getVariantClasses()} ${getAnimationClasses()} ${className}`}
      style={style}
      role="status"
      aria-label="Loading..."
    >
      <span className="sr-only">Loading...</span>
    </div>
  );
}

interface SkeletonTextProps {
  lines?: number;
  className?: string;
}

export function SkeletonText({ lines = 3, className = '' }: SkeletonTextProps) {
  return (
    <div className={`space-y-2 ${className}`}>
      {Array.from({ length: lines }).map((_, index) => (
        <Skeleton
          key={index}
          variant="text"
          width={index === lines - 1 ? '80%' : '100%'}
        />
      ))}
    </div>
  );
}

interface SkeletonCardProps {
  className?: string;
  showHeader?: boolean;
  showFooter?: boolean;
}

export function SkeletonCard({ className = '', showHeader = true, showFooter = false }: SkeletonCardProps) {
  return (
    <div className={`bg-white rounded-lg shadow-md overflow-hidden ${className}`}>
      {showHeader && (
        <div className="bg-gray-100 px-6 py-4 border-b border-gray-200">
          <Skeleton variant="text" width="40%" height={24} />
        </div>
      )}
      <div className="p-6">
        <SkeletonText lines={3} />
      </div>
      {showFooter && (
        <div className="bg-gray-50 px-6 py-4 border-t border-gray-200">
          <Skeleton variant="text" width="30%" />
        </div>
      )}
    </div>
  );
}