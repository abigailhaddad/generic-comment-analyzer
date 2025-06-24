'use client';

import { useState, useEffect } from 'react';
import Link from 'next/link';
import Card from '@/components/ui/Card';
import { getClusterStats } from '@/lib/actions/clusters';

interface ClusterStats {
  totalClusters: number;
  totalPoints: number;
  largestCluster: number;
  smallestCluster: number;
}

export default function ClusterPreviewCard() {
  const [stats, setStats] = useState<ClusterStats | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function fetchClusterStats() {
      try {
        const response = await getClusterStats();
        
        if (response.success && response.stats) {
          const sizes = response.stats.clusterSizes.map(c => c.size);
          
          setStats({
            totalClusters: response.stats.totalClusters,
            totalPoints: response.stats.totalPoints,
            largestCluster: Math.max(...sizes),
            smallestCluster: Math.min(...sizes),
          });
        } else {
          setError(response.error || 'Failed to load cluster data');
        }
      } catch (err) {
        setError('Error loading cluster statistics');
        console.error(err);
      } finally {
        setLoading(false);
      }
    }

    fetchClusterStats();
  }, []);

  if (loading) {
    return (
      <Card collapsible={false}>
        <Card.Header className="bg-gradient-to-r from-slate-100 to-blue-50 border-b border-slate-200">
          <h5 className="text-lg font-semibold text-slate-800 flex items-center">
            <span className="mr-2 opacity-60">🔮</span>
            Cluster Analysis
          </h5>
        </Card.Header>
        <Card.Body className="p-6">
          <div className="animate-pulse">
            <div className="h-4 bg-gray-200 rounded w-3/4 mb-4"></div>
            <div className="grid grid-cols-2 gap-4">
              <div className="h-16 bg-gray-200 rounded"></div>
              <div className="h-16 bg-gray-200 rounded"></div>
            </div>
          </div>
        </Card.Body>
      </Card>
    );
  }

  if (error) {
    return (
      <Card collapsible={false}>
        <Card.Header className="bg-gradient-to-r from-rose-50 to-rose-100 border-b border-rose-200">
          <h5 className="text-lg font-semibold text-rose-800 flex items-center">
            <span className="mr-2 opacity-60">⚠️</span>
            Cluster Analysis Error
          </h5>
        </Card.Header>
        <Card.Body className="p-6 text-center">
          <p className="text-red-500">{error}</p>
        </Card.Body>
      </Card>
    );
  }

  if (!stats) {
    return null;
  }

  return (
    <Card collapsible={false}>
      <Card.Header className="bg-gradient-to-r from-purple-500 to-pink-500">
        <h5 className="text-lg font-bold text-white flex items-center">
          <span className="mr-2">🔮</span>
          Cluster Analysis
        </h5>
      </Card.Header>
      <Card.Body className="p-6">
        <p className="text-slate-600 mb-4">
          Explore comment clusters visualized using PCA analysis to identify patterns and themes.
        </p>
        
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
          <div className="text-center">
            <p className="text-2xl font-bold text-slate-700">{stats.totalClusters}</p>
            <p className="text-sm text-slate-500">Clusters</p>
          </div>
          <div className="text-center">
            <p className="text-2xl font-bold text-slate-700">{stats.totalPoints}</p>
            <p className="text-sm text-slate-500">Comments</p>
          </div>
          <div className="text-center">
            <p className="text-2xl font-bold text-slate-700">{stats.largestCluster}</p>
            <p className="text-sm text-slate-500">Largest</p>
          </div>
          <div className="text-center">
            <p className="text-2xl font-bold text-slate-700">{stats.smallestCluster}</p>
            <p className="text-sm text-slate-500">Smallest</p>
          </div>
        </div>
        
        <Link
          href="/clusters"
          className="block w-full text-center bg-slate-700 text-white py-3 px-6 rounded-lg font-medium hover:bg-slate-800 transition-all transform hover:scale-[1.02] hover:shadow-lg border border-slate-700"
        >
          View Cluster Visualization →
        </Link>
      </Card.Body>
    </Card>
  );
}