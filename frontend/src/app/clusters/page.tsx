import { Metadata } from 'next';
import { getClusterData } from '@/lib/actions/clusters';
import ClusterVisualization from '@/components/ClusterVisualization';

// Next.js 13+ App Router revalidation
export const revalidate = 86400; // 24 hours in seconds

export const metadata: Metadata = {
  title: 'Comment Clusters - Schedule F Analysis',
  description: 'Visualization of comment clusters using PCA analysis',
};

export default async function ClustersPage() {
  // Fetch 80% of data points from each cluster for the main view
  const clusterResponse = await getClusterData(0.8); 

  // Pass the data with the correct prop names
  return (
    <main className="min-h-screen bg-gray-50 flex flex-col">
      <div className="flex-1 flex flex-col">
        <div className="px-4 py-6">
          <h1 className="text-3xl font-bold">Comment Cluster Analysis</h1>
        </div>
        <div className="flex-1">
          <ClusterVisualization 
            initialData={clusterResponse.data || null}
            isLoading={false}
            error={clusterResponse.error || null}
            initialSelectedCluster={null}
          />
        </div>
      </div>
    </main>
  );
}