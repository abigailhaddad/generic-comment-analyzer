'use client';

import ServerFilterSection from './ServerFilterSection';
import ServerCommentTable from './ServerCommentTable';
import ServerStatisticsCard from './ServerStatisticsCard';
import DedupedStatisticsCard from './DedupedStatisticsCard';
import StanceOverTimeClient from './StanceOverTime/StanceOverTimeClient';
import Card from '@/components/ui/Card';

export default function ServerDataState() {
  return (
    <div className="container-fluid py-4">
      {/* First row: Statistics and Filters side by side */}
      <div className="row mb-4">
        <div className="col-md-6">
          <ServerStatisticsCard />
          <div className="mt-4">
            <DedupedStatisticsCard />
          </div>
        </div>
        <div className="col-md-6">
          <ServerFilterSection />
        </div>
      </div>

      {/* Second row: Stance Over Time Chart */}
      <div className="row mb-4">
        <div className="col-12">
          <StanceOverTimeClient />
        </div>
      </div>
      
      {/* Third row: Data Table */}
      <div className="row">
        <div className="col-12">
          <Card collapsible={true} initiallyCollapsed={false}>
            {/* <Card.Header>Optional: Table Title</Card.Header> */}
            <Card.Body className="table-responsive">
              <ServerCommentTable />
            </Card.Body>
          </Card>
        </div>
      </div>
    </div>
  );
}