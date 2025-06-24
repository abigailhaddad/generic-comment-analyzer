'use client';

import Card from '@/components/ui/Card';
import Skeleton from '@/components/ui/Skeleton';

const LoadingSkeleton = () => {
  return (
    <Card className="h-96" collapsible={false}>
      <Card.Body className="flex items-center justify-center h-full">
        <Skeleton variant="rounded" width={400} height={256} />
      </Card.Body>
    </Card>
  );
};

export default LoadingSkeleton; 