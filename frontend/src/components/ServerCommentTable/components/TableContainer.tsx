import React from 'react';
import Card from '@/components/ui/Card';

interface TableContainerProps {
  children: React.ReactNode;
}

export function TableContainer({ children }: TableContainerProps) {
  return (
    <Card collapsible={false} className="bg-white rounded-lg shadow-md border border-gray-200">
      <div className="flex flex-col h-full min-h-0">
        {children}
      </div>
    </Card>
  );
} 