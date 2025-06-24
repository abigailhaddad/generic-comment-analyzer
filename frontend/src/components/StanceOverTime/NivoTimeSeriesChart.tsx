// src/components/StanceOverTime/NivoTimeSeriesChart.tsx
'use client';

import { ResponsiveLine } from '@nivo/line';
import { StanceData } from './types';

interface NivoTimeSeriesChartProps {
  data: StanceData[];
}

export default function NivoTimeSeriesChart({ data }: NivoTimeSeriesChartProps) {
  if (!data || data.length === 0) {
    return <div className="text-center text-gray-500 p-8">No data available</div>;
  }

  // Transform data for Nivo
  const chartData = [
    {
      id: 'For',
      color: '#10b981',
      data: data.map(d => ({ x: d.date, y: d.For }))
    },
    {
      id: 'Against', 
      color: '#ef4444',
      data: data.map(d => ({ x: d.date, y: d.Against }))
    },
    {
      id: 'Neutral/Unclear',
      color: '#64748b',
      data: data.map(d => ({ x: d.date, y: d['Neutral/Unclear'] }))
    }
  ];

  return (
    <div style={{ height: 400 }}>
      <ResponsiveLine
        data={chartData}
        margin={{ top: 20, right: 20, bottom: 60, left: 100 }}
        xScale={{ 
          type: 'time',
          format: '%Y-%m-%d',
          useUTC: false,
          precision: 'day',
        }}
        xFormat="time:%b %d"
        yScale={{
          type: 'linear',
          min: 0,
          max: 'auto',
          stacked: false,
        }}
        axisTop={null}
        axisRight={null}
        axisBottom={{
          format: '%b %d',
          tickValues: 'every 2 weeks',
          tickRotation: -45,
          legend: 'Date',
          legendOffset: 50,
          legendPosition: 'middle'
        }}
        axisLeft={{
          legend: 'Number of Comments',
          legendOffset: -40,
          legendPosition: 'middle'
        }}
        colors={d => d.color}
        pointSize={6}
        pointColor={{ theme: 'background' }}
        pointBorderWidth={2}
        pointBorderColor={{ from: 'serieColor' }}
        pointLabelYOffset={-12}
        useMesh={true}
        enableSlices="x"
        sliceTooltip={({ slice }) => (
          <div className="bg-white p-3 shadow-lg rounded-md border">
            <div className="font-semibold mb-2">
              {new Date(slice.points[0].data.x as string).toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric' })}
            </div>
            {slice.points.map(point => (
              <div key={point.id} className="flex items-center gap-2 text-sm">
                <div 
                  className="w-3 h-3 rounded-full" 
                  style={{ backgroundColor: point.seriesColor }} // Fixed: serieColor → seriesColor
                />
                <span>{point.seriesId}:</span> {/* Fixed: serieId → seriesId */}
                <span className="font-medium">{point.data.yFormatted}</span>
              </div>
            ))}
          </div>
        )}
        legends={[
          {
            anchor: 'top-left',
            direction: 'row',
            justify: false,
            translateX: 0,
            translateY: -10,
            itemsSpacing: 0,
            itemDirection: 'bottom-to-top',
            itemWidth: 70,
            itemHeight: 20,
            itemOpacity: 0.75,
            symbolSize: 12,
            symbolShape: 'circle',
          }
        ]}
      />
    </div>
  );
}