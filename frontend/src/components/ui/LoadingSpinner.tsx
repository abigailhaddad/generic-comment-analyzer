'use client';

interface LoadingSpinnerProps {
  size?: 'small' | 'medium' | 'large';
  color?: 'blue' | 'gray' | 'white';
  label?: string;
}

export default function LoadingSpinner({ 
  size = 'medium', 
  color = 'blue',
  label = 'Loading' 
}: LoadingSpinnerProps) {
  // Determine size classes
  let sizeClasses = '';
  switch (size) {
    case 'small':
      sizeClasses = 'w-5 h-5';
      break;
    case 'medium':
      sizeClasses = 'w-8 h-8';
      break;
    case 'large':
      sizeClasses = 'w-12 h-12';
      break;
  }
  
  // Determine color classes
  let colorClasses = '';
  switch (color) {
    case 'blue':
      colorClasses = 'text-blue-600';
      break;
    case 'gray':
      colorClasses = 'text-gray-600';
      break;
    case 'white':
      colorClasses = 'text-white';
      break;
  }
  
  return (
    <div 
      className="flex justify-center items-center"
      role="status"
      aria-live="polite"
      aria-busy="true"
    >
      <svg 
        className={`animate-spin ${sizeClasses} ${colorClasses}`} 
        xmlns="http://www.w3.org/2000/svg" 
        fill="none" 
        viewBox="0 0 24 24"
        aria-hidden="true"
      >
        <circle 
          className="opacity-25" 
          cx="12" 
          cy="12" 
          r="10" 
          stroke="currentColor" 
          strokeWidth="4"
        ></circle>
        <path 
          className="opacity-75" 
          fill="currentColor" 
          d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
        ></path>
      </svg>
      <span className="sr-only">{label}</span>
    </div>
  );
} 