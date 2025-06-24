'use client';

import { useRouter } from 'next/navigation';

interface BackButtonProps {
  returnUrl: string;
  label?: string;
}

export default function BackButton({ 
  returnUrl,
  label = 'Back to Results'
}: BackButtonProps) {
  const router = useRouter();

  const handleBack = () => {
    router.push(returnUrl);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault();
      handleBack();
    }
  };

  return (
    <button
      onClick={handleBack}
      onKeyDown={handleKeyDown}
      className="mb-4 inline-flex items-center px-4 py-2 bg-gray-100 hover:bg-gray-200 text-gray-800 rounded-md transition-colors focus:outline-none focus:ring-2 focus:ring-blue-500"
      aria-label={label}
    >
      <svg 
        xmlns="http://www.w3.org/2000/svg" 
        className="h-5 w-5 mr-2" 
        fill="none" 
        viewBox="0 0 24 24" 
        stroke="currentColor"
        aria-hidden="true"
      >
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 19l-7-7m0 0l7-7m-7 7h18" />
      </svg>
      {label}
    </button>
  );
} 