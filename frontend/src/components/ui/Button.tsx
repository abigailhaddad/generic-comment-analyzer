'use client';

import React from 'react';

type ButtonVariant = 'primary' | 'secondary' | 'outline' | 'danger' | 'success';
type ButtonSize = 'sm' | 'md' | 'lg';

interface ButtonProps {
  variant?: ButtonVariant;
  size?: ButtonSize;
  onClick?: () => void;
  disabled?: boolean;
  type?: 'button' | 'submit' | 'reset';
  children: React.ReactNode;
  className?: string;
  title?: string;
  style?: React.CSSProperties;
  ariaLabel?: string;
}

export default function Button({
  variant = 'primary',
  size = 'md',
  onClick,
  disabled = false,
  type = 'button',
  children,
  className = '',
  title,
  style,
  ariaLabel
}: ButtonProps) {
  // Get variant classes based on standard names
  const getVariantClasses = () => {
    switch (variant) {
      case 'primary':
        return 'bg-slate-700 hover:bg-slate-800 text-white border border-slate-700';
      case 'secondary':
        return 'bg-white hover:bg-slate-50 text-slate-700 border border-slate-300';
      case 'outline':
        return 'bg-transparent hover:bg-slate-100 text-slate-700 border border-slate-300';
      case 'danger':
        return 'bg-rose-600 hover:bg-rose-700 text-white border border-rose-600';
      case 'success':
        return 'bg-emerald-600 hover:bg-emerald-700 text-white border border-emerald-600';
      default:
        return 'bg-slate-700 hover:bg-slate-800 text-white border border-slate-700';
    }
  };

  // Get size classes
  const getSizeClasses = () => {
    switch (size) {
      case 'sm':
        return 'px-3 py-1.5 text-sm';
      case 'md':
        return 'px-4 py-2 text-base';
      case 'lg':
        return 'px-6 py-3 text-lg';
      default:
        return 'px-4 py-2 text-base';
    }
  };

  // Combine all classes
  const buttonClasses = `inline-flex items-center justify-center rounded-md font-medium transition-colors focus:outline-none focus:ring-2 focus:ring-slate-400 focus:ring-opacity-50 disabled:opacity-50 disabled:cursor-not-allowed ${getVariantClasses()} ${getSizeClasses()} ${className}`;

  return (
    <button
      type={type}
      className={buttonClasses}
      onClick={onClick}
      disabled={disabled}
      aria-disabled={disabled}
      title={title}
      style={style}
      aria-label={ariaLabel}
    >
      {children}
    </button>
  );
} 