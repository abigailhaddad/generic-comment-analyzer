// Sophisticated, trustworthy color palette
export const colors = {
  // Primary colors - muted blues and grays for trustworthiness
  primary: {
    50: '#f8fafc',   // Very light blue-gray
    100: '#f1f5f9',  // Light blue-gray
    200: '#e2e8f0',  // Soft blue-gray
    300: '#cbd5e1',  // Medium blue-gray
    400: '#94a3b8',  // Muted blue-gray
    500: '#64748b',  // Base slate
    600: '#475569',  // Dark slate
    700: '#334155',  // Darker slate
    800: '#1e293b',  // Very dark slate
    900: '#0f172a',  // Nearly black
  },
  
  // Accent colors - very subtle and sophisticated
  accent: {
    blue: '#3b82f6',    // Softer blue
    green: '#10b981',   // Softer emerald
    amber: '#f59e0b',   // Warmer amber
    rose: '#f43f5e',    // Softer rose
  },
  
  // Semantic colors - muted for sophistication
  semantic: {
    success: {
      light: '#dcfce7',  // Very light green
      DEFAULT: '#86efac', // Light green
      dark: '#16a34a',   // Darker green
    },
    error: {
      light: '#fee2e2',  // Very light red
      DEFAULT: '#fca5a5', // Light red
      dark: '#dc2626',   // Darker red
    },
    warning: {
      light: '#fef3c7',  // Very light amber
      DEFAULT: '#fde047', // Light amber
      dark: '#ca8a04',   // Darker amber
    },
    neutral: {
      light: '#f3f4f6',  // Very light gray
      DEFAULT: '#e5e7eb', // Light gray
      dark: '#6b7280',   // Darker gray
    },
  },
  
  // Background gradients - very subtle
  gradients: {
    primary: 'from-slate-50 to-slate-100',           // Very subtle gray
    secondary: 'from-blue-50 to-slate-50',           // Subtle blue to gray
    accent: 'from-slate-100 to-blue-50',             // Subtle gray to blue
    surface: 'from-white to-slate-50',               // Almost imperceptible
  },
  
  // Text colors
  text: {
    primary: '#1e293b',   // Dark slate
    secondary: '#475569', // Medium slate
    muted: '#64748b',     // Muted slate
    inverse: '#f8fafc',   // Light for dark backgrounds
  },
};

// Header gradient classes for different card types
export const headerGradients = {
  default: 'bg-gradient-to-r from-slate-50 to-slate-100 border-b border-slate-200',
  primary: 'bg-gradient-to-r from-slate-100 to-blue-50 border-b border-slate-200',
  stats: 'bg-gradient-to-r from-white to-slate-50 border-b border-slate-200',
  chart: 'bg-gradient-to-r from-blue-50 to-slate-50 border-b border-blue-100',
  filter: 'bg-gradient-to-r from-slate-50 to-slate-100 border-b border-slate-200',
  cluster: 'bg-gradient-to-r from-slate-100 to-blue-50 border-b border-blue-100',
};

// Stance colors - more muted and sophisticated
export const stanceColors = {
  for: {
    bg: 'bg-emerald-50',
    text: 'text-emerald-700',
    border: 'border-emerald-200',
    badge: 'bg-emerald-100 text-emerald-700',
    chart: '#10b981',
  },
  against: {
    bg: 'bg-rose-50',
    text: 'text-rose-700',
    border: 'border-rose-200',
    badge: 'bg-rose-100 text-rose-700',
    chart: '#f43f5e',
  },
  neutral: {
    bg: 'bg-slate-50',
    text: 'text-slate-700',
    border: 'border-slate-200',
    badge: 'bg-slate-100 text-slate-700',
    chart: '#64748b',
  },
};

// Button variants - subtle and professional
export const buttonVariants = {
  primary: 'bg-slate-700 hover:bg-slate-800 text-white border border-slate-700',
  secondary: 'bg-white hover:bg-slate-50 text-slate-700 border border-slate-300',
  subtle: 'bg-slate-100 hover:bg-slate-200 text-slate-700 border border-slate-200',
  ghost: 'bg-transparent hover:bg-slate-100 text-slate-700',
};