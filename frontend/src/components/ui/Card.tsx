'use client';

import React, { useState, createContext, useContext } from 'react';

// Context for Card collapsibility
interface CardContextProps {
  collapsible: boolean;
  isCollapsed: boolean;
  toggleCollapse: () => void;
}
const CardContext = createContext<CardContextProps | undefined>(undefined);

interface CardProps {
  children: React.ReactNode;
  className?: string;
  role?: string;
  ariaLabel?: string;
  collapsible: boolean; // Now required
  initiallyCollapsed?: boolean;
  onToggleCollapse?: (isCollapsed: boolean) => void;
}

interface CardHeaderProps {
  children: React.ReactNode;
  className?: string;
  id?: string;
}

interface CardBodyProps {
  children: React.ReactNode;
  className?: string;
  noPadding?: boolean;
}

interface CardFooterProps {
  children: React.ReactNode;
  className?: string;
}

function Card({ 
  children, 
  className = '', 
  role, 
  ariaLabel, 
  collapsible, 
  initiallyCollapsed = false, 
  onToggleCollapse 
}: CardProps) {
  const [isCollapsed, setIsCollapsed] = useState(initiallyCollapsed);

  const toggleCollapse = () => {
    if (collapsible) { // Only toggle if the card is collapsible
      const newState = !isCollapsed;
      setIsCollapsed(newState);
      if (onToggleCollapse) {
        onToggleCollapse(newState);
      }
    }
  };
  
  // If not collapsible, ensure isCollapsed is false so body always shows
  const effectiveIsCollapsed = collapsible ? isCollapsed : false;

  return (
    <CardContext.Provider value={{ collapsible, isCollapsed: effectiveIsCollapsed, toggleCollapse }}>
      <section 
        className={`card shadow-sm mb-6 border border-slate-200 ${className}`}
        role={role}
        aria-label={ariaLabel}
      >
        {children}
      </section>
    </CardContext.Provider>
  );
}

function CardHeader({ children, className = '', id }: CardHeaderProps) {
  const context = useContext(CardContext);

  if (!context) {
    // Context not found, render header without collapse functionality
    return <header className={`card-header ${className}`} id={id}>{children}</header>;
  }

  const { collapsible, isCollapsed, toggleCollapse } = context;

  return (
    <header 
      className={`card-header ${className} flex justify-between items-center`}
      id={id}
    >
      <div className="flex-grow">{children}</div>
      
      {collapsible && (
        <button 
          onClick={toggleCollapse}
          className="ml-2 p-1.5 text-slate-600 hover:text-slate-800 hover:bg-slate-100 rounded-full transition-colors duration-150 ease-in-out focus:outline-none focus:ring-2 focus:ring-slate-300 focus:ring-opacity-75"
          aria-label={isCollapsed ? "Expand card" : "Collapse card"}
          title={isCollapsed ? "Expand card" : "Collapse card"}
          aria-expanded={!isCollapsed}
        >
          <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className={`w-5 h-5 transform transition-transform duration-200 ease-in-out ${isCollapsed ? '-rotate-90' : ''}`}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M19.5 8.25l-7.5 7.5-7.5-7.5" />
          </svg>
        </button>
      )}
    </header>
  );
}

function CardBody({ children, className = '', noPadding = false }: CardBodyProps) {
  const context = useContext(CardContext);
  
  // If context is undefined (e.g. CardBody used outside Card or non-collapsible card context not provided)
  // or if the card is not collapsible, render children directly.
  if (!context || !context.collapsible) {
    return <div className={`card-body ${noPadding ? 'p-0' : ''} ${className}`}>{children}</div>;
  }

  const { isCollapsed } = context;

  if (isCollapsed) {
    return null; // Don't render body if collapsed
  }

  return (
    <div className={`card-body ${noPadding ? 'p-0' : ''} ${className}`}>
      {children}
    </div>
  );
}

function CardFooter({ children, className = '' }: CardFooterProps) {
  // Footer is typically not affected by collapse, but could be if desired.
  // For now, it renders normally.
  return (
    <footer className={`card-footer ${className}`}>
      {children}
    </footer>
  );
}

Card.Header = CardHeader;
Card.Body = CardBody;
Card.Footer = CardFooter;

export default Card; 