'use client';

import React, { useEffect, useRef } from 'react';
import { createPortal } from 'react-dom';

interface ModalProps {
  isOpen: boolean;
  onClose: () => void;
  title: string;
  children: React.ReactNode;
  footer?: React.ReactNode;
  id?: string;
}

export default function Modal({ isOpen, onClose, title, children, footer, id = 'modal' }: ModalProps) {
  const modalRef = useRef<HTMLDivElement>(null);
  const previousFocusRef = useRef<HTMLElement | null>(null);
  
  // Close modal when clicking outside the modal content
  const handleBackdropClick = (e: React.MouseEvent) => {
    if (modalRef.current && !modalRef.current.contains(e.target as Node)) {
      onClose();
    }
  };
  
  // Handle ESC key to close modal and trap focus within modal
  useEffect(() => {
    const handleEscKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && isOpen) {
        onClose();
      }
    };
    
    const handleTabKey = (e: KeyboardEvent) => {
      if (e.key === 'Tab' && modalRef.current) {
        // Find all focusable elements in modal
        const focusableElements = modalRef.current.querySelectorAll(
          'button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])'
        );
        
        if (focusableElements.length === 0) return;
        
        const firstElement = focusableElements[0] as HTMLElement;
        const lastElement = focusableElements[focusableElements.length - 1] as HTMLElement;
        
        // If shift+tab and on first element, loop to last
        if (e.shiftKey && document.activeElement === firstElement) {
          e.preventDefault();
          lastElement.focus();
        } 
        // If tab and on last element, loop to first
        else if (!e.shiftKey && document.activeElement === lastElement) {
          e.preventDefault();
          firstElement.focus();
        }
      }
    };
    
    if (isOpen) {
      // Store current active element to restore focus later
      previousFocusRef.current = document.activeElement as HTMLElement;
      
      document.addEventListener('keydown', handleEscKey);
      document.addEventListener('keydown', handleTabKey);
      
      // Prevent scrolling of the body when modal is open
      document.body.style.overflow = 'hidden';
      
      // Focus the modal or first focusable element inside
      setTimeout(() => {
        if (modalRef.current) {
          const focusableElement = modalRef.current.querySelector(
            'button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])'
          ) as HTMLElement;
          
          if (focusableElement) {
            focusableElement.focus();
          } else {
            modalRef.current.focus();
          }
        }
      }, 0);
    }
    
    return () => {
      document.removeEventListener('keydown', handleEscKey);
      document.removeEventListener('keydown', handleTabKey);
      document.body.style.overflow = '';
      
      // Restore focus when modal closes
      if (previousFocusRef.current && !isOpen) {
        previousFocusRef.current.focus();
      }
    };
  }, [isOpen, onClose]);
  
  if (!isOpen) return null;
  
  // Use createPortal to render modal at the end of document body
  return createPortal(
    <div 
      className="modal-backdrop"
      onClick={handleBackdropClick}
      role="presentation"
    >
      <div 
        className="modal-dialog"
        role="dialog"
        aria-modal="true"
        aria-labelledby={`${id}-title`}
      >
        <div 
          ref={modalRef} 
          className="modal-content"
          tabIndex={-1}
        >
          <div className="modal-header">
            <h3 className="modal-title" id={`${id}-title`}>{title}</h3>
            <button 
              className="btn-close"
              onClick={onClose}
              aria-label="Close"
            >
              <span aria-hidden="true">×</span>
            </button>
          </div>
          <div className="modal-body">
            {children}
          </div>
          {footer && (
            <div className="modal-footer">
              {footer}
            </div>
          )}
        </div>
      </div>
    </div>,
    document.body
  );
} 