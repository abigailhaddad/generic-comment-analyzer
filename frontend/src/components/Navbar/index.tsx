"use client";

import { useState, useEffect } from "react";
import { datasetConfig } from "@/lib/config";
import BetaBadge from "./BetaBadge";
import LogoTitle from "./LogoTitle";
import DesktopNav from "./DesktopNav";
import MobileMenuButton from "./MobileMenuButton";
import MobileNav from "./MobileNav";

// Simple hook to check if screen is md or larger
function useIsDesktop() {
  const [isDesktop, setIsDesktop] = useState(false);
  useEffect(() => {
    const check = () => setIsDesktop(window.matchMedia("(min-width: 768px)").matches);
    check();
    window.addEventListener("resize", check);
    return () => window.removeEventListener("resize", check);
  }, []);
  return isDesktop;
}

export default function Navbar() {
  const { title, subtitle, description } = datasetConfig;
  const [isMenuOpen, setIsMenuOpen] = useState(false);
  const [scrolled, setScrolled] = useState(false);
  const isDesktop = useIsDesktop();

  // Handle scroll effect
  useEffect(() => {
    const handleScroll = () => {
      setScrolled(window.scrollY > 10);
    };
    window.addEventListener("scroll", handleScroll);
    return () => window.removeEventListener("scroll", handleScroll);
  }, []);

  // Toggle menu and handle keyboard navigation
  const toggleMenu = () => {
    setIsMenuOpen(!isMenuOpen);
  };

  // Handle keyboard events for menu button
  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Escape" && isMenuOpen) {
      setIsMenuOpen(false);
    }
  };

  return (
    <div className="relative">
      <BetaBadge />
      <nav
        className={`fixed top-0 left-0 right-0 z-40 transition-all duration-300 ${
          scrolled
            ? "bg-white/95 backdrop-blur-sm shadow-sm border-b border-slate-200 py-2"
            : "bg-white shadow-sm border-b border-slate-200 py-3"
        } text-slate-800`}
        aria-label="Main navigation"
      >
        <div className="container mx-auto px-4">
          <div className="flex justify-between items-center">
            <LogoTitle title={title} description={description} scrolled={scrolled} />
            {isDesktop ? (
              <DesktopNav subtitle={subtitle} />
            ) : (
              subtitle && (
                <MobileMenuButton
                  isMenuOpen={isMenuOpen}
                  toggleMenu={toggleMenu}
                  handleKeyDown={handleKeyDown}
                />
              )
            )}
          </div>
          {!isDesktop && subtitle && (
            <MobileNav
              isMenuOpen={isMenuOpen}
              setIsMenuOpen={setIsMenuOpen}
              subtitle={subtitle}
            />
          )}
        </div>
      </nav>
    </div>
  );
} 