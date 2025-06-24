import React from "react";
import Badge from "@/components/ui/Badge";

const BetaBadge = () => (
  <>
    {/* Desktop Beta Badge - diagonal in top-right corner, hidden on mobile */}
    <div className="absolute right-0 top-0 z-50 overflow-hidden w-28 h-28 pointer-events-none hidden md:block">
      <div className="absolute top-8 right-[-35px] rotate-45 w-[170px] text-center">
        <Badge 
          type="warning" 
          label="BETA" 
          className="!rounded-none !px-0 w-full !bg-amber-600 !text-white font-bold shadow-md"
        />
      </div>
    </div>
    {/* Mobile Beta Badge - simple horizontal badge in top right */}
    <div className="absolute right-3 top-2 z-50 pointer-events-none md:hidden">
      <Badge 
        type="warning" 
        label="BETA" 
        className="!bg-amber-600 !text-white font-bold !text-xs"
      />
    </div>
  </>
);

export default BetaBadge; 