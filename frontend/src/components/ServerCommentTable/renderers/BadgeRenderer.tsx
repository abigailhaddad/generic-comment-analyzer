import React from 'react';
import Badge, { getBadgeTypeFromClass, BadgeType } from '@/components/ui/Badge';
import { RendererProps } from '../types';

export function BadgeRenderer({ value, field, searchQuery }: RendererProps<string>) {
  let badgeClass: string | undefined = undefined;

  if (field.badges) {
    // We rely on createFieldRenderer to ensure 'value' is a valid key if field.badges exists.
    // However, TypeScript here doesn't know that 'value' is a key of this specific field.badges type.
    // We can cast field.badges to a more general type that allows string indexing.
    badgeClass = (field.badges as Record<string, string | undefined>)[value];
  }

  const badgeType = getBadgeTypeFromClass(badgeClass || '');
  
  return (
    <Badge 
      type={badgeType as BadgeType} 
      label={value}
      highlight={searchQuery}
      filterType={field.key}
    />
  );
} 