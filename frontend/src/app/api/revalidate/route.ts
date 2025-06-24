// app/api/revalidate/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { revalidatePath } from 'next/cache';
import { clearCommentsCache } from '@/lib/actions';

export async function POST(request: NextRequest) {
  try {
    // Optional: Add authentication
    const authHeader = request.headers.get('authorization');
    const token = process.env.REVALIDATION_TOKEN;
    
    if (token && authHeader !== `Bearer ${token}`) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
    }
    
    // Clear the cache
    await clearCommentsCache();
    
    // Revalidate the home page (and any other pages you want)
    revalidatePath('/', 'page');
    revalidatePath('/', 'layout');
    
    // Update the LAST_DATA_UPDATE environment variable
    // Note: This would typically be done during your data update process
    
    return NextResponse.json({ 
      revalidated: true, 
      timestamp: new Date().toISOString() 
    });
  } catch (error) {
    console.error('Error revalidating:', error);
    return NextResponse.json(
      { error: 'Error revalidating' }, 
      { status: 500 }
    );
  }
}

// Optional: GET endpoint for testing
export async function GET(request: NextRequest) {
  // Check for secret token
  const { searchParams } = new URL(request.url);
  const secret = searchParams.get('secret');
  
  if (process.env.REVALIDATION_TOKEN && secret !== process.env.REVALIDATION_TOKEN) {
    return NextResponse.json({ error: 'Invalid token' }, { status: 401 });
  }
  
  try {
    await clearCommentsCache();
    revalidatePath('/', 'page');
    revalidatePath('/', 'layout');
    
    return NextResponse.json({ 
      revalidated: true, 
      timestamp: new Date().toISOString() 
    });
  } catch (error) {
    console.error('Error revalidating (GET):', error);
    return NextResponse.json(
      { error: 'Error revalidating' }, 
      { status: 500 }
    );
  }
}