// app/page.tsx
import { redirect } from 'next/navigation';

export default function Home() {
  // Redirect to the default page and size
  redirect('/page/1/size/10');
}