"use client";
import { useEffect } from "react";
import { useRouter } from "next/navigation";

export default function Home() {
  const router = useRouter();

  useEffect(() => {
    // Redirect directly to keyword dashboard
    router.replace("/keyword-dashboard");
  }, [router]);

  return (
    <main className="min-h-screen bg-[#1a1a1a] text-white p-6 flex items-center justify-center">
      <div className="text-gray-400">Redirecting to dashboard...</div>
    </main>
  );
}