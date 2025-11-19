"use client";
import { useEffect } from "react";
import { useRouter } from "next/navigation";

export default function DashboardRedirect() {
  const router = useRouter();

  useEffect(() => {
    router.replace("/keyword-dashboard");
  }, [router]);

  return (
    <main className="min-h-screen bg-[#1a1a1a] text-white p-6 flex items-center justify-center">
      <div className="text-gray-400">Loading...</div>
    </main>
  );
}
