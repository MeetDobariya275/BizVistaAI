"use client";
import { useEffect } from "react";
import { useRouter } from "next/navigation";
import { useQuery } from "@tanstack/react-query";
import { fetchBusinesses } from "@/lib/api";

export default function DashboardRedirect() {
  const router = useRouter();
  const { data: businesses, isLoading } = useQuery({
    queryKey: ["biz"],
    queryFn: fetchBusinesses,
  });

  useEffect(() => {
    if (businesses && businesses.length > 0) {
      // Get business with highest review_count
      const topBusiness = businesses.reduce((prev, curr) =>
        curr.review_count > prev.review_count ? curr : prev
      );
      router.replace(`/dashboard/${topBusiness.id}`);
    }
  }, [businesses, router]);

  if (isLoading) {
    return (
      <main className="min-h-screen bg-[#1a1a1a] text-white p-6 flex items-center justify-center">
        <div className="text-gray-400">Loading...</div>
      </main>
    );
  }

  return null;
}
