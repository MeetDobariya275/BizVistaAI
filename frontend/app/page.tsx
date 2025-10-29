"use client";
import { useQuery } from "@tanstack/react-query";
import { fetchBusinesses } from "@/lib/api";
import Link from "next/link";

export default function Home() {
  const { data, isLoading } = useQuery({
    queryKey: ["biz"],
    queryFn: fetchBusinesses,
  });

  if (isLoading) return <div className="p-6">Loading…</div>;

  return (
    <main className="min-h-screen bg-[#1a1a1a] text-white p-6">
      <div className="max-w-7xl mx-auto">
        <header className="mb-8">
          <h1 className="text-3xl font-bold mb-2">BizVista AI</h1>
          <p className="text-gray-400">Restaurant Review Analytics Dashboard</p>
        </header>

        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
          {data?.map((b) => (
            <Link
              key={b.id}
              href={`/biz/${b.id}`}
              className="block bg-[#2c2c2c] rounded-lg p-6 hover:bg-[#333333] transition-colors border border-[#3a3a3a]"
            >
              <div className="flex items-start justify-between mb-3">
                <h2 className="text-xl font-semibold">{b.name}</h2>
                <div className="flex items-center gap-1 text-yellow-400">
                  <span className="text-lg">★</span>
                  <span className="font-medium">{b.stars.toFixed(1)}</span>
                </div>
              </div>

              <div className="space-y-2 text-sm text-gray-400">
                <div className="flex items-center gap-2">
                  <span className="w-2 h-2 bg-green-500 rounded-full"></span>
                  {b.city}
                </div>
                <div>
                  {b.category || "Restaurant"}
                </div>
                <div className="pt-2 border-t border-[#3a3a3a]">
                  {b.review_count.toLocaleString()} reviews
                </div>
              </div>
            </Link>
          ))}
        </div>

        <div className="mt-8 text-center">
          <Link
            href="/compare"
            className="inline-block px-6 py-3 bg-blue-600 hover:bg-blue-700 rounded-lg font-medium transition-colors"
          >
            Compare Restaurants
          </Link>
        </div>
      </div>
    </main>
  );
}