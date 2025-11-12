"use client";
import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchBusinesses, fetchCompareNarrative } from "@/lib/api";
import Link from "next/link";
import clsx from "clsx";

export default function ComparePage() {
  const { data: all, isLoading: loadingAll } = useQuery({
    queryKey: ["biz"],
    queryFn: fetchBusinesses,
  });
  
  const [selected, setSelected] = useState<string[]>([]);
  
  const { data: narrative, isLoading: loadingNarrative } = useQuery({
    queryKey: ["cmp-narrative", selected],
    queryFn: () => fetchCompareNarrative(selected),
    enabled: selected.length >= 2,
  });
  
  const toggleBusiness = (id: string) => {
    setSelected((s) =>
      s.includes(id)
        ? s.filter((x) => x !== id)
        : s.length < 3
        ? [...s, id]
        : s
    );
  };

  const selectedBusinesses = all?.filter((b) => selected.includes(b.id)) || [];

  return (
    <main className="min-h-screen bg-[#1a1a1a] text-white p-6">
      <div className="max-w-7xl mx-auto">
        {/* Breadcrumb */}
        <div className="mb-6 flex items-center gap-2 text-sm text-gray-400">
          <Link href="/" className="hover:text-white">
            Dashboard
          </Link>
          <span>/</span>
          <span>Compare</span>
        </div>

        {/* Header */}
        <header className="mb-8">
          <h1 className="text-3xl font-bold">Compare Restaurants</h1>
          <p className="text-gray-400 mt-2">
            Select up to 3 restaurants to compare insights
          </p>
        </header>

        {/* Business Selector */}
        <section className="bg-[#2c2c2c] rounded-lg p-6 mb-6 border border-[#3a3a3a]">
          <h2 className="text-lg font-semibold mb-4">
            Select Businesses ({selected.length}/3)
          </h2>
          <div className="flex flex-wrap gap-2">
            {loadingAll ? (
              <div>Loading businesses...</div>
            ) : (
              all?.map((b) => (
                <button
                  key={b.id}
                  onClick={() => toggleBusiness(b.id)}
                  disabled={!selected.includes(b.id) && selected.length >= 3}
                  className={clsx(
                    "text-sm px-4 py-2 rounded-lg font-medium transition-all",
                    selected.includes(b.id)
                      ? "bg-blue-600 text-white hover:bg-blue-700"
                      : selected.length >= 3
                      ? "bg-[#2a2a2a] text-gray-500 cursor-not-allowed opacity-50"
                      : "bg-[#3a3a3a] text-gray-300 hover:bg-[#4a4a4a]"
                  )}
                >
                  {b.name}
                </button>
              ))
            )}
          </div>
        </section>

        {/* Narrative Content */}
        {loadingNarrative && selected.length >= 2 && (
          <div className="text-center py-8 text-gray-400">
            Generating comparison narrative...
          </div>
        )}

        {narrative && selected.length >= 2 && (
          <div className="space-y-6">
            {/* Summary */}
            <section className="bg-[#2c2c2c] rounded-lg p-6 border border-[#3a3a3a]">
              <div className="flex items-center justify-between mb-4">
                <h2 className="text-lg font-semibold">Summary</h2>
                <span className={clsx(
                  "text-xs px-2 py-1 rounded",
                  narrative.source === "llm" ? "bg-green-600 text-white" : "bg-yellow-600 text-white"
                )}>
                  {narrative.source === "llm" ? "AI Generated" : "Fallback"}
                </span>
              </div>
              <p className="text-gray-300 leading-relaxed">{narrative.summary}</p>
            </section>

            {/* By Theme */}
            {(() => {
              // Handle both array and object formats
              const byTheme = Array.isArray(narrative.by_theme) 
                ? narrative.by_theme 
                : typeof narrative.by_theme === 'object' && narrative.by_theme !== null
                ? Object.entries(narrative.by_theme).map(([k, v]) => `${k}: ${v}`)
                : [];
              
              return byTheme.length > 0 && (
                <section className="bg-[#2c2c2c] rounded-lg p-6 border border-[#3a3a3a]">
                  <h2 className="text-lg font-semibold mb-4">By Theme</h2>
                  <ul className="space-y-2">
                    {byTheme.map((item, i) => (
                      <li key={i} className="text-gray-300 flex items-start gap-2">
                        <span className="text-blue-500 mt-1">•</span>
                        <span>{item}</span>
                      </li>
                    ))}
                  </ul>
                </section>
              );
            })()}

            {/* Risks & Opportunities */}
            <div className="grid md:grid-cols-2 gap-6">
              {narrative.risks.length > 0 && (
                <section className="bg-[#2c2c2c] rounded-lg p-6 border border-[#3a3a3a]">
                  <h2 className="text-lg font-semibold mb-4">Risks</h2>
                  <ul className="space-y-2">
                    {narrative.risks.map((item, i) => (
                      <li key={i} className="text-gray-300 flex items-start gap-2">
                        <span className="text-red-500 mt-1">•</span>
                        <span>{item}</span>
                      </li>
                    ))}
                  </ul>
                </section>
              )}

              {narrative.opportunities.length > 0 && (
                <section className="bg-[#2c2c2c] rounded-lg p-6 border border-[#3a3a3a]">
                  <h2 className="text-lg font-semibold mb-4">Opportunities</h2>
                  <ul className="space-y-2">
                    {narrative.opportunities.map((item, i) => (
                      <li key={i} className="text-gray-300 flex items-start gap-2">
                        <span className="text-green-500 mt-1">•</span>
                        <span>{item}</span>
                      </li>
                    ))}
                  </ul>
                </section>
              )}
            </div>
          </div>
        )}

        {selected.length === 0 && (
          <div className="text-center py-12 text-gray-400">
            Select up to 3 restaurants above to compare
          </div>
        )}

        {/* Navigation */}
        <div className="flex gap-4 mt-6">
          <Link
            href="/"
            className="px-6 py-3 bg-[#3a3a3a] hover:bg-[#4a4a4a] rounded-lg font-medium transition-colors"
          >
            Back to Dashboard
          </Link>
        </div>
      </div>
    </main>
  );
}