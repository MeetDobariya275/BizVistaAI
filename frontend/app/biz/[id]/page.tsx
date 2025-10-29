"use client";
import { useParams } from "next/navigation";
import { useQuery } from "@tanstack/react-query";
import { fetchOverview } from "@/lib/api";
import Link from "next/link";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
} from "recharts";

export default function BusinessOverview() {
  const { id } = useParams<{ id: string }>();
  const { data, isLoading } = useQuery({
    queryKey: ["ov", id],
    queryFn: () => fetchOverview(id as string),
  });

  if (isLoading) return <div className="p-6">Loading…</div>;
  if (!data) return <div className="p-6">Not found</div>;

  const chartData = data.themes.map((t) => ({
    name: t.theme.replace(/_/g, " ").replace(/\b\w/g, (l) => l.toUpperCase()),
    score: Math.round((t.score + 1) * 50), // Convert -1 to 1 range to 0-100
    delta: t.delta,
    rawScore: t.score,
  }));

  return (
    <main className="min-h-screen bg-[#1a1a1a] text-white p-6">
      <div className="max-w-7xl mx-auto">
        {/* Breadcrumb */}
        <div className="mb-6 flex items-center gap-2 text-sm text-gray-400">
          <Link href="/" className="hover:text-white">
            Dashboard
          </Link>
          <span>/</span>
          <span>{data.business.name}</span>
        </div>

        {/* Header */}
        <header className="mb-8">
          <div className="flex items-center justify-between mb-2">
            <h1 className="text-3xl font-bold">{data.business.name}</h1>
            <div className="flex items-center gap-1 text-yellow-400">
              <span className="text-2xl">★</span>
              <span className="text-xl font-semibold">{data.business.stars}</span>
            </div>
          </div>
          <div className="flex items-center gap-4 text-sm text-gray-400">
            <span>{data.business.city}</span>
            <span>•</span>
            <span>
              Last updated: {new Date(data.last_run || "").toLocaleDateString()}
            </span>
          </div>
        </header>

        {/* Theme Scores - Compact Table */}
        <section className="bg-[#2c2c2c] rounded-lg p-6 mb-6 border border-[#3a3a3a]">
          <h2 className="text-xl font-semibold mb-4">Theme Performance</h2>
          
          <div className="space-y-3">
            {chartData.map((item) => {
              const getColor = (score: number) => {
                if (score >= 70) return "#10b981"; // green
                if (score >= 50) return "#f59e0b"; // yellow
                return "#ef4444"; // red
              };
              
              const delta = item.delta ? item.delta * 50 : 0;
              const deltaColor = delta > 0 ? "text-green-500" : delta < 0 ? "text-red-500" : "text-gray-400";
              
              return (
                <div key={item.name} className="flex items-center gap-4">
                  <div className="w-32 text-sm text-gray-400">{item.name}</div>
                  <div className="flex-1 relative h-6 bg-[#1a1a1a] rounded-full overflow-hidden">
                    <div 
                      className="h-full transition-all duration-300"
                      style={{ 
                        width: `${item.score}%`,
                        backgroundColor: getColor(item.score)
                      }}
                    />
                    <div className="absolute inset-0 flex items-center justify-center text-xs font-semibold">
                      {item.score}%
                    </div>
                  </div>
                  {delta !== 0 && (
                    <div className={`text-sm font-medium w-16 text-right ${deltaColor}`}>
                      {delta > 0 ? '+' : ''}{delta.toFixed(0)}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
          
          <div className="mt-4 pt-4 border-t border-[#3a3a3a] text-xs text-gray-500">
            <div>Score: 0-100% (sentiment scale). Delta: change vs prior period.</div>
          </div>
        </section>

        {/* Insights Cards */}
        <section className="grid md:grid-cols-3 gap-4 mb-6">
          {/* Love */}
          <div className="bg-[#2c2c2c] rounded-lg p-6 border border-[#3a3a3a]">
            <div className="flex items-center gap-2 mb-3">
              <div className="w-8 h-8 bg-green-600 rounded-lg flex items-center justify-center">
                <span className="text-xl">👍</span>
              </div>
              <h3 className="text-lg font-semibold">What Customers Love</h3>
            </div>
            <ul className="space-y-2 text-sm">
              {data.insights.love.map((x, i) => (
                <li key={i} className="flex items-start gap-2">
                  <span className="text-green-500 mt-1">•</span>
                  <span className="text-gray-300">{x}</span>
                </li>
              ))}
            </ul>
          </div>

          {/* Improve */}
          <div className="bg-[#2c2c2c] rounded-lg p-6 border border-[#3a3a3a]">
            <div className="flex items-center gap-2 mb-3">
              <div className="w-8 h-8 bg-orange-600 rounded-lg flex items-center justify-center">
                <span className="text-xl">📈</span>
              </div>
              <h3 className="text-lg font-semibold">Needs Improvement</h3>
            </div>
            <ul className="space-y-2 text-sm">
              {data.insights.improve.map((x, i) => (
                <li key={i} className="flex items-start gap-2">
                  <span className="text-orange-500 mt-1">•</span>
                  <span className="text-gray-300">{x}</span>
                </li>
              ))}
            </ul>
          </div>

          {/* Recommendations */}
          <div className="bg-[#2c2c2c] rounded-lg p-6 border border-[#3a3a3a]">
            <div className="flex items-center gap-2 mb-3">
              <div className="w-8 h-8 bg-blue-600 rounded-lg flex items-center justify-center">
                <span className="text-xl">💡</span>
              </div>
              <h3 className="text-lg font-semibold">Recommendations</h3>
            </div>
            <ul className="space-y-2 text-sm">
              {data.insights.recommendations.map((x, i) => (
                <li key={i} className="flex items-start gap-2">
                  <span className="text-blue-500 mt-1">•</span>
                  <span className="text-gray-300">{x}</span>
                </li>
              ))}
            </ul>
          </div>
        </section>

        {/* Keywords */}
        <section className="bg-[#2c2c2c] rounded-lg p-6 mb-6 border border-[#3a3a3a]">
          <h3 className="text-lg font-semibold mb-4">Top Keywords</h3>
          <div className="flex flex-wrap gap-2">
            {data.keywords.slice(0, 24).map((k, i) => (
              <span
                key={i}
                className="text-xs bg-[#3a3a3a] text-gray-300 px-3 py-1.5 rounded-full border border-[#4a4a4a]"
              >
                {k.term} ({k.count})
              </span>
            ))}
          </div>
        </section>

        {/* Navigation */}
        <div className="flex gap-4">
          <Link
            href={`/biz/${id}/trends`}
            className="px-6 py-3 bg-blue-600 hover:bg-blue-700 rounded-lg font-medium transition-colors"
          >
            View Trends
          </Link>
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
