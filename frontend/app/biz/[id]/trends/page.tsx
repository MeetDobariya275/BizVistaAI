"use client";
import { useParams } from "next/navigation";
import { useQuery } from "@tanstack/react-query";
import { fetchTrends } from "@/lib/api";
import Link from "next/link";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from "recharts";

export default function TrendsPage() {
  const { id } = useParams<{ id: string }>();
  const { data, isLoading } = useQuery({
    queryKey: ["trends", id],
    queryFn: () => fetchTrends(id as string),
  });

  if (isLoading) return <div className="p-6">Loading…</div>;
  if (!data) return <div className="p-6">Not found</div>;

  // Aggregate data by month
  const monthlyMap = new Map<string, { month: string; total: number; count: number }>();
  
  data.forEach((d) => {
    const existing = monthlyMap.get(d.month) || { month: d.month, total: 0, count: 0 };
    existing.total += d.avg_sentiment * d.review_count;
    existing.count += d.review_count;
    monthlyMap.set(d.month, existing);
  });

  const chartData = Array.from(monthlyMap.values())
    .map((d) => ({
      month: d.month,
      sentiment: d.count > 0 ? d.total / d.count : 0,
    }))
    .sort((a, b) => a.month.localeCompare(b.month));

  return (
    <main className="min-h-screen bg-[#1a1a1a] text-white p-6">
      <div className="max-w-7xl mx-auto">
        {/* Breadcrumb */}
        <div className="mb-6 flex items-center gap-2 text-sm text-gray-400">
          <Link href="/" className="hover:text-white">
            Dashboard
          </Link>
          <span>/</span>
          <Link href={`/biz/${id}`} className="hover:text-white">
            Business
          </Link>
          <span>/</span>
          <span>Trends</span>
        </div>

        {/* Header */}
        <header className="mb-8">
          <h1 className="text-3xl font-bold">Monthly Trends</h1>
          <p className="text-gray-400 mt-2">
            Overall sentiment over time from customer reviews
          </p>
        </header>

        {/* Trend Chart */}
        <section className="bg-[#2c2c2c] rounded-lg p-6 mb-6 border border-[#3a3a3a]">
          <h2 className="text-xl font-semibold mb-4">Sentiment Over Time</h2>
          <div className="h-96">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={chartData}>
                <XAxis
                  dataKey="month"
                  tick={{ fill: "#999" }}
                  angle={-45}
                  textAnchor="end"
                  height={100}
                />
                <YAxis domain={[-1, 1]} tick={{ fill: "#999" }} />
                <Tooltip
                  contentStyle={{
                    backgroundColor: "#2c2c2c",
                    border: "1px solid #3a3a3a",
                    borderRadius: "8px",
                    color: "#ededed",
                  }}
                />
                <Legend
                  wrapperStyle={{ color: "#999" }}
                />
                <Line
                  type="monotone"
                  dataKey="sentiment"
                  name="Overall Sentiment"
                  stroke="#3b82f6"
                  strokeWidth={2}
                  dot={{ fill: "#3b82f6", r: 4 }}
                  activeDot={{ r: 6 }}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </section>

        {/* Stats */}
        <section className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
          <div className="bg-[#2c2c2c] rounded-lg p-4 border border-[#3a3a3a]">
            <div className="text-sm text-gray-400 mb-1">Total Reviews</div>
            <div className="text-2xl font-bold">{data.length}</div>
          </div>
          <div className="bg-[#2c2c2c] rounded-lg p-4 border border-[#3a3a3a]">
            <div className="text-sm text-gray-400 mb-1">Average Sentiment</div>
            <div className="text-2xl font-bold">
              {(
                chartData.reduce((sum, d) => sum + d.sentiment, 0) /
                chartData.length
              ).toFixed(2)}
            </div>
          </div>
          <div className="bg-[#2c2c2c] rounded-lg p-4 border border-[#3a3a3a]">
            <div className="text-sm text-gray-400 mb-1">Peak Month</div>
            <div className="text-2xl font-bold">
              {chartData.reduce(
                (max, d) => (d.sentiment > max.sentiment ? d : max),
                chartData[0]
              )?.month || "N/A"}
            </div>
          </div>
          <div className="bg-[#2c2c2c] rounded-lg p-4 border border-[#3a3a3a]">
            <div className="text-sm text-gray-400 mb-1">Latest Sentiment</div>
            <div className="text-2xl font-bold">
              {chartData[chartData.length - 1]?.sentiment.toFixed(2) || "N/A"}
            </div>
          </div>
        </section>

        {/* Navigation */}
        <div className="flex gap-4">
          <Link
            href={`/biz/${id}`}
            className="px-6 py-3 bg-blue-600 hover:bg-blue-700 rounded-lg font-medium transition-colors"
          >
            Back to Overview
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
