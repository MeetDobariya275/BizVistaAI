"use client";
import { useState, useEffect, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { searchBusinesses, queryKeywordAnalytics, fetchBusinesses, getBusinessDateRange } from "@/lib/api";
import { QueryResponse, SearchBusiness, DateRange } from "@/lib/schemas";
import {
  BarChart,
  Bar,
  LineChart,
  Line,
  PieChart,
  Pie,
  Cell,
  ScatterChart,
  Scatter,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";
import clsx from "clsx";
import Link from "next/link";

const COLORS = {
  green: "#22c55e",
  yellow: "#eab308",
  red: "#ef4444",
  blue: "#3b82f6",
  gray: "#6b7280",
};

export default function KeywordDashboard() {
  // Filter state
  const [selectedBusiness, setSelectedBusiness] = useState<string>("");
  const [businessSearchQuery, setBusinessSearchQuery] = useState("");
  const [startDate, setStartDate] = useState<string>("");
  const [endDate, setEndDate] = useState<string>("");
  const [keywords, setKeywords] = useState<string[]>([]);
  const [keywordInput, setKeywordInput] = useState("");

  // Get default business (highest review count)
  const { data: allBusinesses } = useQuery({
    queryKey: ["biz"],
    queryFn: fetchBusinesses,
  });

  useEffect(() => {
    if (allBusinesses && allBusinesses.length > 0 && !selectedBusiness) {
      const topBusiness = allBusinesses.reduce((prev, curr) =>
        curr.review_count > prev.review_count ? curr : prev
      );
      setSelectedBusiness(topBusiness.id);
    }
  }, [allBusinesses, selectedBusiness]);

  // Business search
  const { data: searchResults } = useQuery({
    queryKey: ["search-businesses", businessSearchQuery],
    queryFn: () => searchBusinesses(businessSearchQuery),
    enabled: businessSearchQuery.length > 0,
  });

  // Get available date range for selected business
  const { data: dateRange } = useQuery<DateRange>({
    queryKey: ["date-range", selectedBusiness],
    queryFn: () => getBusinessDateRange(selectedBusiness),
    enabled: selectedBusiness !== "",
  });

  // Initialize date range when date range data is loaded or business changes
  useEffect(() => {
    if (dateRange) {
      // Reset to full available range when business or date range changes
      setStartDate(dateRange.min_date);
      setEndDate(dateRange.max_date);
    }
  }, [dateRange]);

  // Query analytics
  const { data: queryData, isLoading, error } = useQuery<QueryResponse>({
    queryKey: ["keyword-query", selectedBusiness, startDate, endDate, keywords.sort().join(",")],
    queryFn: () => queryKeywordAnalytics(selectedBusiness, startDate, endDate, keywords),
    enabled: selectedBusiness !== "" && startDate !== "" && endDate !== "" && keywords.length > 0 && keywords.length <= 10,
  });

  // Add keyword
  const handleAddKeyword = () => {
    const trimmed = keywordInput.trim().toLowerCase();
    if (trimmed && !keywords.includes(trimmed) && keywords.length < 10) {
      setKeywords([...keywords, trimmed]);
      setKeywordInput("");
    }
  };

  // Remove keyword
  const handleRemoveKeyword = (kw: string) => {
    setKeywords(keywords.filter((k) => k !== kw));
  };

  // Format time ago
  const formatTimeAgo = (isoString: string) => {
    const date = new Date(isoString);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffMins = Math.floor(diffMs / 60000);
    if (diffMins < 1) return "just now";
    if (diffMins < 60) return `${diffMins}m ago`;
    const diffHours = Math.floor(diffMins / 60);
    if (diffHours < 24) return `${diffHours}h ago`;
    const diffDays = Math.floor(diffHours / 24);
    return `${diffDays}d ago`;
  };

  // Chart data preparation
  const sentimentMatrixData = queryData?.by_keyword?.map((kw) => {
    const sentiment = Math.round((kw.avg_sentiment + 1) * 50); // Convert -1..1 to 0..100
    return {
      keyword: kw.term,
      sentiment: sentiment - 50, // Center at 0 for diverging (-50 to +50)
      sentimentRaw: sentiment, // Keep original for tooltip
      hits: kw.hits,
    };
  }) || [];

  // Momentum trend - show overall sentiment over time (one line)
  const momentumTrendData = queryData?.time_series?.map((ts) => ({
    bucket: ts.bucket,
    sentiment: Math.round((ts.avg_sentiment + 1) * 50), // Convert -1..1 to 0..100
    hits: ts.hits,
  })) || [];

  const shareOfVoiceData = queryData?.by_keyword?.map((kw) => ({
    name: kw.term,
    value: queryData.share_of_voice?.[kw.term] || 0,
    sentiment: Math.round((kw.avg_sentiment + 1) * 50),
  })) || [];

  const impactVsSentimentData = queryData?.by_keyword?.map((kw) => {
    const sentiment = Math.round((kw.avg_sentiment + 1) * 50);
    const delta = queryData.kpis?.deltas.sentiment || 0;
    return {
      keyword: kw.term,
      sentiment,
      mentions: kw.hits,
      delta: Math.abs(delta),
      polarity: sentiment >= 60 ? "positive" : sentiment >= 40 ? "neutral" : "negative",
    };
  }) || [];

  const selectedBusinessName = allBusinesses?.find((b) => b.id === selectedBusiness)?.name || "Select Business";

  return (
    <main className="min-h-screen bg-[#1a1a1a] text-white p-6">
      <div className="max-w-7xl mx-auto space-y-6">
        {/* Header */}
        <header>
          <h1 className="text-3xl font-bold">BizVista AI - Keyword Analytics</h1>
          <p className="text-gray-400 mt-1">Enter keywords to analyze customer feedback for a restaurant</p>
        </header>

        {/* Filter Bar */}
        <section className="bg-[#2c2c2c] rounded-lg p-6 border border-[#3a3a3a] space-y-4">
          <div className="grid md:grid-cols-2 gap-4">
            {/* Business Search */}
            <div className="relative">
              <label className="block text-sm font-medium mb-2">Business</label>
              <input
                type="text"
                value={businessSearchQuery}
                onChange={(e) => setBusinessSearchQuery(e.target.value)}
                onFocus={() => setBusinessSearchQuery(selectedBusinessName)}
                placeholder="Search business..."
                className="w-full px-4 py-2 bg-[#1a1a1a] border border-[#3a3a3a] rounded-lg text-white focus:outline-none focus:border-blue-500"
              />
              {searchResults && searchResults.length > 0 && (
                <div className="absolute z-10 w-full mt-1 bg-[#2c2c2c] border border-[#3a3a3a] rounded-lg max-h-48 overflow-y-auto">
                  {searchResults.map((biz: SearchBusiness) => (
                    <button
                      key={biz.id}
                      onClick={() => {
                        setSelectedBusiness(biz.id);
                        setBusinessSearchQuery(biz.name);
                        // Reset dates - will be set when date range loads
                        setStartDate("");
                        setEndDate("");
                      }}
                      className="w-full text-left px-4 py-2 hover:bg-[#3a3a3a] transition-colors"
                    >
                      <div className="font-medium">{biz.name}</div>
                      <div className="text-sm text-gray-400">{biz.city} • {biz.review_count} reviews</div>
                    </button>
                  ))}
                </div>
              )}
            </div>

            {/* Date Range Selector */}
            {dateRange ? (
              <div className="col-span-2">
                <label className="block text-sm font-medium mb-2">
                  Select Date Range
                  <span className="text-xs text-gray-400 ml-2">
                    ({dateRange.total_reviews.toLocaleString()} reviews available)
                  </span>
                </label>
                <div className="grid grid-cols-2 gap-2">
                  <div>
                    <label className="block text-xs text-gray-400 mb-1">Start Date</label>
                    <input
                      type="date"
                      value={startDate}
                      min={dateRange.min_date}
                      max={dateRange.max_date}
                      onChange={(e) => {
                        const newStart = e.target.value;
                        setStartDate(newStart);
                        if (newStart > endDate) {
                          setEndDate(newStart);
                        }
                      }}
                      className="w-full px-4 py-2 bg-[#1a1a1a] border border-[#3a3a3a] rounded-lg text-white focus:outline-none focus:border-blue-500"
                    />
                    <div className="text-xs text-gray-500 mt-1">
                      Available from: {new Date(dateRange.min_date).toLocaleDateString()}
                    </div>
                  </div>
                  <div>
                    <label className="block text-xs text-gray-400 mb-1">End Date</label>
                    <input
                      type="date"
                      value={endDate}
                      min={startDate || dateRange.min_date}
                      max={dateRange.max_date}
                      onChange={(e) => {
                        const newEnd = e.target.value;
                        setEndDate(newEnd);
                        if (newEnd < startDate) {
                          setStartDate(newEnd);
                        }
                      }}
                      className="w-full px-4 py-2 bg-[#1a1a1a] border border-[#3a3a3a] rounded-lg text-white focus:outline-none focus:border-blue-500"
                    />
                    <div className="text-xs text-gray-500 mt-1">
                      Available until: {new Date(dateRange.max_date).toLocaleDateString()}
                    </div>
                  </div>
                </div>
                {startDate && endDate && (
                  <div className="mt-2 text-xs text-gray-400">
                    Selected range: {new Date(startDate).toLocaleDateString()} to {new Date(endDate).toLocaleDateString()}
                  </div>
                )}
              </div>
            ) : selectedBusiness ? (
              <div className="col-span-2 text-sm text-gray-400">
                Loading available dates...
              </div>
            ) : (
              <div className="col-span-2 text-sm text-gray-400">
                Select a business to see available date range
              </div>
            )}
          </div>

          {/* Keywords - Main Input Area */}
          <div className="border-t border-[#3a3a3a] pt-4">
            <label className="block text-lg font-semibold mb-3">
              Enter Keywords to Analyze
              <span className="text-sm font-normal text-gray-400 ml-2">(max 10 keywords)</span>
            </label>
            <div className="flex gap-2 flex-wrap items-center mb-3">
              {keywords.map((kw) => (
                <span
                  key={kw}
                  className="inline-flex items-center gap-2 px-4 py-2 bg-blue-600 rounded-full text-sm font-medium"
                >
                  {kw}
                  <button
                    onClick={() => handleRemoveKeyword(kw)}
                    className="hover:text-red-300 transition-colors text-lg leading-none"
                    aria-label={`Remove ${kw}`}
                  >
                    ×
                  </button>
                </span>
              ))}
            </div>
            {keywords.length < 10 && (
              <div className="flex gap-2">
                <input
                  type="text"
                  value={keywordInput}
                  onChange={(e) => setKeywordInput(e.target.value)}
                  onKeyPress={(e) => e.key === "Enter" && handleAddKeyword()}
                  placeholder="Type a keyword and press Enter (e.g., service, food, price, wait time)"
                  className="flex-1 px-4 py-3 bg-[#1a1a1a] border-2 border-[#3a3a3a] rounded-lg text-white focus:outline-none focus:border-blue-500 text-base"
                />
                <button
                  onClick={handleAddKeyword}
                  className="px-6 py-3 bg-blue-600 hover:bg-blue-700 rounded-lg font-medium transition-colors"
                >
                  Add Keyword
                </button>
              </div>
            )}
            {keywords.length === 0 && (
              <p className="text-sm text-gray-400 mt-2">
                💡 Start by adding keywords like: "service", "food quality", "wait time", "price", "ambiance"
              </p>
            )}
          </div>
        </section>

        {/* Loading State */}
        {isLoading && (
          <div className="space-y-6">
            <div className="grid md:grid-cols-3 gap-4">
              {[1, 2, 3].map((i) => (
                <div key={i} className="bg-[#2c2c2c] rounded-lg p-6 border border-[#3a3a3a] animate-pulse">
                  <div className="h-4 bg-[#3a3a3a] rounded w-24 mb-2"></div>
                  <div className="h-8 bg-[#3a3a3a] rounded w-32 mb-2"></div>
                  <div className="h-4 bg-[#3a3a3a] rounded w-20"></div>
                </div>
              ))}
            </div>
            <div className="bg-[#2c2c2c] rounded-lg p-6 border border-[#3a3a3a] animate-pulse">
              <div className="h-6 bg-[#3a3a3a] rounded w-48 mb-4"></div>
              <div className="h-64 bg-[#3a3a3a] rounded"></div>
            </div>
          </div>
        )}

        {/* Error State */}
        {error && (
          <div className="bg-red-900/20 border border-red-500 rounded-lg p-4 text-red-400">
            Error: {error instanceof Error ? error.message : "Failed to load analytics"}
          </div>
        )}

        {/* Insufficient Data */}
        {queryData?.insufficient_data && (
          <div className="bg-yellow-900/20 border border-yellow-500 rounded-lg p-6">
            <h3 className="text-lg font-semibold mb-2">Insufficient Data</h3>
            <p className="text-gray-300">{queryData.message}</p>
            {queryData.matched_reviews !== undefined && (
              <p className="text-sm text-gray-400 mt-2">
                Matched: {queryData.matched_reviews} reviews (need ≥25 for AI summary)
              </p>
            )}
          </div>
        )}

        {/* Main Content */}
        {queryData && !queryData.insufficient_data && queryData.kpis && (
          <>
            {/* Row 1: KPI Cards */}
            <div className="grid md:grid-cols-3 gap-4">
              <div className="bg-[#2c2c2c] rounded-lg p-6 border border-[#3a3a3a]">
                <div className="text-sm text-gray-400 mb-1">Matched Reviews</div>
                <div className="text-3xl font-bold mb-2">{queryData.kpis.matched_reviews.toLocaleString()}</div>
                <div className={clsx(
                  "text-sm",
                  queryData.kpis.deltas.reviews >= 0 ? "text-green-400" : "text-red-400"
                )}>
                  {queryData.kpis.deltas.reviews >= 0 ? "+" : ""}{queryData.kpis.deltas.reviews} vs prior period
                </div>
              </div>

              <div className="bg-[#2c2c2c] rounded-lg p-6 border border-[#3a3a3a]">
                <div className="text-sm text-gray-400 mb-1">Avg Sentiment</div>
                <div className="text-3xl font-bold mb-2">{queryData.kpis.sentiment_score}%</div>
                <div className={clsx(
                  "text-sm",
                  queryData.kpis.deltas.sentiment >= 0 ? "text-green-400" : "text-red-400"
                )}>
                  {queryData.kpis.deltas.sentiment >= 0 ? "+" : ""}{queryData.kpis.deltas.sentiment}% vs prior period
                </div>
              </div>

              <div className="bg-[#2c2c2c] rounded-lg p-6 border border-[#3a3a3a]">
                <div className="text-sm text-gray-400 mb-1">Avg Stars</div>
                <div className="text-3xl font-bold mb-2">{queryData.kpis.avg_stars.toFixed(1)}</div>
                <div className={clsx(
                  "text-sm",
                  queryData.kpis.deltas.stars >= 0 ? "text-green-400" : "text-red-400"
                )}>
                  {queryData.kpis.deltas.stars >= 0 ? "+" : ""}{queryData.kpis.deltas.stars} vs prior period
                </div>
              </div>
            </div>

            {/* Row 2: Keyword Sentiment Matrix */}
            <div className="bg-[#2c2c2c] rounded-lg p-6 border border-[#3a3a3a]">
              <h2 className="text-lg font-semibold mb-4">Keyword Sentiment Matrix</h2>
              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={sentimentMatrixData} layout="vertical">
                  <XAxis type="number" domain={[-50, 50]} />
                  <YAxis dataKey="keyword" type="category" width={120} />
                  <Tooltip
                    formatter={(value: number, name: string, props: any) => [
                      `${props.payload.sentimentRaw}%`,
                      "Sentiment",
                    ]}
                    contentStyle={{ backgroundColor: "#2c2c2c", border: "1px solid #3a3a3a" }}
                  />
                  <Bar dataKey="sentiment">
                    {sentimentMatrixData.map((entry, index) => (
                      <Cell
                        key={`cell-${index}`}
                        fill={
                          entry.sentimentRaw >= 60
                            ? COLORS.green
                            : entry.sentimentRaw >= 40
                            ? COLORS.yellow
                            : COLORS.red
                        }
                      />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>

            {/* Row 3: Momentum Trend */}
            <div className="bg-[#2c2c2c] rounded-lg p-6 border border-[#3a3a3a]">
              <h2 className="text-lg font-semibold mb-4">Sentiment Trend Over Time</h2>
              <ResponsiveContainer width="100%" height={300}>
                <LineChart data={momentumTrendData}>
                  <XAxis dataKey="bucket" />
                  <YAxis domain={[0, 100]} label={{ value: "Sentiment %", angle: -90, position: "insideLeft" }} />
                  <Tooltip
                    formatter={(value: number, name: string, props: any) => [
                      `${value}%`,
                      `Sentiment (${props.payload.hits} reviews)`,
                    ]}
                    contentStyle={{ backgroundColor: "#2c2c2c", border: "1px solid #3a3a3a" }}
                  />
                  <Line
                    type="monotone"
                    dataKey="sentiment"
                    stroke={COLORS.blue}
                    strokeWidth={3}
                    dot={{ fill: COLORS.blue, r: 4 }}
                    activeDot={{ r: 6 }}
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>

            {/* Row 4: Share of Voice */}
            <div className="bg-[#2c2c2c] rounded-lg p-6 border border-[#3a3a3a]">
              <h2 className="text-lg font-semibold mb-4">Share of Voice</h2>
              <ResponsiveContainer width="100%" height={300}>
                <PieChart>
                  <Pie
                    data={shareOfVoiceData}
                    cx="50%"
                    cy="50%"
                    labelLine={false}
                    label={(entry: any) => `${entry.name}: ${entry.value.toFixed(1)}%`}
                    outerRadius={100}
                    fill="#8884d8"
                    dataKey="value"
                  >
                    {shareOfVoiceData.map((entry, index) => (
                      <Cell
                        key={`cell-${index}`}
                        fill={
                          entry.sentiment >= 60
                            ? COLORS.green
                            : entry.sentiment >= 40
                            ? COLORS.yellow
                            : COLORS.red
                        }
                      />
                    ))}
                  </Pie>
                  <Tooltip
                    formatter={(value: unknown) => [`${value}%`, "Share"]}
                    contentStyle={{ backgroundColor: "#2c2c2c", border: "1px solid #3a3a3a" }}
                  />
                </PieChart>
              </ResponsiveContainer>
            </div>

            {/* Row 5: Impact vs Sentiment */}
            <div className="bg-[#2c2c2c] rounded-lg p-6 border border-[#3a3a3a]">
              <h2 className="text-lg font-semibold mb-4">Impact vs Sentiment</h2>
              <ResponsiveContainer width="100%" height={300}>
                <ScatterChart>
                  <XAxis
                    type="number"
                    dataKey="sentiment"
                    name="Sentiment"
                    domain={[0, 100]}
                    label={{ value: "Sentiment Score", position: "insideBottom", offset: -5 }}
                  />
                  <YAxis
                    type="number"
                    dataKey="mentions"
                    name="Mentions"
                    label={{ value: "Mentions", angle: -90, position: "insideLeft" }}
                  />
                  <Tooltip
                    cursor={{ strokeDasharray: "3 3" }}
                    contentStyle={{ backgroundColor: "#2c2c2c", border: "1px solid #3a3a3a" }}
                  />
                  <Scatter
                    name="Keywords"
                    data={impactVsSentimentData}
                    fill="#8884d8"
                  >
                    {impactVsSentimentData.map((entry, index) => (
                      <Cell
                        key={`cell-${index}`}
                        fill={
                          entry.polarity === "positive"
                            ? COLORS.green
                            : entry.polarity === "neutral"
                            ? COLORS.yellow
                            : COLORS.red
                        }
                      />
                    ))}
                  </Scatter>
                </ScatterChart>
              </ResponsiveContainer>
            </div>

            {/* Row 6: AI Summary */}
            <div className="bg-[#2c2c2c] rounded-lg p-6 border border-[#3a3a3a]">
              <div className="flex items-center justify-between mb-4">
                <h2 className="text-lg font-semibold">AI Summary</h2>
                <div className="flex items-center gap-2">
                  <span
                    className={clsx(
                      "text-xs px-2 py-1 rounded",
                      queryData.summary_source === "llm"
                        ? "bg-green-600 text-white"
                        : "bg-yellow-600 text-white"
                    )}
                  >
                    {queryData.summary_source === "llm" ? "Generated by AI" : "Fallback Summary"}
                  </span>
                  <span className="text-xs text-gray-400">
                    {queryData.generated_at ? formatTimeAgo(queryData.generated_at) : ""}
                  </span>
                </div>
              </div>

              <div className="grid md:grid-cols-3 gap-6">
                <div>
                  <h3 className="font-medium mb-2 text-green-400">What Customers Love</h3>
                  <ul className="space-y-1 text-sm text-gray-300">
                    {queryData.summary?.love.map((item, i) => (
                      <li key={i} className="flex items-start gap-2">
                        <span className="text-green-500 mt-1">•</span>
                        <span>{item}</span>
                      </li>
                    ))}
                  </ul>
                </div>

                <div>
                  <h3 className="font-medium mb-2 text-red-400">Needs Improvement</h3>
                  <ul className="space-y-1 text-sm text-gray-300">
                    {queryData.summary?.improve.map((item, i) => (
                      <li key={i} className="flex items-start gap-2">
                        <span className="text-red-500 mt-1">•</span>
                        <span>{item}</span>
                      </li>
                    ))}
                  </ul>
                </div>

                <div>
                  <h3 className="font-medium mb-2 text-blue-400">Recommendations</h3>
                  <ul className="space-y-1 text-sm text-gray-300">
                    {queryData.summary?.recommendations.map((item, i) => (
                      <li key={i} className="flex items-start gap-2">
                        <span className="text-blue-500 mt-1">•</span>
                        <span>{item}</span>
                      </li>
                    ))}
                  </ul>
                </div>
              </div>
            </div>

            {/* Row 7: Quotes Carousel */}
            <div className="bg-[#2c2c2c] rounded-lg p-6 border border-[#3a3a3a]">
              <h2 className="text-lg font-semibold mb-4">Representative Quotes</h2>
              <div className="space-y-6">
                {queryData.by_keyword?.map((kw) => {
                  const quotes = queryData.quotes_by_keyword?.[kw.term];
                  if (!quotes || (quotes.positive.length === 0 && quotes.negative.length === 0)) {
                    return null;
                  }
                  return (
                    <div key={kw.term} className="border-t border-[#3a3a3a] pt-4">
                      <h3 className="font-medium mb-3">{kw.term}</h3>
                      <div className="grid md:grid-cols-2 gap-4">
                        {quotes.positive.length > 0 && (
                          <div>
                            <div className="text-sm text-green-400 mb-2">Positive</div>
                            <div className="space-y-2">
                              {quotes.positive.map((quote, i) => (
                                <div
                                  key={i}
                                  className="text-sm text-gray-300 bg-[#1a1a1a] p-3 rounded border border-[#3a3a3a]"
                                >
                                  "{quote}"
                                </div>
                              ))}
                            </div>
                          </div>
                        )}
                        {quotes.negative.length > 0 && (
                          <div>
                            <div className="text-sm text-red-400 mb-2">Negative</div>
                            <div className="space-y-2">
                              {quotes.negative.map((quote, i) => (
                                <div
                                  key={i}
                                  className="text-sm text-gray-300 bg-[#1a1a1a] p-3 rounded border border-[#3a3a3a]"
                                >
                                  "{quote}"
                                </div>
                              ))}
                            </div>
                          </div>
                        )}
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          </>
        )}
      </div>
    </main>
  );
}

