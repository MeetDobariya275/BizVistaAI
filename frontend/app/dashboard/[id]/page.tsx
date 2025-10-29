"use client";
import { useParams, useRouter } from "next/navigation";
import { useState, useEffect, useMemo } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { fetchBusinesses, fetchKPIs, fetchQuotes, fetchOverview, fetchTrends } from "@/lib/api";
import axios from "axios";
import Link from "next/link";
import {
  BarChart,
  Bar,
  RadialBarChart,
  RadialBar,
  ResponsiveContainer,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  Cell,
  LineChart,
  Line,
} from "recharts";

type Task = {
  id: string;
  title: string;
  theme: string;
  impact: "High" | "Med" | "Low";
  effort: "High" | "Med" | "Low";
  status: "Backlog" | "In Progress" | "In Review" | "Done";
  progress: number;
  created_at?: string;
  due_at?: string;
};

// Theme keywords mapping
const THEME_KEYWORDS: Record<string, string[]> = {
  Service: ["server", "staff", "rude", "polite", "attentive", "friendly", "service", "waiter"],
  "Speed/Wait Time": ["wait", "slow", "queue", "delay", "quick", "fast", "timely"],
  Ambiance: ["decor", "music", "noise", "lighting", "vibe", "atmosphere", "ambiance"],
  "Food Quality": ["taste", "flavor", "fresh", "stale", "cold", "delicious", "quality"],
  Cleanliness: ["clean", "hygiene", "dirty", "sticky", "maintenance"],
  "Portion Size": ["portion", "small", "big", "quantity", "size"],
  "Price/Value": ["price", "expensive", "overpriced", "value", "affordable", "worth"],
  "Staff Behavior": ["behavior", "attitude", "manager", "host"],
};

// Infer theme from title text
function inferTheme(title: string): string {
  const lowerTitle = title.toLowerCase();
  for (const [theme, keywords] of Object.entries(THEME_KEYWORDS)) {
    if (keywords.some((kw) => lowerTitle.includes(kw))) {
      return theme;
    }
  }
  return "Service"; // fallback
}

// Calculate impact based on theme score and delta
function calculateImpact(themeScore?: number, delta?: number, source?: string): "High" | "Med" | "Low" {
  if (source === "improve") return "High";
  if (themeScore !== undefined && themeScore < 0.6) return "High";
  if (delta !== undefined && delta < -0.04) return "High";
  if (delta !== undefined && delta >= -0.04 && delta < 0) return "Med";
  if (themeScore !== undefined && themeScore >= 0.6 && themeScore < 0.7) return "Med";
  return "Low";
}

// Calculate effort based on title wording
function calculateEffort(title: string): "High" | "Med" | "Low" {
  const lowerTitle = title.toLowerCase();
  if (lowerTitle.includes("hire") || lowerTitle.includes("train") || lowerTitle.includes("scheduling") || lowerTitle.includes("hire")) {
    return "High";
  }
  if (lowerTitle.includes("label") || lowerTitle.includes("sign") || lowerTitle.includes("menu note") || lowerTitle.includes("seating zone")) {
    return "Low";
  }
  return "Med";
}

// Format time ago
function formatTimeAgo(dateString: string | null): string {
  if (!dateString) return "Unknown";
  const date = new Date(dateString);
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffMins = Math.floor(diffMs / 60000);
  const diffHours = Math.floor(diffMins / 60);
  const diffDays = Math.floor(diffHours / 24);

  if (diffMins < 1) return "Just now";
  if (diffMins < 60) return `${diffMins}m ago`;
  if (diffHours < 24) return `${diffHours}h ago`;
  return `${diffDays}d ago`;
}

// Truncate text with ellipsis
function truncateText(text: string, maxLength: number = 140): string {
  if (text.length <= maxLength) return text;
  const truncated = text.substring(0, maxLength);
  const lastSpace = truncated.lastIndexOf(" ");
  return lastSpace > 0 ? truncated.substring(0, lastSpace) + "..." : truncated + "...";
}

// Remove duplicate bullets by fuzzy matching
function deduplicateBullets(bullets: string[]): string[] {
  const deduped: string[] = [];
  bullets.forEach((bullet) => {
    const isDuplicate = deduped.some((existing) => {
      const similarity = (existing.toLowerCase().match(new RegExp(bullet.substring(0, 10), "gi")) || []).length;
      return similarity > 0;
    });
    if (!isDuplicate) {
      deduped.push(bullet);
    }
  });
  return deduped;
}

export default function DashboardPage() {
  const { id } = useParams<{ id: string }>();
  const router = useRouter();
  const [period, setPeriod] = useState("30d");
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [selectedTask, setSelectedTask] = useState<Task | null>(null);

  const { data: businesses } = useQuery({
    queryKey: ["biz"],
    queryFn: fetchBusinesses,
  });

  const { data: kpis } = useQuery({
    queryKey: ["kpis", id, period],
    queryFn: () => fetchKPIs(id!, period),
    enabled: !!id,
  });

  const { data: quotes } = useQuery({
    queryKey: ["quotes", id, period],
    queryFn: () => fetchQuotes(id!, period),
    enabled: !!id,
  });

  const { data: overview } = useQuery({
    queryKey: ["overview", id],
    queryFn: () => fetchOverview(id!),
    enabled: !!id,
  });

  const { data: trends } = useQuery({
    queryKey: ["trends", id],
    queryFn: () => fetchTrends(id!),
    enabled: !!id,
  });

  const queryClient = useQueryClient();
  const currentBusiness = businesses?.find((b) => b.id === id);
  const [tasks, setTasks] = useState<Task[]>([]);
  const [refreshing, setRefreshing] = useState(false);
  const [lastRefreshTime, setLastRefreshTime] = useState<Date | null>(null);

  // Generate tasks from insights
  useEffect(() => {
    if (!overview?.insights || !id) return;

    const storageKey = `tasks:${id}:${period}`;
    const saved = localStorage.getItem(storageKey);
    const savedTasks: Task[] = saved ? JSON.parse(saved) : [];

    const generated: Task[] = [];
    let taskId = 1;

    // Add recommendations
    overview.insights.recommendations.slice(0, 3).forEach((rec) => {
      generated.push({
        id: `gen-${taskId++}`,
        title: rec,
        theme: inferTheme(rec),
        impact: calculateImpact(),
        effort: calculateEffort(rec),
        status: "Backlog" as const,
        progress: 0,
        created_at: new Date().toISOString(),
      });
    });

    // Add top improvements
    overview.insights.improve.slice(0, 3).forEach((improve) => {
      // Convert to imperative
      const imperative = improve.startsWith("Improve") ? improve : `Improve ${improve.toLowerCase()}`;
      generated.push({
        id: `gen-${taskId++}`,
        title: imperative,
        theme: inferTheme(imperative),
        impact: calculateImpact(undefined, undefined, "improve"),
        effort: calculateEffort(imperative),
        status: "Backlog" as const,
        progress: 0,
        created_at: new Date().toISOString(),
      });
    });

    // Merge with saved tasks, preserving user edits
    const merged = generated.map((genTask) => {
      const savedTask = savedTasks.find((st) => st.title === genTask.title);
      return savedTask ? { ...genTask, ...savedTask, id: savedTask.id } : genTask;
    });

    setTasks(merged);
  }, [overview, id, period]);

  // Save tasks to localStorage
  useEffect(() => {
    if (!id || tasks.length === 0) return;
    const storageKey = `tasks:${id}:${period}`;
    localStorage.setItem(storageKey, JSON.stringify(tasks));
  }, [tasks, id, period]);

  // Prepare chart data from trends
  const chartData = trends
    ? trends
        .map((t) => ({
          month: t.month,
          sentiment: Math.round((t.avg_sentiment + 1) * 50), // Convert -1 to 1 range to 0-100
        }))
        .slice(-12) // Last 12 months
    : [];

  // Color function for bars based on sentiment
  const getBarColor = (sentiment: number) => {
    if (sentiment >= 60) return "#10b981"; // green
    if (sentiment >= 40) return "#f59e0b"; // yellow/amber
    return "#ef4444"; // red
  };

  // Get theme trend data for drawer
  const getThemeTrend = (theme: string) => {
    if (!trends) return [];
    // Use all trends (no theme filtering since the API doesn't provide it)
    return trends
      .map((t) => ({
        month: t.month,
        sentiment: Math.round((t.avg_sentiment + 1) * 50),
      }))
      .slice(-6);
  };

  // Get quotes for theme
  const getThemeQuotes = (theme: string) => {
    if (!quotes?.quotes_by_theme) return { positive: [], negative: [] };
    
    // Map theme names (e.g., "Service" -> "service")
    const themeKey = theme.toLowerCase().replace(/[\s\/]/g, "_");
    const themeData = quotes.quotes_by_theme[themeKey];
    
    if (!themeData) return { positive: [], negative: [] };
    
    return {
      positive: themeData.positive.slice(0, 1).map((text: string) => ({ text, sentiment: "positive" })),
      negative: themeData.negative.slice(0, 1).map((text: string) => ({ text, sentiment: "negative" })),
    };
  };

  const cycleStatus = (taskId: string) => {
    setTasks((prev) =>
      prev.map((t) => {
        if (t.id === taskId) {
          const statuses: Task["status"][] = ["Backlog", "In Progress", "In Review", "Done"];
          const currentIdx = statuses.indexOf(t.status);
          return { ...t, status: statuses[(currentIdx + 1) % 4] };
        }
        return t;
      })
    );
  };

  const updateProgress = (taskId: string, delta: number) => {
    setTasks((prev) =>
      prev.map((t) => {
        if (t.id === taskId) {
          return { ...t, progress: Math.max(0, Math.min(100, t.progress + delta)) };
        }
        return t;
      })
    );
  };

  const handleRefresh = async () => {
    if (refreshing) return;
    setRefreshing(true);
    
    try {
      // Call refresh API
      const response = await axios.post(
        `http://localhost:4174/api/businesses/${id}/refresh`,
        {},
        { params: { period } }
      );
      
      console.log("Refresh result:", response.data);
      
      if (response.data.success) {
        // Invalidate all queries to refetch
        await Promise.all([
          queryClient.invalidateQueries({ queryKey: ["kpis", id, period] }),
          queryClient.invalidateQueries({ queryKey: ["overview", id] }),
          queryClient.invalidateQueries({ queryKey: ["trends", id] }),
          queryClient.invalidateQueries({ queryKey: ["quotes", id, period] }),
        ]);
        
        setLastRefreshTime(new Date());
        
        // Show success toast (simple alert for now)
        alert(`Refreshed ${response.data.processed_reviews} reviews successfully!`);
      }
    } catch (error: any) {
      console.error("Failed to refresh:", error);
      alert("Couldn't refresh; using cached data.");
    } finally {
      setRefreshing(false);
    }
  };

  const openDrawer = (task: Task) => {
    setSelectedTask(task);
    setDrawerOpen(true);
  };

  // Generate trend sentence from KPIs and themes
  const getTrendSentence = () => {
    if (!kpis || !overview?.themes) return "";
    
    const sentimentDelta = kpis.deltas.sentiment || 0;
    if (sentimentDelta === 0) {
      return `Sentiment score is ${kpis.sentiment_score.toFixed(0)}/100, stable vs last period.`;
    }

    // Find top theme by delta
    const topTheme = overview.themes.reduce((prev, curr) => 
      (curr.delta || 0) > (prev.delta || 0) ? curr : prev
    );

    if (sentimentDelta > 0) {
      return `Overall sentiment improved ${Math.abs(sentimentDelta).toFixed(1)}% vs last period, led by ${topTheme.theme || "service improvements"}.`;
    } else {
      return `Overall sentiment declined ${Math.abs(sentimentDelta).toFixed(1)}% vs last period, driven by ${topTheme.theme || "service issues"}.`;
    }
  };

  return (
    <main className="min-h-screen bg-[#1a1a1a] text-white">
      <div className="flex">
        {/* Sidebar */}
        <aside className="w-64 min-h-screen bg-[#2c2c2c] border-r border-[#3a3a3a] p-6">
          <div className="mb-8">
            <div className="text-2xl font-bold mb-1">BizVista</div>
            <div className="text-xs text-gray-400">AI Analytics</div>
          </div>

          <nav className="space-y-2">
            <Link href="/" className="flex items-center gap-2 px-3 py-2 rounded-lg hover:bg-[#3a3a3a] text-gray-400">
              <span className="w-5">📊</span>
              <span>Dashboard</span>
            </Link>
            <Link href="/compare" className="flex items-center gap-2 px-3 py-2 rounded-lg bg-[#3a3a3a] text-white">
              <span className="w-5">⚖️</span>
              <span>Compare</span>
            </Link>
          </nav>

          <div className="mt-12">
            <div className="text-xs text-gray-500 uppercase mb-2">Quick Actions</div>
            <div className="space-y-1">
              <button className="w-full text-left px-3 py-2 rounded-lg text-sm text-gray-400 hover:bg-[#3a3a3a]">
                Insights
              </button>
              <button className="w-full text-left px-3 py-2 rounded-lg text-sm text-gray-400 hover:bg-[#3a3a3a]">
                Theme Analysis
              </button>
            </div>
          </div>
        </aside>

        {/* Main Content */}
        <div className="flex-1">
          {/* Top Bar */}
          <header className="bg-[#2c2c2c] border-b border-[#3a3a3a] px-6 py-4">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-4">
                <div className="flex items-center gap-2 text-sm text-gray-400">
                  <Link href="/dashboard" className="hover:text-white">Dashboard</Link>
                  <span>/</span>
                  <span className="text-white">{currentBusiness?.name || "Loading..."}</span>
                </div>
              </div>

              <div className="flex items-center gap-4">
                {/* Restaurant Selector */}
                <select
                  className="bg-[#3a3a3a] border border-[#4a4a4a] rounded-lg px-3 py-2 text-sm"
                  value={id}
                  onChange={(e) => router.push(`/dashboard/${e.target.value}`)}
                >
                  {businesses?.map((b) => (
                    <option key={b.id} value={b.id}>
                      {b.name}
                    </option>
                  ))}
                </select>

                {/* Period Selector */}
                <select
                  className="bg-[#3a3a3a] border border-[#4a4a4a] rounded-lg px-3 py-2 text-sm"
                  value={period}
                  onChange={(e) => setPeriod(e.target.value)}
                >
                  <option value="30d">Last 30 Days</option>
                  <option value="90d">Last 90 Days</option>
                  <option value="ytd">Year to Date</option>
                </select>

                {/* Refresh */}
                <button className="p-2 bg-[#3a3a3a] hover:bg-[#4a4a4a] rounded-lg transition-colors">
                  <span className="text-xl">⟳</span>
                </button>

                <div className="text-sm text-gray-400">Updated 2m ago</div>
              </div>
            </div>
          </header>

          {/* Content */}
          <div className="p-6 space-y-6">
            {/* Row 1: KPI Cards */}
            <div className="grid grid-cols-3 gap-4">
              <div className="bg-[#2c2c2c] border border-[#3a3a3a] rounded-lg p-6">
                <div className="text-sm text-gray-400 mb-2">Total Reviews</div>
                <div className="text-3xl font-bold mb-1">
                  {kpis ? kpis.total_reviews.toLocaleString() : "---"}
                </div>
                <div className={`text-sm ${kpis?.deltas.reviews && kpis.deltas.reviews > 0 ? 'text-green-500' : 'text-red-500'}`}>
                  {kpis?.deltas.reviews ? 
                    `${kpis.deltas.reviews > 0 ? '+' : ''}${kpis.deltas.reviews.toFixed(0)} reviews` : 
                    'No data'}
                </div>
              </div>

              <div className="bg-[#2c2c2c] border border-[#3a3a3a] rounded-lg p-6">
                <div className="text-sm text-gray-400 mb-2">Overall Sentiment</div>
                <div className="text-3xl font-bold mb-1">
                  {kpis ? kpis.sentiment_score.toFixed(0) : "---"}
                </div>
                <div className={`text-sm ${kpis?.deltas.sentiment && kpis.deltas.sentiment > 0 ? 'text-green-500' : 'text-red-500'}`}>
                  {kpis?.deltas.sentiment ? 
                    `${kpis.deltas.sentiment > 0 ? '+' : ''}${kpis.deltas.sentiment.toFixed(1)} points` : 
                    'No data'}
                </div>
              </div>

              <div className="bg-[#2c2c2c] border border-[#3a3a3a] rounded-lg p-6">
                <div className="text-sm text-gray-400 mb-2">Avg Stars</div>
                <div className="text-3xl font-bold mb-1">
                  {kpis ? `${kpis.avg_stars.toFixed(1)}★` : "---"}
                </div>
                <div className={`text-sm ${kpis?.deltas.stars && kpis.deltas.stars > 0 ? 'text-green-500' : 'text-red-500'}`}>
                  {kpis?.deltas.stars ? 
                    `${kpis.deltas.stars > 0 ? '+' : ''}${kpis.deltas.stars.toFixed(1)}★` : 
                    'No data'}
                </div>
              </div>
            </div>

            {/* Row 2: Performance Tracker + Gauge */}
            <div className="grid grid-cols-[2fr,1fr] gap-6">
              <div className="bg-[#2c2c2c] border border-[#3a3a3a] rounded-lg p-6">
                <div className="flex items-center justify-between mb-4">
                  <h2 className="text-lg font-semibold">Performance Tracker</h2>
                  <div className={`text-sm ${kpis?.deltas.sentiment && kpis.deltas.sentiment > 0 ? 'text-green-500' : 'text-red-500'}`}>
                    {kpis?.deltas.sentiment ? 
                      `${kpis.deltas.sentiment > 0 ? '+' : ''}${kpis.deltas.sentiment.toFixed(1)}% vs last period` : 
                      ''}
                  </div>
                </div>
                <ResponsiveContainer width="100%" height={260}>
                  <BarChart data={chartData}>
                    <XAxis
                      dataKey="month"
                      tick={{ fill: "#9ca3af" }}
                      tickFormatter={(value) => value.substring(5, 7)} // Show just MM
                    />
                    <YAxis
                      domain={[0, 100]}
                      tick={{ fill: "#9ca3af" }}
                    />
                    <Tooltip
                      contentStyle={{
                        backgroundColor: "#2c2c2c",
                        border: "1px solid #3a3a3a",
                        borderRadius: "8px",
                      }}
                      labelStyle={{ color: "#fff", marginBottom: "8px" }}
                      formatter={(value: number) => `${value.toFixed(1)}`}
                    />
                    <Bar dataKey="sentiment" radius={[4, 4, 0, 0]}>
                      {chartData.map((entry, index) => (
                        <Cell key={`cell-${index}`} fill={getBarColor(entry.sentiment)} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div>

              <div className="bg-[#2c2c2c] border border-[#3a3a3a] rounded-lg p-6">
                <div className="flex items-center justify-between mb-4">
                  <h2 className="text-lg font-semibold">Overall Sentiment</h2>
                </div>
                <div className="flex flex-col items-center justify-center h-[260px]">
                  <div className="relative w-32 h-32 mb-4">
                    <ResponsiveContainer width="100%" height="100%">
                      <RadialBarChart
                        cx="50%"
                        cy="50%"
                        innerRadius="60%"
                        outerRadius="80%"
                        data={[
                          {
                            name: "sentiment",
                            value: kpis ? kpis.sentiment_score : 0,
                            fill: kpis?.sentiment_score
                              ? kpis.sentiment_score >= 70
                                ? "#10b981"
                                : kpis.sentiment_score >= 50
                                ? "#f59e0b"
                                : "#ef4444"
                              : "#6b7280",
                          },
                        ]}
                        startAngle={180}
                        endAngle={0}
                      >
                        <RadialBar
                          dataKey="value"
                          cornerRadius={8}
                          fill="#10b981"
                        />
                      </RadialBarChart>
                    </ResponsiveContainer>
                    <div className="absolute inset-0 flex items-center justify-center">
                      <div className="text-center">
                        <div className="text-4xl font-bold">
                          {kpis ? kpis.sentiment_score.toFixed(0) : "---"}
                        </div>
                        <div className="text-xs text-gray-400">/100</div>
                      </div>
                    </div>
                  </div>
                  <div className="text-sm text-gray-400 mb-4">Overall Experience Score</div>
                  <div className="grid grid-cols-2 gap-3 w-full">
                    <div className="bg-[#3a3a3a] rounded p-3">
                      <div className="text-xs text-gray-400">Reviews</div>
                      <div className="text-lg font-semibold">{kpis ? kpis.total_reviews.toLocaleString() : "---"}</div>
                      {kpis?.deltas.reviews && (
                        <div className={`text-xs ${kpis.deltas.reviews > 0 ? 'text-green-500' : 'text-red-500'}`}>
                          {kpis.deltas.reviews > 0 ? '+' : ''}{kpis.deltas.reviews}
                        </div>
                      )}
                    </div>
                    <div className="bg-[#3a3a3a] rounded p-3">
                      <div className="text-xs text-gray-400">Avg Stars</div>
                      <div className="text-lg font-semibold">{kpis ? kpis.avg_stars.toFixed(1) : "---"}★</div>
                      {kpis?.deltas.stars && (
                        <div className={`text-xs ${kpis.deltas.stars > 0 ? 'text-green-500' : 'text-red-500'}`}>
                          {kpis.deltas.stars > 0 ? '+' : ''}{kpis.deltas.stars.toFixed(1)}★
                        </div>
                      )}
                    </div>
                  </div>
                </div>
              </div>
            </div>

            {/* Row 3: Task Monitoring */}
            <div className="bg-[#2c2c2c] border border-[#3a3a3a] rounded-lg p-6">
              <div className="flex items-center justify-between mb-4">
                <h2 className="text-lg font-semibold">Task Monitoring</h2>
                <div className="flex gap-2">
                  <input
                    type="text"
                    placeholder="Search..."
                    className="bg-[#3a3a3a] border border-[#4a4a4a] rounded px-3 py-1 text-sm"
                  />
                  <button className="px-3 py-1 bg-[#3a3a3a] hover:bg-[#4a4a4a] rounded text-sm">
                    Filters
                  </button>
                </div>
              </div>
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-[#3a3a3a]">
                    <th className="text-left py-3 px-3">Action Item</th>
                    <th className="text-left py-3 px-3">Theme</th>
                    <th className="text-left py-3 px-3">Impact</th>
                    <th className="text-left py-3 px-3">Status</th>
                    <th className="text-left py-3 px-3">Progress</th>
                  </tr>
                </thead>
                <tbody>
                  {tasks.length === 0 ? (
                    <tr>
                      <td colSpan={5} className="py-8 text-center text-gray-500">
                        No tasks yet. Loading insights...
                      </td>
                    </tr>
                  ) : (
                    tasks.map((task) => (
                      <tr
                        key={task.id}
                        className="border-b border-[#3a3a3a] hover:bg-[#333333] cursor-pointer"
                        onClick={() => openDrawer(task)}
                      >
                        <td className="py-3 px-3">{task.title}</td>
                        <td className="py-3 px-3">
                          <span className="bg-[#3a3a3a] px-2 py-1 rounded text-xs">
                            {task.theme}
                          </span>
                        </td>
                        <td className="py-3 px-3">
                          <span
                            className={`px-2 py-1 rounded text-xs ${
                              task.impact === "High"
                                ? "bg-red-600"
                                : task.impact === "Med"
                                ? "bg-yellow-600"
                                : "bg-gray-600"
                            }`}
                          >
                            {task.impact}
                          </span>
                        </td>
                        <td
                          className="py-3 px-3"
                          onClick={(e) => {
                            e.stopPropagation();
                            cycleStatus(task.id);
                          }}
                        >
                          <span
                            className={`px-2 py-1 rounded text-xs cursor-pointer ${
                              task.status === "In Progress"
                                ? "bg-blue-600"
                                : task.status === "Done"
                                ? "bg-green-600"
                                : task.status === "In Review"
                                ? "bg-purple-600"
                                : "bg-[#3a3a3a] text-gray-400"
                            }`}
                          >
                            {task.status}
                          </span>
                        </td>
                        <td className="py-3 px-3">
                          <div className="flex items-center gap-2">
                            <div className="flex-1 bg-[#3a3a3a] h-2 rounded-full">
                              <div
                                className="bg-blue-600 h-2 rounded-full"
                                style={{ width: `${task.progress}%` }}
                              />
                            </div>
                            <span className="text-xs">{task.progress}%</span>
                          </div>
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>

            {/* Row 4: Executive Summary */}
            <div className="bg-[#2c2c2c] border border-[#3a3a3a] rounded-lg p-6">
              <div className="flex items-center justify-between mb-4">
                <h2 className="text-lg font-semibold">Executive Summary</h2>
                <div className="flex items-center gap-2">
                  {overview && (
                    <span className={`text-xs px-2 py-1 rounded ${
                      (overview as any).source === 'llm' ? 'bg-green-600' : 'bg-gray-600'
                    }`}>
                      {(overview as any).source === 'llm' ? 'Generated by AI' : 'Fallback Summary'}
                    </span>
                  )}
                  <button
                    onClick={handleRefresh}
                    disabled={refreshing}
                    className="px-3 py-1 bg-[#3a3a3a] hover:bg-[#4a4a4a] rounded text-sm disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    {refreshing ? "⟳ Refreshing..." : "⟳ Refresh"}
                  </button>
                  {overview && (
                    <span className="text-xs text-gray-400">
                      Updated {formatTimeAgo(overview.last_run || null)}
                    </span>
                  )}
                </div>
              </div>

              {overview?.insights ? (
                <div className="grid md:grid-cols-3 gap-6">
                  <div>
                    <h3 className="font-medium mb-2 text-green-400">What customers love</h3>
                    <ul className="space-y-1 text-sm text-gray-300">
                      {deduplicateBullets(overview.insights.love.slice(0, 5)).map((item, i) => (
                        <li key={i}>• {truncateText(item)}</li>
                      ))}
                    </ul>
                  </div>

                  <div>
                    <h3 className="font-medium mb-2 text-red-400">Needs improvement</h3>
                    <ul className="space-y-1 text-sm text-gray-300">
                      {deduplicateBullets(overview.insights.improve.slice(0, 5)).map((item, i) => (
                        <li key={i}>• {truncateText(item)}</li>
                      ))}
                    </ul>
                  </div>

                  <div>
                    <h3 className="font-medium mb-2 text-blue-400">Recommendations</h3>
                    <ul className="space-y-1 text-sm text-gray-300">
                      {deduplicateBullets(overview.insights.recommendations.slice(0, 3)).map((item, i) => (
                        <li key={i}>• {truncateText(item)}</li>
                      ))}
                    </ul>
                  </div>
                </div>
              ) : (
                <div className="text-gray-500">Loading insights...</div>
              )}

              <div className="mt-6 pt-6 border-t border-[#3a3a3a]">
                <p className="text-sm text-gray-300">
                  {getTrendSentence() || (kpis ? (
                    <>
                      Overall sentiment score: {kpis.sentiment_score.toFixed(0)}/100.
                      {kpis.deltas.sentiment && kpis.deltas.sentiment > 0
                        ? ` Improved ${kpis.deltas.sentiment.toFixed(1)} points vs last period.`
                        : ''}
                    </>
                  ) : (
                    'Loading summary data...'
                  ))}
                </p>
              </div>
            </div>
          </div>
        </div>

        {/* Right Drawer */}
        {drawerOpen && selectedTask && (
          <aside className="w-96 bg-[#2c2c2c] border-l border-[#3a3a3a] p-6 overflow-y-auto">
            <div className="flex items-center justify-between mb-6">
              <h2 className="text-lg font-semibold">Task Details</h2>
              <button
                onClick={() => setDrawerOpen(false)}
                className="text-gray-400 hover:text-white"
              >
                ✕
              </button>
            </div>

            <div className="space-y-4">
              <div>
                <div className="text-sm text-gray-400 mb-1">Action</div>
                <div className="font-medium">{selectedTask.title}</div>
              </div>

              <div>
                <div className="text-sm text-gray-400 mb-1">Theme</div>
                <div className="inline-block bg-[#3a3a3a] px-2 py-1 rounded text-xs">
                  {selectedTask.theme}
                </div>
              </div>

              <div>
                <div className="text-sm text-gray-400 mb-1">Impact</div>
                <span
                  className={`px-2 py-1 rounded text-xs ${
                    selectedTask.impact === "High"
                      ? "bg-red-600"
                      : selectedTask.impact === "Med"
                      ? "bg-yellow-600"
                      : "bg-gray-600"
                  }`}
                >
                  {selectedTask.impact}
                </span>
              </div>

              <div>
                <div className="text-sm text-gray-400 mb-2">Why this matters</div>
                <div className="text-sm text-gray-300 space-y-2">
                  {(() => {
                    const themeQuotes = getThemeQuotes(selectedTask.theme);
                    return (
                      <>
                        {themeQuotes.positive.length > 0 && (
                          <p className="text-green-400">
                            ✓ "{themeQuotes.positive[0].text}"
                          </p>
                        )}
                        {themeQuotes.negative.length > 0 && (
                          <p className="text-red-400">
                            ✗ "{themeQuotes.negative[0].text}"
                          </p>
                        )}
                      </>
                    );
                  })()}
                </div>
              </div>

              <div>
                <div className="text-sm text-gray-400 mb-2">Recent trend</div>
                <div className="h-24 bg-[#1a1a1a] rounded p-2">
                  <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={getThemeTrend(selectedTask.theme)}>
                      <Line
                        type="monotone"
                        dataKey="sentiment"
                        stroke="#10b981"
                        strokeWidth={2}
                        dot={false}
                      />
                    </LineChart>
                  </ResponsiveContainer>
                </div>
              </div>

              <div className="flex gap-2 pt-4 border-t border-[#3a3a3a]">
                <button
                  onClick={() => {
                    const newStatus = selectedTask.status === "Backlog" ? "In Progress" : "Done";
                    setTasks((prev) =>
                      prev.map((t) => (t.id === selectedTask.id ? { ...t, status: newStatus } : t))
                    );
                  }}
                  className="flex-1 px-4 py-2 bg-blue-600 hover:bg-blue-700 rounded text-sm"
                >
                  {selectedTask.status === "Backlog" ? "Start Task" : "Mark Done"}
                </button>
              </div>
            </div>
          </aside>
        )}
      </div>
    </main>
  );
}
