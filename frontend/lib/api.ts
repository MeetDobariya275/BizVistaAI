import axios from "axios";
import { BusinessZ, OverviewZ, TrendsZ, CompareZ, KPIZ, QuotesZ } from "./schemas";

const base = process.env.NEXT_PUBLIC_API_BASE ?? "http://localhost:4174/api";

const axiosConfig = {
  headers: {
    'Content-Type': 'application/json',
  },
};

export async function fetchBusinesses() {
  try {
    const { data } = await axios.get(`${base}/businesses`, axiosConfig);
    return BusinessZ.array().parse(data);
  } catch (error) {
    console.error("Error fetching businesses:", error);
    throw error;
  }
}

export async function fetchOverview(id: string | number) {
  try {
    const { data } = await axios.get(`${base}/businesses/${id}/overview`, axiosConfig);
    return OverviewZ.parse(data);
  } catch (error) {
    console.error("Error fetching overview:", error);
    throw error;
  }
}

export async function fetchTrends(id: string | number) {
  try {
    const { data } = await axios.get(`${base}/businesses/${id}/trends`, axiosConfig);
    return TrendsZ.parse(data);
  } catch (error) {
    console.error("Error fetching trends:", error);
    throw error;
  }
}

export async function fetchCompare(ids: (string | number)[]) {
  try {
    console.log("Fetching compare with IDs:", ids);
    const { data } = await axios.get(`${base}/compare`, {
      ...axiosConfig,
      params: { ids: ids.join(",") },
    });
    console.log("Got compare data:", data);
    
    // Manual validation instead of Zod parse to avoid module issues
    if (!data || !Array.isArray(data.themes) || !Array.isArray(data.scores)) {
      throw new Error("Invalid compare data structure");
    }
    
    return data as { themes: string[]; scores: Array<Record<string, string | number>> };
  } catch (error) {
    console.error("Error fetching compare:", error);
    throw error;
  }
}

export async function fetchCompareNarrative(ids: (string | number)[]) {
  try {
    console.log("Fetching narrative compare with IDs:", ids);
    const { data } = await axios.get(`${base}/compare-narrative`, {
      ...axiosConfig,
      params: { ids: ids.join(",") },
    });
    console.log("Got narrative data:", data);
    return data as {
      summary: string;
      by_theme: string[];
      risks: string[];
      opportunities: string[];
      overall_leader: string;
      source: string;
      cached: boolean;
      generated_at: string;
    };
  } catch (error) {
    console.error("Error fetching narrative compare:", error);
    throw error;
  }
}

export async function fetchKPIs(id: string | number, period: string = "30d") {
  try {
    const { data } = await axios.get(`${base}/businesses/${id}/kpis`, {
      ...axiosConfig,
      params: { period },
    });
    return KPIZ.parse(data);
  } catch (error) {
    console.error("Error fetching KPIs:", error);
    throw error;
  }
}

export async function fetchQuotes(id: string | number, period: string = "30d") {
  try {
    const { data } = await axios.get(`${base}/businesses/${id}/quotes`, {
      ...axiosConfig,
      params: { period },
    });
    return QuotesZ.parse(data);
  } catch (error) {
    console.error("Error fetching quotes:", error);
    throw error;
  }
}
