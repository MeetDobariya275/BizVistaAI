import axios from "axios";
import { BusinessZ, SearchBusinessZ, QueryResponseZ, DateRangeZ } from "./schemas";

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

export async function searchBusinesses(query: string = "") {
  try {
    const { data } = await axios.get(`${base}/search/businesses`, {
      ...axiosConfig,
      params: { q: query },
    });
    return SearchBusinessZ.array().parse(data);
  } catch (error) {
    console.error("Error searching businesses:", error);
    throw error;
  }
}

export async function getBusinessDateRange(businessId: string) {
  try {
    const { data } = await axios.get(`${base}/businesses/${businessId}/date-range`, axiosConfig);
    return DateRangeZ.parse(data);
  } catch (error) {
    console.error("Error fetching date range:", error);
    throw error;
  }
}

export async function queryKeywordAnalytics(
  businessId: string,
  startDate: string,
  endDate: string,
  keywords: string[]
) {
  try {
    const { data } = await axios.post(
      `${base}/query`,
      {
        business_id: businessId,
        start_date: startDate,
        end_date: endDate,
        keywords: keywords,
      },
      axiosConfig
    );
    return QueryResponseZ.parse(data);
  } catch (error) {
    console.error("Error querying keyword analytics:", error);
    throw error;
  }
}
