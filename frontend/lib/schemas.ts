import { z } from "zod";

export const BusinessZ = z.object({
  id: z.string(),
  name: z.string(),
  city: z.string(),
  category: z.string().nullable().optional(),
  review_count: z.number(),
  stars: z.number(),
});
export type Business = z.infer<typeof BusinessZ>;

// Search businesses schema
export const SearchBusinessZ = z.object({
  id: z.string(),
  name: z.string(),
  city: z.string(),
  review_count: z.number(),
});
export type SearchBusiness = z.infer<typeof SearchBusinessZ>;

// Date range schema
export const DateRangeZ = z.object({
  min_date: z.string(),
  max_date: z.string(),
  total_reviews: z.number(),
});
export type DateRange = z.infer<typeof DateRangeZ>;

// Query response schema - handles both success and insufficient_data cases
export const QueryResponseZ = z.object({
  insufficient_data: z.boolean().optional(),
  message: z.string().optional(),
  matched_reviews: z.number().optional(),
  total_reviews: z.number().optional(),
  kpis: z.object({
    matched_reviews: z.number(),
    sentiment_score: z.number(),
    avg_stars: z.number(),
    deltas: z.object({
      reviews: z.number(),
      sentiment: z.number(),
      stars: z.number(),
    }),
    sparkline: z.array(z.number()),
  }).optional(),
  time_series: z.array(z.object({
    bucket: z.string(),
    hits: z.number(),
    avg_sentiment: z.number(),
  })).optional(),
  by_keyword: z.array(z.object({
    term: z.string(),
    hits: z.number(),
    avg_sentiment: z.number(),
  })).optional(),
  quotes_by_keyword: z.record(
    z.string(),
    z.object({
      positive: z.array(z.string()),
      negative: z.array(z.string()),
    })
  ).optional(),
  summary: z.object({
    love: z.array(z.string()),
    improve: z.array(z.string()),
    recommendations: z.array(z.string()),
  }).optional(),
  summary_source: z.string().optional(),
  share_of_voice: z.record(z.string(), z.number()).optional(),
  generated_at: z.string().optional(),
});
export type QueryResponse = z.infer<typeof QueryResponseZ>;
