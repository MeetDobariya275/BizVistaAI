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

export const ThemeZ = z.object({
  theme: z.string(),
  score: z.number(),
  delta: z.number().nullable().optional(),
});

export const OverviewZ = z.object({
  business: z.object({
    id: z.string(),
    name: z.string(),
    city: z.string(),
    stars: z.number(),
  }),
  themes: z.array(ThemeZ),
  keywords: z.array(z.object({
    term: z.string(),
    count: z.number(),
    tfidf: z.number(),
  })),
  insights: z.object({
    love: z.array(z.string()),
    improve: z.array(z.string()),
    recommendations: z.array(z.string()),
  }),
  last_run: z.string().nullable(),
});
export type Overview = z.infer<typeof OverviewZ>;

export const TrendRowZ = z.object({
  month: z.string(),
  avg_sentiment: z.number(),
  review_count: z.number(),
});
export const TrendsZ = z.array(TrendRowZ);
export type Trends = z.infer<typeof TrendsZ>;

export const CompareZ = z.object({
  themes: z.array(z.string()),
  scores: z.array(z.record(z.string(), z.union([z.string(), z.number()]))),
});
export type CompareResp = z.infer<typeof CompareZ>;

// KPIs schema
export const KPIZ = z.object({
  total_reviews: z.number(),
  sentiment_score: z.number(),
  avg_stars: z.number(),
  deltas: z.object({
    reviews: z.number().nullable(),
    sentiment: z.number().nullable(),
    stars: z.number().nullable(),
  }),
  sparkline: z.array(z.object({
    month: z.string(),
    sentiment: z.number(),
  })),
});
export type KPIs = z.infer<typeof KPIZ>;

// Quotes schema
export const QuotesZ = z.object({
  quotes_by_theme: z.record(
    z.string(),
    z.object({
      positive: z.array(z.string()),
      negative: z.array(z.string()),
    })
  ),
});
export type Quotes = z.infer<typeof QuotesZ>;
