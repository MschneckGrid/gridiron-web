import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";


const TELEGRAM_TOKEN = Deno.env.get("TELEGRAM_BOT_TOKEN") || "";
const MIKE_CHAT = Deno.env.get("TELEGRAM_MIKE_ID") || "5202884769";
const DAVID_CHAT = Deno.env.get("TELEGRAM_DAVID_ID") || "8324047763";

async function sendTelegram(chatId: string, text: string) {
  if (!TELEGRAM_TOKEN) return;
  try { await fetch(`https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ chat_id: chatId, text, parse_mode: "HTML" }) }); } catch (_e) { /* non-fatal */ }
}
async function sendBoth(text: string) { await Promise.all([sendTelegram(MIKE_CHAT, text), sendTelegram(DAVID_CHAT, text)]); }

const CRON_SECRET = Deno.env.get("CRON_SECRET") || "";
const PARAMETER_VERSION = "v4.3-telegram";
const STRONG_BUY = 75, BUY = 60, ACCUMULATE = 52, HOLD = 40, SELL = 20;
const HORIZONS = [30, 60, 90, 180];
const DEFAULT_VOL = 25;

const BDC_LAYER_WEIGHTS = { L1: 0.25, L2: 0.25, L3: 0.15, L4: 0.20, L6: 0.10 };
const CEF_LAYER_WEIGHTS = { L1: 0.30, L2: 0.20, L3: 0.05, L4: 0.25, L6: 0.10 };
const MIDSTREAM_LAYER_WEIGHTS = { L1: 0.30, L2: 0.25, L3: 0.10, L4: 0.20, L6: 0.15 };
const MIDSTREAM_TIER_PREMIUM: Record<string, number> = { T1: 0.02, T2: 0.0275, T3: 0.0375 };
const REIT_LAYER_WEIGHTS = { L1: 0.45, L2: 0.35, L3: 0.05, L4: 0.05, L6: 0.10 };
const L1_W = { discount: 0.50, momentum: 0.25, yield_vol: 0.25 };

const BDC_L2_T = { nii_coverage_excellent: 1.20, nii_coverage_good: 1.05, nii_coverage_warning: 0.90, nav_decline_warning: -5.0, nav_decline_severe: -10.0, leverage_comfortable: 1.0, leverage_moderate: 1.25, leverage_aggressive: 1.50 };
const BDC_L2_SUB_W = { nii_coverage: 0.35, nav_trajectory: 0.30, leverage: 0.35 };
const CEF_L2_T = { leverage_conservative: 25.0, leverage_moderate: 33.0, leverage_aggressive: 40.0 };
const BDC_L4_T = { first_lien_excellent: 80.0, first_lien_good: 60.0, non_accrual_low: 1.0, non_accrual_moderate: 3.0, non_accrual_high: 5.0, concentration_safe: 30.0, concentration_moderate: 50.0, pik_portfolio_concern: 10.0 };
const BDC_L4_SUB_W = { seniority: 0.25, non_accruals: 0.30, concentration: 0.20, pik_exposure: 0.15, diversity: 0.10 };

const CEF_VETOES = {
  leverage_breach:  { severity: "hard" as const, penalty: 25, description: "Effective leverage > 45% \u2014 elevated deleveraging risk" },
  distribution_cut: { severity: "hard" as const, penalty: 25, description: "Distribution cut > 5% in last 6 months \u2014 fundamental deterioration" },
  liquidity_risk:   { severity: "soft" as const, penalty: 10, description: "Avg 90-day volume < 50K \u2014 wide spreads, difficult exit" },
};
const BDC_VETOES = {
  leverage_breach:  { severity: "hard" as const, penalty: 25, description: "D/E > 1.5x \u2014 approaching regulatory limit" },
  liquidity_risk:   { severity: "soft" as const, penalty: 10, description: "Avg 90-day volume < 50K \u2014 wide spreads, difficult exit" },
};
const EI_VETOES = {
  yield_trap: { severity: "soft" as const, penalty: 10, description: "Yield > 12% \u2014 potential sustainability risk" },
  momentum_collapse: { severity: "soft" as const, penalty: 15, description: "60d momentum < -20% \u2014 severe price deterioration" },
  sector_hard_stop: { severity: "hard" as const, penalty: 50, description: "Sector excluded by investment policy" },
  premium_to_peers: { severity: "soft" as const, penalty: 10, description: "Yield 300bp+ below sector avg \u2014 significant premium to peers" },
};

const REIT_HARD_STOP_SECTORS = new Set(["Office", "Mortgage"]);

interface VetoTrigger { name: string; severity: "hard" | "soft"; penalty: number; description: string; data_point?: string; }
interface ZRow { ticker: string; trade_date: string; price: number; nav: number; discount_pct: number | null; yield_pct: number | null; z1y?: number | null; z3y?: number | null; z5y?: number | null; z1m?: number | null; z3m?: number | null; z6m?: number | null; z7y?: number | null; z10y?: number | null; }
interface MomRow { ticker: string; momentum_60d: number; discount_slope_30d: number; rsi_14: number; volume_ratio: number; current_yield: number; }
interface PeerRow { sector: string; peer_median_discount: number; peer_count: number; peer_avg_yield: number; }
interface BDCFundRow { ticker: string; nii_coverage_ratio: number | null; debt_to_equity: number | null; nav_per_share: number | null; }
interface BDCHoldRow { ticker: string; pct_first_lien: number; pct_second_lien: number; pct_equity: number; non_accrual_pct: number; pik_pct: number; sector_count: number; max_single_position_pct: number; total_holdings: number; }
interface BDCNavRow { ticker: string; report_date: string; nav_per_share: number; }
interface CEFFundRow { ticker: string; leverage_pct: number | null; distribution_yield: number | null; effective_duration: number | null; }
interface EICacheRow { ticker: string; asset_class: string; sector: string; price: number; yield_pct: number | null; yield_z1m: number | null; yield_z3m: number | null; yield_z6m: number | null; yield_z1y: number | null; momentum_60d: number | null; discount_slope_30d: number | null; rsi_14: number | null; volume_ratio: number | null; sector_avg_yield: number | null; yield_spread: number | null; trade_date: string; }
interface LayerScore { raw_score: number; weight: number; weighted_score: number; has_data: boolean; components: Record<string, number>; notes?: string[]; }

function clamp(v: number, lo = 0, hi = 100): number { return Math.max(lo, Math.min(hi, v)); }
function zScoreToScore(z: number | null): number { if (z === null || z === undefined) return 50; return clamp((-z + 2.0) / 4.0 * 100); }
function normalizeScore(val: number, min: number, max: number, invert = false): number { if (max === min) return 50; let n = (val - min) / (max - min) * 100; if (invert) n = 100 - n; return clamp(n); }

function scoreDiscountPremium(z1y: number | null, z3y: number | null, z5y: number | null, discount: number, peerMedian: number, percentile5yr: number): { composite: number; z_blend: number; peer_relative: number; percentile_5yr: number } {
  const z1 = zScoreToScore(z1y); const z3 = zScoreToScore(z3y); const z5 = zScoreToScore(z5y);
  const z_blend = z1 * 0.50 + z3 * 0.30 + z5 * 0.20;
  const peerDiff = (peerMedian || 0) - discount;
  const peer_relative = clamp(50 + peerDiff * 5);
  const percentile_5yr = clamp(100 - percentile5yr);
  const composite = z_blend * 0.50 + peer_relative * 0.25 + percentile_5yr * 0.25;
  return { composite: clamp(composite), z_blend, peer_relative, percentile_5yr };
}

function scoreMomentum(mom60d: number, slope30d: number, volRatio: number, rsi: number): { composite: number; momentum_60d: number; discount_slope: number; volume_ratio: number; rsi: number } {
  const momScore = clamp(50 + mom60d * 2.5);
  const slopeScore = clamp(50 - slope30d * 500);
  const volScore = volRatio > 0 ? clamp(volRatio * 50) : 25;
  let rsiScore: number;
  if (rsi < 30) rsiScore = 80 + (30 - rsi); else if (rsi < 50) rsiScore = 60 + (50 - rsi); else if (rsi < 70) rsiScore = 50 - (rsi - 50) * 1.5; else rsiScore = Math.max(0, 20 - (rsi - 70));
  const composite = momScore * 0.35 + slopeScore * 0.30 + clamp(volScore) * 0.15 + clamp(rsiScore) * 0.20;
  return { composite: clamp(composite), momentum_60d: momScore, discount_slope: slopeScore, volume_ratio: volScore, rsi: clamp(rsiScore) };
}

function scoreYieldVol(yieldPct: number, peerAvgYield: number, vol30d: number): { composite: number; yield: number; yield_spread: number; volatility: number } {
  const yieldRaw = Math.min(yieldPct || 0, 15);
  const yieldScore = clamp(yieldRaw / 15 * 100);
  const spread = (yieldPct || 0) - (peerAvgYield || 0);
  const spreadScore = clamp(50 + spread * 10);
  const volScore = normalizeScore(vol30d, 5, 50, true);
  const composite = yieldScore * 0.35 + spreadScore * 0.35 + volScore * 0.30;
  return { composite: clamp(composite), yield: yieldScore, yield_spread: spreadScore, volatility: volScore };
}

function scoreL1(dp: ReturnType<typeof scoreDiscountPremium>, mt: ReturnType<typeof scoreMomentum>, yv: ReturnType<typeof scoreYieldVol>): LayerScore {
  const raw = dp.composite * L1_W.discount + mt.composite * L1_W.momentum + yv.composite * L1_W.yield_vol;
  return { raw_score: clamp(raw), weight: 0, weighted_score: 0, has_data: true, components: { discount_premium: r2(dp.composite), z_blend: r2(dp.z_blend), peer_relative: r2(dp.peer_relative), momentum_trend: r2(mt.composite), momentum_60d: r2(mt.momentum_60d), discount_slope: r2(mt.discount_slope), rsi: r2(mt.rsi), yield_vol: r2(yv.composite), yield_score: r2(yv.yield), yield_spread: r2(yv.yield_spread), volatility: r2(yv.volatility) } };
}

function scoreEIL1(row: EICacheRow): LayerScore {
  const yz1 = row.yield_z1y ? clamp(((row.yield_z1y) + 2.0) / 4.0 * 100) : 50;
  const yz3 = row.yield_z3m ? clamp(((row.yield_z3m) + 2.0) / 4.0 * 100) : 50;
  const yz6 = row.yield_z6m ? clamp(((row.yield_z6m) + 2.0) / 4.0 * 100) : 50;
  const yieldScore = yz1 * 0.50 + yz3 * 0.30 + yz6 * 0.20;
  const spread = Number(row.yield_spread) || 0;
  const spreadScore = clamp(50 + spread * 500);
  const mom60d = Number(row.momentum_60d) || 0;
  const slope30d = Number(row.discount_slope_30d) || 0;
  const momScore = clamp(50 + mom60d * 2.5);
  const slopeScore = clamp(50 - slope30d * 500);
  const momentumComposite = momScore * 0.60 + slopeScore * 0.40;
  const raw = yieldScore * 0.50 + spreadScore * 0.15 + momentumComposite * 0.35;
  return { raw_score: r2(clamp(raw)), weight: 0, weighted_score: 0, has_data: true, components: { yield_attractiveness: r2(yieldScore), yield_z1y: r2(yz1), yield_spread: r2(spreadScore), momentum: r2(momentumComposite), momentum_60d: r2(momScore), price_slope: r2(slopeScore) } };
}

function scoreREITL2(row: EICacheRow, payoutRatio: number | undefined, privateCapRate: number | undefined, fund: any | null): LayerScore {
  const notes: string[] = []; const subScores: Record<string, number> = {}; const subWeights: Record<string, number> = {}; let hasAnyData = false;
  if (row.yield_pct && payoutRatio) { const yieldPct = Number(row.yield_pct) * 100; const impliedCapRate = yieldPct / payoutRatio; const privRate = privateCapRate || 5.5; const capSpread = impliedCapRate - privRate; subScores.cap_rate_spread = clamp(50 + capSpread * 15); subScores.implied_cap_rate = r2(impliedCapRate); subWeights.cap_rate_spread = 0.30; hasAnyData = true; }
  if (fund?.debt_to_ebitda != null) { const de = Number(fund.debt_to_ebitda); let s: number; if (de <= 6) s = 80 + (6 - de) * 5; else if (de <= 8) s = 45 + (8 - de) / 2 * 35; else s = Math.max(10, 45 - (de - 8) * 10); subScores.debt_to_ebitda = clamp(s); subWeights.debt_to_ebitda = 0.25; hasAnyData = true; } else { notes.push("No debt/EBITDA"); }
  if (fund?.ffo_payout_ratio != null) { const payout = Number(fund.ffo_payout_ratio); let s: number; if (payout <= 85) s = 85 + (85 - payout) * 0.5; else if (payout <= 95) s = 50 + (95 - payout) / 10 * 25; else if (payout <= 100) s = 25 + (100 - payout) / 5 * 25; else s = Math.max(0, 25 - (payout - 100) * 3); subScores.ffo_payout = clamp(s); subWeights.ffo_payout = 0.30; hasAnyData = true; } else { notes.push("No FFO payout"); }
  if (row.yield_pct) { const yieldPct = Number(row.yield_pct) * 100; subScores.yield_level = clamp(yieldPct / 10 * 80); subWeights.yield_level = 0.15; hasAnyData = true; }
  if (!hasAnyData) return { raw_score: 50, weight: 0, weighted_score: 0, has_data: false, components: {}, notes: ["No REIT L2 data"] };
  const totalW = Object.values(subWeights).reduce((s, w) => s + w, 0); let raw = 0; for (const [k, w] of Object.entries(subWeights)) { raw += subScores[k] * (w / totalW); }
  return { raw_score: r2(clamp(raw)), weight: 0, weighted_score: 0, has_data: true, components: roundAll(subScores), notes };
}

function scoreMidstreamL2(fund: any | null, tier: string, distSummaryMap: Record<string, any> = {}): LayerScore {
  const notes: string[] = []; const subScores: Record<string, number> = {}; const subWeights: Record<string, number> = {}; let hasAnyData = false;
  if (fund?.debt_to_ebitda != null) { const de = Number(fund.debt_to_ebitda); let s: number; if (de <= 3.5) s = 85 + (3.5 - de) * 5; else if (de <= 4.5) s = 55 + (4.5 - de) / 1 * 30; else s = Math.max(10, 55 - (de - 4.5) * 15); if (tier === "T1") s = Math.min(100, s + 10); subScores.debt_to_ebitda = clamp(s); subWeights.debt_to_ebitda = 0.35; hasAnyData = true; } else { notes.push("No debt/EBITDA"); }
  if (fund?.dcf_coverage_ratio != null) { const cov = Number(fund.dcf_coverage_ratio); let s: number; if (cov >= 1.4) s = 85 + Math.min(15, (cov - 1.4) * 30); else if (cov >= 1.2) s = 65 + (cov - 1.2) / 0.2 * 20; else if (cov >= 1.1) s = 40 + (cov - 1.1) / 0.1 * 25; else if (cov >= 1.0) s = 15 + (cov - 1.0) / 0.1 * 25; else s = Math.max(0, cov / 1.0 * 15); subScores.dcf_coverage = clamp(s); subWeights.dcf_coverage = 0.35; hasAnyData = true; } else { notes.push("No DCF coverage"); }
  const growthYears = (fund?.consecutive_growth_quarters ? Number(fund.consecutive_growth_quarters) / 4 : null); const hasCutHistory = (fund?.distribution_cut_flag); if (growthYears != null) { const years = growthYears; let s: number; if (years >= 10) s = 90; else if (years >= 5) s = 70 + (years - 5) / 5 * 20; else if (years >= 2) s = 50 + (years - 2) / 3 * 20; else s = 30; if (hasCutHistory) s = Math.min(s, 35); subScores.distribution_growth = clamp(s); subWeights.distribution_growth = 0.30; hasAnyData = true; } else { notes.push("No dist history"); }
  if (!hasAnyData) return { raw_score: 50, weight: 0, weighted_score: 0, has_data: false, components: {}, notes: ["No Midstream L2 data"] };
  const totalW = Object.values(subWeights).reduce((s, w) => s + w, 0); let raw = 0; for (const [k, w] of Object.entries(subWeights)) { raw += subScores[k] * (w / totalW); }
  return { raw_score: r2(clamp(raw)), weight: 0, weighted_score: 0, has_data: true, components: roundAll(subScores), notes };
}

function buildEIVetoes(row: EICacheRow, distSummaryMap: Record<string, any> = {}): VetoTrigger[] {
  const vetoes: VetoTrigger[] = [];
  const yieldPct = row.yield_pct ? Number(row.yield_pct) * 100 : 0;
  if (yieldPct > 12) vetoes.push({ name: "yield_trap", severity: EI_VETOES.yield_trap.severity, penalty: EI_VETOES.yield_trap.penalty, description: EI_VETOES.yield_trap.description, data_point: "yield=" + yieldPct.toFixed(1) + "%" });
  const mom = Number(row.momentum_60d) || 0;
  if (mom < -20) vetoes.push({ name: "momentum_collapse", severity: EI_VETOES.momentum_collapse.severity, penalty: EI_VETOES.momentum_collapse.penalty, description: EI_VETOES.momentum_collapse.description, data_point: "mom60d=" + mom.toFixed(1) + "%" });
  if (row.asset_class === "REIT" && REIT_HARD_STOP_SECTORS.has(row.sector)) vetoes.push({ name: "sector_hard_stop", severity: EI_VETOES.sector_hard_stop.severity, penalty: EI_VETOES.sector_hard_stop.penalty, description: "Zell framework: " + row.sector + " REITs excluded", data_point: "sector=" + row.sector });
  if (row.asset_class === "REIT" && Number(row.yield_spread) < -0.03) vetoes.push({ name: "premium_to_peers", severity: EI_VETOES.premium_to_peers.severity, penalty: EI_VETOES.premium_to_peers.penalty, description: EI_VETOES.premium_to_peers.description, data_point: "spread=" + ((Number(row.yield_spread)||0)*100).toFixed(1) + "%" });
  const distSum = distSummaryMap[row.ticker];
  if (distSum?.days_since_cut != null && distSum.days_since_cut <= 180) {
    vetoes.push({ name: "distribution_cut", severity: "hard" as const, penalty: 25, description: "Distribution cut within last 6 months \u2014 fundamental deterioration", data_point: "cut=" + (distSum.last_cut_pct || 0).toFixed(1) + "% on " + distSum.last_cut_date });
  }
  return vetoes;
}

function buildEISignalRow(row: EICacheRow, assetClass: string, today: string, signal: string, composite: number, adjustedScore: number, confidence: string, layers: Record<string, LayerScore>, vetoes: VetoTrigger[], riskPenalty: number, conviction: { bull_case: string; bear_case: string; key_assumptions: string[]; invalidation_triggers: string[] }): Record<string, unknown> {
  const layerScoresObj: Record<string, unknown> = {};
  for (const [key, ls] of Object.entries(layers)) { layerScoresObj[key] = { raw_score: ls.raw_score, weight: ls.weight, weighted_score: ls.weighted_score, has_data: ls.has_data, components: ls.components, ...(ls.notes?.length ? { notes: ls.notes } : {}) }; }
  const yieldPct = row.yield_pct ? Number(row.yield_pct) * 100 : 0;
  return {
    ticker: row.ticker, asset_class: assetClass, sector: row.sector || "General", signal_date: row.trade_date || today,
    signal, composite_score: composite, adjusted_score: adjustedScore, confidence,
    price_at_signal: row.price, nav_at_signal: null, discount_at_signal: null,
    layer_scores: layerScoresObj,
    veto_triggers: vetoes.length > 0 ? vetoes.map(v => ({ name: v.name, severity: v.severity, penalty: v.penalty, description: v.description, data_point: v.data_point })) : [],
    risk_penalties: riskPenalty,
    key_metrics: { price: row.price, yield_pct: yieldPct, yield_spread: Number(row.yield_spread) || 0, momentum_60d: Number(row.momentum_60d) || 0, rsi_14: Number(row.rsi_14) || 50, volume_ratio: Number(row.volume_ratio) || 1, sector_avg_yield: (Number(row.sector_avg_yield) || 0) * 100, ...(layers.L2?.components?.implied_cap_rate != null ? { implied_cap_rate: layers.L2.components.implied_cap_rate } : {}) },
    bull_case: conviction.bull_case, bear_case: conviction.bear_case, key_assumptions: conviction.key_assumptions, invalidation_triggers: conviction.invalidation_triggers,
    parameter_version: PARAMETER_VERSION, generated_at: new Date().toISOString(),
  };
}

function scoreBDCNiiCoverage(ratio: number | null): number {
  if (ratio === null || ratio === undefined) return 50;
  const T = BDC_L2_T;
  if (ratio >= T.nii_coverage_excellent) return clamp(90 + Math.min(10, (ratio - T.nii_coverage_excellent) * 50));
  if (ratio >= T.nii_coverage_good) return 65 + (ratio - T.nii_coverage_good) / (T.nii_coverage_excellent - T.nii_coverage_good) * 25;
  if (ratio >= T.nii_coverage_warning) return 35 + (ratio - T.nii_coverage_warning) / (T.nii_coverage_good - T.nii_coverage_warning) * 30;
  return Math.max(0, ratio / T.nii_coverage_warning * 35);
}

function scoreBDCNavTrajectory(navHistory: BDCNavRow[]): number {
  if (!navHistory || navHistory.length < 2) return 50;
  const sorted = [...navHistory].sort((a, b) => b.report_date.localeCompare(a.report_date));
  const latest = sorted[0].nav_per_share; const prior = sorted[1].nav_per_share;
  if (!latest || !prior || prior <= 0) return 50;
  const qoqChange = ((latest - prior) / prior) * 100;
  let fourQChange = 0;
  if (sorted.length >= 4) { const fourQAgo = sorted[3].nav_per_share; if (fourQAgo > 0) fourQChange = ((latest - fourQAgo) / fourQAgo) * 100; }
  const T = BDC_L2_T;
  let qoqScore: number;
  if (qoqChange >= 2) qoqScore = 90; else if (qoqChange >= 0) qoqScore = 65 + qoqChange / 2 * 25; else if (qoqChange >= T.nav_decline_warning) qoqScore = 35 + (qoqChange - T.nav_decline_warning) / (-T.nav_decline_warning) * 30; else if (qoqChange >= T.nav_decline_severe) qoqScore = 15 + (qoqChange - T.nav_decline_severe) / (T.nav_decline_warning - T.nav_decline_severe) * 20; else qoqScore = Math.max(0, 15 + (qoqChange - T.nav_decline_severe) * 1.5);
  let trendScore = 50;
  if (fourQChange > 5) trendScore = 85; else if (fourQChange > 0) trendScore = 60 + fourQChange / 5 * 25; else if (fourQChange > -10) trendScore = 30 + (fourQChange + 10) / 10 * 30; else trendScore = Math.max(0, 30 + (fourQChange + 10) * 2);
  return clamp(qoqScore * 0.60 + trendScore * 0.40);
}

function scoreBDCLeverage(deRatio: number | null): number {
  if (deRatio === null || deRatio === undefined) return 50;
  const T = BDC_L2_T;
  if (deRatio <= T.leverage_comfortable) return clamp(85 + (T.leverage_comfortable - deRatio) * 15);
  if (deRatio <= T.leverage_moderate) return 60 + (T.leverage_moderate - deRatio) / (T.leverage_moderate - T.leverage_comfortable) * 25;
  if (deRatio <= T.leverage_aggressive) return 30 + (T.leverage_aggressive - deRatio) / (T.leverage_aggressive - T.leverage_moderate) * 30;
  return Math.max(0, 30 - (deRatio - T.leverage_aggressive) * 40);
}

function scoreBDCL2(fund: BDCFundRow | null, navHistory: BDCNavRow[]): LayerScore {
  const notes: string[] = []; let hasAnyData = false;
  const subScores: Record<string, number> = {}; const activeWeights: Record<string, number> = {};
  if (fund?.nii_coverage_ratio != null) { subScores.nii_coverage = scoreBDCNiiCoverage(fund.nii_coverage_ratio); activeWeights.nii_coverage = BDC_L2_SUB_W.nii_coverage; hasAnyData = true; } else { notes.push("No NII coverage data"); }
  if (navHistory.length >= 2) { subScores.nav_trajectory = scoreBDCNavTrajectory(navHistory); activeWeights.nav_trajectory = BDC_L2_SUB_W.nav_trajectory; hasAnyData = true; } else { notes.push("Insufficient NAV history"); }
  if (fund?.debt_to_equity != null) { subScores.leverage = scoreBDCLeverage(fund.debt_to_equity); activeWeights.leverage = BDC_L2_SUB_W.leverage; hasAnyData = true; } else { notes.push("No leverage data"); }
  if (!hasAnyData) return { raw_score: 50, weight: 0, weighted_score: 0, has_data: false, components: {}, notes: ["No BDC fundamental data"] };
  const totalW = Object.values(activeWeights).reduce((s, w) => s + w, 0);
  let raw = 0; for (const [k, w] of Object.entries(activeWeights)) { raw += subScores[k] * (w / totalW); }
  return { raw_score: r2(clamp(raw)), weight: 0, weighted_score: 0, has_data: true, components: { ...roundAll(subScores) }, notes };
}

function scoreCEFLeverage(leveragePct: number | null): number {
  if (leveragePct === null || leveragePct === undefined) return 50;
  const T = CEF_L2_T;
  if (leveragePct <= T.leverage_conservative) return clamp(85 + (T.leverage_conservative - leveragePct) * 0.6);
  if (leveragePct <= T.leverage_moderate) return 60 + (T.leverage_moderate - leveragePct) / (T.leverage_moderate - T.leverage_conservative) * 25;
  if (leveragePct <= T.leverage_aggressive) return 35 + (T.leverage_aggressive - leveragePct) / (T.leverage_aggressive - T.leverage_moderate) * 25;
  return Math.max(0, 35 - (leveragePct - T.leverage_aggressive) * 3);
}

function scoreCEFDistYield(distYield: number | null, peerAvgYield: number): number {
  if (distYield === null || distYield === undefined) return 50;
  const yieldPct = distYield * 100; const peerPct = peerAvgYield * 100;
  if (yieldPct <= 0) return 20; if (yieldPct > 15) return 40;
  let base = clamp(yieldPct / 12 * 80);
  const spread = yieldPct - peerPct; base += clamp(spread * 5, -15, 15);
  return clamp(base);
}

function scoreCEFL2Enhanced(cefFund: any | null, fundInfo: any | null, distSummary: any | null, peerAvgYield: number): LayerScore {
  const notes: string[] = []; const subScores: Record<string, number> = {}; const subWeights: Record<string, number> = {}; let hasAnyData = false;
  // Leverage (30%)
  if (cefFund?.leverage_pct != null) { const lev = Number(cefFund.leverage_pct); let s: number; if (lev <= 0) s = 70; else if (lev <= 25) s = 85 + (25 - lev) * 0.6; else if (lev <= 33) s = 60 + (33 - lev) / 8 * 25; else if (lev <= 40) s = 35 + (40 - lev) / 7 * 25; else s = Math.max(5, 35 - (lev - 40) * 5); subScores.leverage = clamp(s); subWeights.leverage = 0.30; hasAnyData = true; } else { notes.push("No leverage"); }
  // Distribution stability (25%)
  if (distSummary) { const gy = Number(distSummary.consecutive_growth_years) || 0; const cuts = Number(distSummary.cut_count_5yr) || 0; let s: number; if (gy >= 10) s = 90; else if (gy >= 5) s = 70 + (gy - 5) / 5 * 20; else if (gy >= 2) s = 50 + (gy - 2) / 3 * 20; else s = 35; if (cuts >= 3) s = Math.min(s, 25); else if (cuts >= 2) s = Math.min(s, 35); else if (cuts >= 1) s = Math.min(s, 50); subScores.distribution_stability = clamp(s); subWeights.distribution_stability = 0.25; hasAnyData = true; } else { notes.push("No dist history"); }
  // Expense ratio (15%)
  if (fundInfo?.total_expense_pct != null) { const exp = Number(fundInfo.total_expense_pct); let s: number; if (exp <= 1.0) s = 85; else if (exp <= 1.5) s = 70; else if (exp <= 2.0) s = 55; else if (exp <= 2.5) s = 40; else if (exp <= 3.0) s = 25; else s = 15; subScores.expense_ratio = clamp(s); subWeights.expense_ratio = 0.15; hasAnyData = true; }
  // Yield quality (15%)
  if (cefFund?.distribution_yield != null) { const yp = Number(cefFund.distribution_yield) * 100; const pp = peerAvgYield * 100; const sp = yp - pp; let s: number; if (sp > 4) s = 40; else if (sp > 2) s = 60; else if (sp > 0) s = 75; else if (sp > -1) s = 65; else s = 50; subScores.yield_quality = clamp(s); subWeights.yield_quality = 0.15; hasAnyData = true; }
  // Fund quality (15%)
  if (fundInfo) { let s = 50; const assets = Number(fundInfo.total_assets) || 0; if (assets >= 5e9) s = 85; else if (assets >= 1e9) s = 70; else if (assets >= 500e6) s = 60; else if (assets >= 100e6) s = 45; else s = 30; if (fundInfo.inception_date) { const years = (Date.now() - new Date(fundInfo.inception_date).getTime()) / (365.25 * 86400000); if (years >= 20) s = Math.min(100, s + 10); else if (years >= 10) s = Math.min(100, s + 5); } subScores.fund_quality = clamp(s); subWeights.fund_quality = 0.15; hasAnyData = true; }
  if (!hasAnyData) return { raw_score: 50, weight: 0, weighted_score: 0, has_data: false, components: {}, notes: ["No CEF L2 data"] };
  const totalW = Object.values(subWeights).reduce((s, w) => s + w, 0); let raw = 0;
  for (const [k, w] of Object.entries(subWeights)) { raw += subScores[k] * (w / totalW); }
  return { raw_score: r2(clamp(raw)), weight: 0, weighted_score: 0, has_data: true, components: roundAll(subScores), notes };
}

function scoreCEFL3(fundInfo: any | null): LayerScore {
  if (!fundInfo) return { raw_score: 50, weight: 0, weighted_score: 0, has_data: false, components: {}, notes: ["No CEF fund info"] };
  const notes: string[] = []; const subScores: Record<string, number> = {};
  // Manager quality proxy: assets + track record
  let mgrScore = 50;
  const assets = Number(fundInfo.total_assets) || 0;
  if (assets >= 5e9) mgrScore = 85; else if (assets >= 1e9) mgrScore = 70; else if (assets >= 500e6) mgrScore = 60; else if (assets >= 100e6) mgrScore = 45; else mgrScore = 30;
  if (fundInfo.inception_date) { const years = (Date.now() - new Date(fundInfo.inception_date).getTime()) / (365.25 * 86400000); if (years >= 25) mgrScore = Math.min(100, mgrScore + 15); else if (years >= 15) mgrScore = Math.min(100, mgrScore + 10); else if (years >= 10) mgrScore = Math.min(100, mgrScore + 5); else if (years < 3) mgrScore = Math.max(0, mgrScore - 10); }
  subScores.manager_quality = clamp(mgrScore);
  // Fee pressure
  let feeScore = 50;
  if (fundInfo.mgmt_fee_pct != null) { const fee = Number(fundInfo.mgmt_fee_pct); if (fee <= 0.5) feeScore = 90; else if (fee <= 0.75) feeScore = 75; else if (fee <= 1.0) feeScore = 60; else if (fee <= 1.5) feeScore = 40; else feeScore = 25; }
  subScores.fee_pressure = feeScore;
  // Term fund bonus
  subScores.term_fund = fundInfo.is_term_fund ? 75 : 50;
  const raw = subScores.manager_quality * 0.50 + subScores.fee_pressure * 0.35 + subScores.term_fund * 0.15;
  return { raw_score: r2(clamp(raw)), weight: 0, weighted_score: 0, has_data: true, components: roundAll(subScores), notes };
}

function scoreCEFL4(nport: any | null, sector: string): LayerScore {
  if (!nport) return { raw_score: 50, weight: 0, weighted_score: 0, has_data: false, components: {}, notes: ["No N-PORT data"] };
  const subScores: Record<string, number> = {};
  // Credit quality
  const igPct = (Number(nport.aaa_pct || 0)) + (Number(nport.aa_pct || 0)) + (Number(nport.a_pct || 0)) + (Number(nport.bbb_pct || 0));
  const belowIg = Number(nport.below_ig_pct || 0);
  let cqScore: number;
  if (igPct >= 80) cqScore = 90; else if (igPct >= 60) cqScore = 75; else if (igPct >= 40) cqScore = 55; else if (belowIg >= 60) cqScore = 25; else cqScore = 40;
  subScores.credit_quality = clamp(cqScore);
  // Concentration
  const top10 = Number(nport.top_10_pct || 0);
  const topHolding = Number(nport.top_holding_pct || 0);
  let concScore: number;
  if (top10 <= 15) concScore = 90; else if (top10 <= 25) concScore = 75; else if (top10 <= 40) concScore = 55; else concScore = 30;
  if (topHolding > 10) concScore = Math.min(concScore, 35);
  subScores.concentration = clamp(concScore);
  // Duration risk
  const dur = Number(nport.effective_duration || 0);
  let durScore: number;
  if (dur <= 2) durScore = 85; else if (dur <= 4) durScore = 70; else if (dur <= 6) durScore = 55; else if (dur <= 8) durScore = 35; else durScore = 20;
  subScores.duration_risk = clamp(durScore);
  // Diversification
  const holdings = Number(nport.total_holdings || 0);
  let divScore: number;
  if (holdings >= 500) divScore = 90; else if (holdings >= 200) divScore = 75; else if (holdings >= 100) divScore = 60; else if (holdings >= 50) divScore = 45; else divScore = 25;
  subScores.diversification = clamp(divScore);
  const raw = subScores.credit_quality * 0.35 + subScores.concentration * 0.25 + subScores.duration_risk * 0.20 + subScores.diversification * 0.20;
  return { raw_score: r2(clamp(raw)), weight: 0, weighted_score: 0, has_data: true, components: roundAll(subScores) };
}

function scoreBDCL4(hold: BDCHoldRow | null): LayerScore {
  if (!hold) return { raw_score: 50, weight: 0, weighted_score: 0, has_data: false, components: {}, notes: ["No BDC holdings data"] };
  const subScores: Record<string, number> = {};
  const T = BDC_L4_T;
  // Seniority
  const fl = Number(hold.pct_first_lien || 0);
  if (fl >= T.first_lien_excellent) subScores.seniority = 90; else if (fl >= T.first_lien_good) subScores.seniority = 65 + (fl - T.first_lien_good) / (T.first_lien_excellent - T.first_lien_good) * 25; else subScores.seniority = Math.max(15, fl / T.first_lien_good * 65);
  // Non-accruals
  const na = Number(hold.non_accrual_pct || 0);
  if (na <= T.non_accrual_low) subScores.non_accruals = 90; else if (na <= T.non_accrual_moderate) subScores.non_accruals = 60 + (T.non_accrual_moderate - na) / (T.non_accrual_moderate - T.non_accrual_low) * 30; else if (na <= T.non_accrual_high) subScores.non_accruals = 30 + (T.non_accrual_high - na) / (T.non_accrual_high - T.non_accrual_moderate) * 30; else subScores.non_accruals = Math.max(0, 30 - (na - T.non_accrual_high) * 5);
  // Concentration
  const maxPos = Number(hold.max_single_position_pct || 0);
  if (maxPos <= 2) subScores.concentration = 90; else if (maxPos <= 4) subScores.concentration = 70; else if (maxPos <= 6) subScores.concentration = 50; else subScores.concentration = Math.max(10, 50 - (maxPos - 6) * 8);
  // PIK
  const pik = Number(hold.pik_pct || 0);
  if (pik <= 3) subScores.pik_exposure = 85; else if (pik <= T.pik_portfolio_concern) subScores.pik_exposure = 50 + (T.pik_portfolio_concern - pik) / (T.pik_portfolio_concern - 3) * 35; else subScores.pik_exposure = Math.max(10, 50 - (pik - T.pik_portfolio_concern) * 4);
  // Diversity
  const sc = Number(hold.sector_count || 0); const th = Number(hold.total_holdings || 0);
  let divScore = 50; if (th >= 200) divScore = 85; else if (th >= 100) divScore = 70; else if (th >= 50) divScore = 55; else divScore = 35;
  if (sc >= 15) divScore = Math.min(100, divScore + 10); else if (sc < 5) divScore = Math.max(0, divScore - 15);
  subScores.diversity = clamp(divScore);
  const W = BDC_L4_SUB_W;
  const raw = subScores.seniority * W.seniority + subScores.non_accruals * W.non_accruals + subScores.concentration * W.concentration + subScores.pik_exposure * W.pik_exposure + subScores.diversity * W.diversity;
  return { raw_score: r2(clamp(raw)), weight: 0, weighted_score: 0, has_data: true, components: roundAll(subScores) };
}

function scoreL3(assetClass: string): LayerScore {
  return { raw_score: 50, weight: 0, weighted_score: 0, has_data: false, components: {}, notes: [assetClass + " L3 placeholder"] };
}

function scoreREITL3(mgmt: any | null): LayerScore {
  if (!mgmt) return { raw_score: 50, weight: 0, weighted_score: 0, has_data: false, components: {}, notes: ["No REIT management config"] };
  const subScores: Record<string, number> = {};
  subScores.management_structure = mgmt.is_internal_mgmt ? 75 : 40;
  subScores.governance = clamp(Number(mgmt.governance_score) || 50);
  subScores.track_record = clamp(Number(mgmt.track_record_score) || 50);
  const raw = subScores.management_structure * 0.35 + subScores.governance * 0.35 + subScores.track_record * 0.30;
  return { raw_score: r2(clamp(raw)), weight: 0, weighted_score: 0, has_data: true, components: roundAll(subScores) };
}

function scoreREITL4(mgmt: any | null): LayerScore {
  if (!mgmt) return { raw_score: 50, weight: 0, weighted_score: 0, has_data: false, components: {}, notes: ["No REIT L4 data"] };
  const subScores: Record<string, number> = {};
  subScores.portfolio_quality = clamp(Number(mgmt.portfolio_quality_score) || 50);
  subScores.tenant_diversification = clamp(Number(mgmt.tenant_diversification_score) || 50);
  subScores.geographic_diversification = clamp(Number(mgmt.geographic_diversification_score) || 50);
  const raw = subScores.portfolio_quality * 0.40 + subScores.tenant_diversification * 0.35 + subScores.geographic_diversification * 0.25;
  return { raw_score: r2(clamp(raw)), weight: 0, weighted_score: 0, has_data: true, components: roundAll(subScores) };
}

function scoreMidstreamL3(mgmt: any | null): LayerScore {
  if (!mgmt) return { raw_score: 50, weight: 0, weighted_score: 0, has_data: false, components: {}, notes: ["No midstream management config"] };
  const subScores: Record<string, number> = {};
  subScores.management_quality = clamp(Number(mgmt.management_quality_score) || 50);
  subScores.governance = clamp(Number(mgmt.governance_score) || 50);
  subScores.capital_allocation = clamp(Number(mgmt.capital_allocation_score) || 50);
  const raw = subScores.management_quality * 0.40 + subScores.governance * 0.30 + subScores.capital_allocation * 0.30;
  return { raw_score: r2(clamp(raw)), weight: 0, weighted_score: 0, has_data: true, components: roundAll(subScores) };
}

function scoreMidstreamL4(mgmt: any | null): LayerScore {
  if (!mgmt) return { raw_score: 50, weight: 0, weighted_score: 0, has_data: false, components: {}, notes: ["No midstream L4 data"] };
  const subScores: Record<string, number> = {};
  subScores.asset_quality = clamp(Number(mgmt.asset_quality_score) || 50);
  subScores.contract_quality = clamp(Number(mgmt.contract_quality_score) || 50);
  subScores.basin_diversification = clamp(Number(mgmt.basin_diversification_score) || 50);
  const raw = subScores.asset_quality * 0.35 + subScores.contract_quality * 0.40 + subScores.basin_diversification * 0.25;
  return { raw_score: r2(clamp(raw)), weight: 0, weighted_score: 0, has_data: true, components: roundAll(subScores) };
}

function scoreL6(assetClass: string): LayerScore {
  return { raw_score: 50, weight: 0, weighted_score: 0, has_data: false, components: {}, notes: [assetClass + " L6 macro placeholder"] };
}

function computeComposite(layers: Record<string, LayerScore>, weights: Record<string, number>): { composite: number; layers: Record<string, LayerScore> } {
  let totalWeight = 0; let weighted = 0;
  for (const [key, w] of Object.entries(weights)) {
    const layer = layers[key]; if (!layer) continue;
    layer.weight = w; layer.weighted_score = r2(layer.raw_score * w); totalWeight += w; weighted += layer.weighted_score;
  }
  const composite = totalWeight > 0 ? clamp(weighted / totalWeight) : 50;
  return { composite: r2(composite), layers };
}

function checkMomentumConfirmation(mom60d: number, slope30d: number, volRatio: number): boolean {
  return mom60d > -5 && slope30d < 0.002 && volRatio > 0.5;
}

function determineSignal(composite: number, vetoes: VetoTrigger[], mom60d: number, slope30d: number, volRatio: number, assetClass: string): { signal: string; adjustedScore: number; riskPenalty: number } {
  let adjustedScore = composite;
  let riskPenalty = 0;
  for (const v of vetoes) { riskPenalty += v.penalty; }
  adjustedScore = Math.max(0, adjustedScore - riskPenalty);
  const hasHardVeto = vetoes.some(v => v.severity === "hard");
  if (hasHardVeto) { return { signal: "AVOID", adjustedScore, riskPenalty }; }
  let signal: string;
  if (adjustedScore >= STRONG_BUY) signal = "STRONG BUY";
  else if (adjustedScore >= BUY) signal = "BUY";
  else if (adjustedScore >= ACCUMULATE) signal = "ACCUMULATE";
  else if (adjustedScore >= HOLD) signal = "HOLD";
  else if (adjustedScore >= SELL) signal = "SELL";
  else signal = "STRONG SELL";
  // Momentum confirmation gate for BUY+
  if ((signal === "BUY" || signal === "STRONG BUY") && !checkMomentumConfirmation(mom60d, slope30d, volRatio)) {
    signal = "ACCUMULATE";
  }
  return { signal, adjustedScore: r2(adjustedScore), riskPenalty };
}

function determineConfidence(layers: Record<string, LayerScore>, vetoes: VetoTrigger[]): string {
  const dataLayers = Object.values(layers).filter(l => l.has_data).length;
  const totalLayers = Object.values(layers).length;
  const dataRatio = totalLayers > 0 ? dataLayers / totalLayers : 0;
  if (vetoes.length > 0) return dataRatio >= 0.8 ? "medium" : "low";
  if (dataRatio >= 0.8) return "high";
  if (dataRatio >= 0.5) return "medium";
  return "low";
}

function buildConviction(ticker: string, signal: string, composite: number, discount: number, z1y: number | null, yieldPct: number, mom60d: number, vetoes: VetoTrigger[], layers: Record<string, LayerScore>): { bull_case: string; bear_case: string; key_assumptions: string[]; invalidation_triggers: string[] } {
  const bulls: string[] = []; const bears: string[] = []; const assumptions: string[] = []; const invalidations: string[] = [];
  if (composite >= 70) bulls.push("Strong multi-factor score of " + composite.toFixed(0));
  if (discount < -0.10) bulls.push("Deep discount at " + (discount * 100).toFixed(1) + "%");
  if (z1y != null && z1y < -1.5) bulls.push("Z-score indicates historically cheap level");
  if (yieldPct > 8) bulls.push("Attractive yield of " + yieldPct.toFixed(1) + "%");
  if (mom60d > 5) bulls.push("Positive momentum confirmation");
  if (layers.L2?.has_data && layers.L2.raw_score >= 70) bulls.push("Strong fundamentals (L2=" + layers.L2.raw_score.toFixed(0) + ")");
  if (layers.L4?.has_data && layers.L4.raw_score >= 70) bulls.push("Solid portfolio quality (L4=" + layers.L4.raw_score.toFixed(0) + ")");
  if (composite < 40) bears.push("Weak composite score of " + composite.toFixed(0));
  if (discount > 0) bears.push("Trading at premium to NAV");
  if (mom60d < -10) bears.push("Negative momentum: " + mom60d.toFixed(1) + "%");
  for (const v of vetoes) bears.push(v.description);
  if (layers.L2?.has_data && layers.L2.raw_score < 35) bears.push("Weak fundamentals (L2=" + layers.L2.raw_score.toFixed(0) + ")");
  assumptions.push("Current market regime continues"); assumptions.push("No distribution changes in next 90 days"); assumptions.push("Liquidity conditions remain stable");
  if (signal === "BUY" || signal === "STRONG BUY") { invalidations.push("Distribution cut > 5%"); invalidations.push("NAV decline > 10% in single quarter"); invalidations.push("Momentum reversal below -15%"); }
  if (signal === "SELL" || signal === "STRONG SELL") { invalidations.push("Discount widens beyond z-score -2.0"); invalidations.push("Distribution increase > 5%"); invalidations.push("Momentum reversal above +10%"); }
  return { bull_case: bulls.join(". ") || "Limited positive catalysts identified", bear_case: bears.join(". ") || "No significant risks identified", key_assumptions: assumptions, invalidation_triggers: invalidations.length > 0 ? invalidations : ["Material change in fundamentals"] };
}

function buildCEFVetoes(ticker: string, vetoData: Record<string, any>): VetoTrigger[] {
  const vetoes: VetoTrigger[] = [];
  if (vetoData.leverage?.[ticker]) { const v = CEF_VETOES.leverage_breach; vetoes.push({ name: "leverage_breach", severity: v.severity, penalty: v.penalty, description: v.description, data_point: "leverage=" + vetoData.leverage[ticker] }); }
  if (vetoData.distribution_cuts?.[ticker]) { const v = CEF_VETOES.distribution_cut; vetoes.push({ name: "distribution_cut", severity: v.severity, penalty: v.penalty, description: v.description, data_point: "cut=" + vetoData.distribution_cuts[ticker] }); }
  if (vetoData.low_volume?.[ticker]) { const v = CEF_VETOES.liquidity_risk; vetoes.push({ name: "liquidity_risk", severity: v.severity, penalty: v.penalty, description: v.description, data_point: "vol=" + vetoData.low_volume[ticker] }); }
  return vetoes;
}

function buildBDCVetoes(ticker: string, vetoData: Record<string, any>): VetoTrigger[] {
  const vetoes: VetoTrigger[] = [];
  if (vetoData.leverage?.[ticker]) { const v = BDC_VETOES.leverage_breach; vetoes.push({ name: "leverage_breach", severity: v.severity, penalty: v.penalty, description: v.description, data_point: "d/e=" + vetoData.leverage[ticker] }); }
  if (vetoData.low_volume?.[ticker]) { const v = BDC_VETOES.liquidity_risk; vetoes.push({ name: "liquidity_risk", severity: v.severity, penalty: v.penalty, description: v.description, data_point: "vol=" + vetoData.low_volume[ticker] }); }
  return vetoes;
}

function detectRegime(macro: Record<string, number>): { label: string; score: number; description: string; buy_threshold_adj: number } {
  const t10y = macro.UST_10Y || 4.25; const t2y = macro.UST_2Y || 3.70; const cpi = macro.CPI_YOY || 2.5;
  const curve = t10y - t2y; let score = 50;
  // Rate shock: 10Y > 5% or curve inversion + high CPI
  if (t10y > 5.0) score -= 20;
  else if (t10y > 4.5) score -= 10;
  else if (t10y < 3.5) score += 10;
  if (curve < 0) score -= 15; else if (curve > 1.0) score += 5;
  if (cpi > 4.0) score -= 10; else if (cpi < 2.5) score += 5;
  let label: string; let description: string; let buyAdj = 0;
  if (score <= 25) { label = "rate_shock"; description = "Elevated rates + inflation pressure"; buyAdj = 10; }
  else if (score <= 40) { label = "risk_off"; description = "Cautious macro environment"; buyAdj = 5; }
  else if (score <= 60) { label = "neutral"; description = "Normal macro conditions"; buyAdj = 0; }
  else if (score <= 75) { label = "risk_on"; description = "Favorable conditions for income assets"; buyAdj = -3; }
  else { label = "goldilocks"; description = "Optimal conditions: low rates, low inflation"; buyAdj = -5; }
  return { label, score: clamp(score), description, buy_threshold_adj: buyAdj };
}

function r2(v: number): number { return Math.round(v * 100) / 100; }
function r4(v: number): number { return Math.round(v * 10000) / 10000; }
function roundAll(obj: Record<string, number>): Record<string, number> { const out: Record<string, number> = {}; for (const [k, v] of Object.entries(obj)) out[k] = r2(v); return out; }

Deno.serve(async (req) => {
  const startTime = Date.now();
  const log: string[] = [];
  function addLog(msg: string) { log.push(msg); }

  const url = new URL(req.url);
  if (url.pathname === "/health" || url.pathname === "/run-signal-engine/health") {
    return new Response(JSON.stringify({ status: "healthy", version: PARAMETER_VERSION, timestamp: new Date().toISOString() }), { headers: { "Content-Type": "application/json" } });
  }

  // Auth check
  const authHeader = req.headers.get("authorization") || "";
  const cronHeader = req.headers.get("x-cron-secret") || "";
  if (CRON_SECRET && cronHeader !== CRON_SECRET && !authHeader.includes("Bearer")) {
    return new Response(JSON.stringify({ error: "Unauthorized" }), { status: 401, headers: { "Content-Type": "application/json" } });
  }

  const sbUrl = Deno.env.get("SUPABASE_URL") || "";
  const sbKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || "";
  if (!sbUrl || !sbKey) return new Response(JSON.stringify({ error: "Missing env" }), { status: 500 });
  const sb = createClient(sbUrl, sbKey);

  const today = new Date().toISOString().split("T")[0];

  async function writeLog(status: string, detail: unknown) {
    try { await sb.from("engine_run_log").insert({ run_date: today, status, engine_version: PARAMETER_VERSION, detail, log_messages: log }); } catch (_e) { /* non-fatal */ }
  }

  try {
    addLog("=== Gridiron Signal Engine " + PARAMETER_VERSION + " ===");
    addLog("Run date: " + today);

    addLog("[1/12] Loading CEF data...");
    const { data: cefZData } = await sb.from("cef_latest_with_zscores").select("ticker,trade_date,price,nav,discount_pct,yield_pct,z1y,z3y,z5y");
    const cefData: ZRow[] = cefZData || [];
    addLog("  CEF tickers loaded: " + cefData.length);
    const { data: cefTickerInfo } = await sb.from("cef_tickers").select("ticker,sector,asset_class");
    const cefSectors: Record<string, string> = {};
    for (const t of (cefTickerInfo || [])) cefSectors[t.ticker] = t.sector || "General";

    addLog("[2/12] Loading BDC data...");
    const { data: bdcZData } = await sb.from("bdc_latest_with_zscores").select("ticker,trade_date,price,nav,discount_pct,yield_pct,z1y,z3y,z5y");
    const bdcData: ZRow[] = bdcZData || [];
    addLog("  BDC tickers loaded: " + bdcData.length);
    const { data: bdcTickerInfo } = await sb.from("bdc_tickers").select("ticker,sector");
    const bdcSectors: Record<string, string> = {};
    for (const t of (bdcTickerInfo || [])) bdcSectors[t.ticker] = t.sector || "Diversified";

    addLog("[3/12] Loading MLP + REIT data...");
    const { data: eiCache } = await sb.from("equity_income_zscore_cache").select("*");
    const midstreamRows: EICacheRow[] = (eiCache || []).filter((r: any) => r.asset_class === "MIDSTREAM" || r.asset_class === "MLP" && r.price > 0);
    const reitRows: EICacheRow[] = (eiCache || []).filter((r: any) => r.asset_class === "REIT" && r.price > 0);
    addLog("  Midstream tickers: " + midstreamRows.length + ", REIT tickers: " + reitRows.length);

    addLog("  Loading REIT cap rate benchmarks...");
    const { data: reitCapRates } = await sb.from("reit_private_cap_rates").select("sector,cap_rate_mid").eq("quality_tier", "Overall").eq("market_tier", "Overall");
    const capRateMap: Record<string, number> = {};
    for (const r of (reitCapRates || [])) capRateMap[r.sector] = Number(r.cap_rate_mid);
    const { data: reitSectorCfg } = await sb.from("reit_sector_config").select("sector,assumed_payout_ratio,spread_entry_bps,spread_compelling_bps,supply_pipeline_score,supply_sensitivity");
    const payoutMap: Record<string, number> = {};
    const sectorEntryBps: Record<string, number> = {};
    const sectorCompellingBps: Record<string, number> = {};
    const sectorSupply: Record<string, number> = {};
    const sectorSupplySens: Record<string, string> = {};
    for (const r of (reitSectorCfg || [])) {
      payoutMap[r.sector] = Number(r.assumed_payout_ratio);
      sectorEntryBps[r.sector] = r.spread_entry_bps || 100;
      sectorCompellingBps[r.sector] = r.spread_compelling_bps || 200;
      sectorSupply[r.sector] = r.supply_pipeline_score || 50;
      sectorSupplySens[r.sector] = r.supply_sensitivity || "medium";
    }
    addLog("  REIT cap rates: " + Object.keys(capRateMap).length + " sectors, payout ratios: " + Object.keys(payoutMap).length);

    addLog("  Loading REIT fundamentals for L2...");
    const { data: reitFundRaw } = await sb.rpc("get_latest_reit_fundamentals");
    const reitFundMap: Record<string, any> = {};
    for (const r of (reitFundRaw || [])) reitFundMap[r.ticker] = r;
    addLog("  REIT fundamentals: " + Object.keys(reitFundMap).length);

    addLog("  Loading REIT management config...");
    const { data: reitMgmtRows } = await sb.from("reit_management_config").select("*");
    const reitMgmtMap: Record<string, any> = {};
    for (const r of (reitMgmtRows || [])) reitMgmtMap[r.ticker] = r;
    addLog("  REIT management configs: " + Object.keys(reitMgmtMap).length);

    addLog("  Loading Midstream fundamentals for L2...");
    const { data: midFundRaw } = await sb.rpc("get_latest_midstream_fundamentals");
    const midFundMap: Record<string, any> = {};
    for (const r of (midFundRaw || [])) midFundMap[r.ticker] = r;
    addLog("  Midstream fundamentals: " + Object.keys(midFundMap).length);

    addLog("  Loading Midstream management config...");
    const { data: midMgmtRows } = await sb.from("midstream_management_config").select("*");
    const midMgmtMap: Record<string, any> = {};
    for (const r of (midMgmtRows || [])) midMgmtMap[r.ticker] = r;
    addLog("  Midstream management configs: " + Object.keys(midMgmtMap).length);

    addLog("  Loading distribution summaries...");
    const { data: distSummaryRows } = await sb.from("distribution_summary").select("*");
    const distSummaryMap: Record<string, any> = {};
    for (const r of (distSummaryRows || [])) distSummaryMap[r.ticker] = r;
    addLog("  Distribution summaries: " + Object.keys(distSummaryMap).length);

    addLog("  Loading Treasury yields from macro_daily...");
    const { data: macroRows } = await sb.from("macro_daily").select("indicator,value").in("indicator", ["UST_10Y", "UST_2Y", "CPI_YOY"]).order("trade_date", { ascending: false });
    let treasury10y = 4.25; // fallback
    let treasury2y = 3.70;
    let cpiYoy = 2.5;
    for (const m of (macroRows || [])) {
      if (m.indicator === "UST_10Y" && treasury10y === 4.25) treasury10y = Number(m.value);
      if (m.indicator === "UST_2Y" && treasury2y === 3.70) treasury2y = Number(m.value);
      if (m.indicator === "CPI_YOY" && cpiYoy === 2.5) cpiYoy = Number(m.value);
    }
    addLog("  Treasury 10Y: " + treasury10y + "%, 2Y: " + treasury2y + "%, CPI YoY: " + cpiYoy + "%");

    // Marks: Cycle regime detection
    const macroMapRegime: Record<string, number> = { UST_10Y: treasury10y, UST_2Y: treasury2y, CPI_YOY: cpiYoy };
    const regime = detectRegime(macroMapRegime);
    addLog("  Market regime: " + regime.label + " (score=" + regime.score + ", " + regime.description + ")");

    addLog("  Loading quality tiers...");
    const { data: tierRows } = await sb.from("equity_income_tickers").select("ticker,quality_tier").eq("asset_class", "MIDSTREAM");
    const tierMap: Record<string, string> = {};
    for (const t of (tierRows || [])) tierMap[t.ticker] = t.quality_tier || "T3";
    addLog("  Quality tiers: " + Object.keys(tierMap).length + " tickers");

    addLog("[4/12] Loading BDC fundamentals for L2...");
    const { data: bdcFundRaw } = await sb.rpc("get_latest_bdc_fundamentals");
    const bdcFundMap: Record<string, BDCFundRow> = {};
    if (bdcFundRaw) { for (const r of bdcFundRaw) bdcFundMap[r.ticker] = r; }
    else { const { data: niiRows } = await sb.from("bdc_nii_coverage").select("ticker,nii_coverage_ratio,debt_to_equity,nav_per_share").order("report_date", { ascending: false }); const seen = new Set<string>(); for (const r of (niiRows || [])) { if (!seen.has(r.ticker)) { seen.add(r.ticker); bdcFundMap[r.ticker] = r; } } }
    addLog("  BDC fundamentals loaded: " + Object.keys(bdcFundMap).length);

    addLog("[5/12] Loading BDC NAV trajectory...");
    const { data: navRows } = await sb.from("bdc_quarterly_nav").select("ticker,report_date,nav_per_share").order("report_date", { ascending: false });
    const bdcNavMap: Record<string, BDCNavRow[]> = {};
    for (const r of (navRows || [])) { if (!bdcNavMap[r.ticker]) bdcNavMap[r.ticker] = []; if (bdcNavMap[r.ticker].length < 5) bdcNavMap[r.ticker].push(r); }
    addLog("  BDC NAV history: " + Object.keys(bdcNavMap).length);

    addLog("[6/12] Loading BDC holdings for L4...");
    const { data: holdRows } = await sb.from("bdc_holdings_summary").select("ticker,report_date,total_holdings,pct_first_lien,pct_second_lien,pct_equity,non_accrual_count,non_accrual_pct,pik_pct,sector_count,max_single_position_pct").order("report_date", { ascending: false });
    const bdcHoldMap: Record<string, BDCHoldRow> = {}; const holdSeen = new Set<string>();
    for (const r of (holdRows || [])) { if (!holdSeen.has(r.ticker)) { holdSeen.add(r.ticker); bdcHoldMap[r.ticker] = r; } }
    addLog("  BDC holdings: " + Object.keys(bdcHoldMap).length);

    addLog("[7/12] Loading CEF fund characteristics for L2...");
    const { data: cefFundRows } = await sb.from("fund_characteristics").select("ticker,leverage_pct,distribution_yield,effective_duration");
    const cefFundMap: Record<string, CEFFundRow> = {};
    for (const r of (cefFundRows || [])) cefFundMap[r.ticker] = r;
    addLog("  CEF fund characteristics: " + Object.keys(cefFundMap).length);

    addLog("  Loading CEF N-PORT data for L4...");
    const { data: nportRows } = await sb.from("cef_nport_data").select("ticker,effective_duration,aaa_pct,aa_pct,a_pct,bbb_pct,below_ig_pct,not_rated_pct,top_10_pct,top_holding_pct,total_holdings").order("filing_date", { ascending: false });
    const nportMap: Record<string, any> = {}; const nportSeen = new Set<string>();
    for (const r of (nportRows || [])) { if (!nportSeen.has(r.ticker)) { nportSeen.add(r.ticker); nportMap[r.ticker] = r; } }
    addLog("  N-PORT data: " + Object.keys(nportMap).length);

    addLog("  Loading CEF fund info for L3...");
    const { data: fundInfoRows } = await sb.from("cef_fund_info").select("ticker,adviser_name,mgmt_fee_pct,total_expense_pct,inception_date,is_term_fund,total_assets");
    const fundInfoMap: Record<string, any> = {};
    for (const r of (fundInfoRows || [])) fundInfoMap[r.ticker] = r;
    addLog("  CEF fund info: " + Object.keys(fundInfoMap).length);

    addLog("[8/12] Computing momentum indicators...");
    const { data: cefMom, error: cefMomErr } = await sb.rpc("compute_momentum_indicators", { p_table: "cef_daily_clean", p_lookback: 90 });
    if (cefMomErr) addLog("  CEF momentum error: " + cefMomErr.message);
    const cefMomMap: Record<string, MomRow> = {}; for (const m of (cefMom || [])) cefMomMap[m.ticker] = m;
    addLog("  CEF momentum: " + Object.keys(cefMomMap).length);
    let bdcMomMap: Record<string, MomRow> = {};
    if (bdcData.length > 0) { const { data: bdcMom } = await sb.rpc("compute_momentum_indicators", { p_table: "bdc_daily_clean", p_lookback: 90 }); for (const m of (bdcMom || [])) bdcMomMap[m.ticker] = m; addLog("  BDC momentum: " + Object.keys(bdcMomMap).length); }

    addLog("  Computing 30-day volatility...");
    const { data: cefVol } = await sb.rpc("compute_volatility_30d", { p_table: "cef_daily_clean", p_lookback: 35 });
    const cefVolMap: Record<string, number> = {}; for (const v of (cefVol || [])) { if (v.volatility_30d !== null) cefVolMap[v.ticker] = Number(v.volatility_30d); }
    let bdcVolMap: Record<string, number> = {};
    if (bdcData.length > 0) { const { data: bdcVol } = await sb.rpc("compute_volatility_30d", { p_table: "bdc_daily_clean", p_lookback: 35 }); for (const v of (bdcVol || [])) { if (v.volatility_30d !== null) bdcVolMap[v.ticker] = Number(v.volatility_30d); } }

    addLog("[9/12] Computing peer group statistics...");
    const { data: cefPeers } = await sb.rpc("compute_peer_stats", { p_asset_class: "CEF" });
    const cefPeerMap: Record<string, PeerRow> = {}; for (const p of (cefPeers || [])) cefPeerMap[p.sector] = p;
    let bdcPeerMap: Record<string, PeerRow> = {};
    if (bdcData.length > 0) { const { data: bdcPeers } = await sb.rpc("compute_peer_stats", { p_asset_class: "BDC" }); for (const p of (bdcPeers || [])) bdcPeerMap[p.sector] = p; }

    addLog("  Loading risk veto data...");
    let cefVetoData: Record<string, any> = { leverage: {}, distribution_cuts: {}, low_volume: {} };
    let bdcVetoData: Record<string, any> = { leverage: {}, distribution_cuts: {}, low_volume: {} };
    try { const { data: cefVD } = await sb.rpc("get_veto_data", { asset_class_filter: "CEF" }); if (cefVD) cefVetoData = cefVD; } catch (_e) { /* non-fatal */ }
    if (bdcData.length > 0) { try { const { data: bdcVD } = await sb.rpc("get_veto_data", { asset_class_filter: "BDC" }); if (bdcVD) bdcVetoData = bdcVD; } catch (_e) { /* non-fatal */ } }

    addLog("[10/12] Running multi-layer scoring...");
    const signals: Record<string, unknown>[] = [];
    const signalCounts: Record<string, number> = {};
    let vetoedCount = 0, bdcL2Active = 0, bdcL4Active = 0, cefL2Active = 0, cefL3Active = 0, cefL4Active = 0;

    for (const row of cefData) {
      if (!row.price || !row.nav || row.price <= 0 || row.nav <= 0) continue;
      const discount = row.discount_pct ?? ((row.price - row.nav) / row.nav);
      const sector = cefSectors[row.ticker] || "General";
      const peer = cefPeerMap[sector] || { peer_median_discount: -0.05, peer_avg_yield: 0, peer_count: 0 };
      const mom = cefMomMap[row.ticker] || { momentum_60d: 0, discount_slope_30d: 0, rsi_14: 50, volume_ratio: 1, current_yield: 0 };
      const peerZ1yValues = cefData.filter(r => cefSectors[r.ticker] === sector && r.z1y != null).map(r => r.z1y as number);
      const percentile5yr = peerZ1yValues.length > 0 ? (peerZ1yValues.filter(v => v < (row.z1y ?? 0)).length / peerZ1yValues.length) * 100 : 50;
      const yieldDecimal = mom.current_yield || (row.yield_pct ? Number(row.yield_pct) : 0);
      const yieldPct = yieldDecimal * 100; const peerAvgYieldPct = (peer.peer_avg_yield || 0) * 100;
      const vol30d = cefVolMap[row.ticker] ?? DEFAULT_VOL;
      const dp = scoreDiscountPremium(row.z1y ?? null, row.z3y ?? null, row.z5y ?? null, discount, peer.peer_median_discount || 0, percentile5yr);
      const mt = scoreMomentum(mom.momentum_60d, mom.discount_slope_30d, mom.volume_ratio, mom.rsi_14);
      const yv = scoreYieldVol(yieldPct, peerAvgYieldPct, vol30d);
      const l1 = scoreL1(dp, mt, yv);
      const l2 = scoreCEFL2Enhanced(cefFundMap[row.ticker] || null, fundInfoMap[row.ticker] || null, distSummaryMap[row.ticker] || null, peer.peer_avg_yield || 0); if (l2.has_data) cefL2Active++;
      const l3 = scoreCEFL3(fundInfoMap[row.ticker] || null);
      const l4 = scoreCEFL4(nportMap[row.ticker] || null, sector);
      if (l3.has_data) cefL3Active++; if (l4.has_data) cefL4Active++;
      const l6 = scoreL6("CEF");
      const allLayers: Record<string, LayerScore> = { L1: l1, L2: l2, L3: l3, L4: l4, L6: l6 };
      const { composite, layers } = computeComposite(allLayers, CEF_LAYER_WEIGHTS);
      const vetoes = buildCEFVetoes(row.ticker, cefVetoData); if (vetoes.length > 0) vetoedCount++;
      let { signal, adjustedScore, riskPenalty } = determineSignal(composite, vetoes, mom.momentum_60d, mom.discount_slope_30d, mom.volume_ratio, "CEF");
      // Marks regime gate: tighten BUY on duration-sensitive sectors
      const isDurationSensitive = sector.includes("Muni") || sector.includes("Municipal") || sector.includes("Taxable") || sector.includes("Corp") || sector.includes("Mortgage") || sector.includes("Preferred") || sector.includes("Bond") || sector.includes("Multi-Sector") || (nportMap[row.ticker]?.effective_duration != null && Number(nportMap[row.ticker].effective_duration) > 1.5);
      if (isDurationSensitive && regime.buy_threshold_adj > 0) {
        if ((signal === "BUY" || signal === "STRONG BUY") && adjustedScore < (BUY + regime.buy_threshold_adj)) signal = "ACCUMULATE";
        if (signal === "STRONG BUY" && adjustedScore < (STRONG_BUY + regime.buy_threshold_adj)) signal = "BUY";
      }
      const confidence = determineConfidence(layers, vetoes);
      const conviction = buildConviction(row.ticker, signal, composite, discount, row.z1y ?? null, yieldPct, mom.momentum_60d, vetoes, layers);
      signalCounts[signal] = (signalCounts[signal] || 0) + 1;
      const cefSigRow = buildSignalRow(row, "CEF", sector, today, signal, composite, adjustedScore, confidence, discount, mom, yieldPct, vol30d, peer, layers, vetoes, riskPenalty, conviction);
      const cefKm = cefSigRow.key_metrics as Record<string, unknown>;
      const cefFI = fundInfoMap[row.ticker]; const cefNP = nportMap[row.ticker]; const cefDS = distSummaryMap[row.ticker];
      if (cefFI?.total_expense_pct != null) cefKm.total_expense_pct = Number(cefFI.total_expense_pct);
      if (cefFI?.adviser_name) cefKm.adviser_name = cefFI.adviser_name;
      if (cefDS?.consecutive_growth_years != null) cefKm.consecutive_growth_years = Number(cefDS.consecutive_growth_years);
      if (cefDS?.cut_count_5yr != null) cefKm.cut_count_5yr = Number(cefDS.cut_count_5yr);
      if (cefNP?.effective_duration != null) cefKm.effective_duration = Number(cefNP.effective_duration);
      if (cefNP?.top_10_pct != null) cefKm.top_10_pct = Number(cefNP.top_10_pct);
      if (cefNP?.total_holdings != null) cefKm.total_holdings = Number(cefNP.total_holdings);
      const cefIg = (Number(cefNP?.aaa_pct || 0)) + (Number(cefNP?.aa_pct || 0)) + (Number(cefNP?.a_pct || 0)) + (Number(cefNP?.bbb_pct || 0));
      if (cefIg > 0) cefKm.ig_pct = r2(cefIg);
      const cefFC = cefFundMap[row.ticker]; if (cefFC?.leverage_pct != null) cefKm.leverage_pct = Number(cefFC.leverage_pct);
      const cefPos = getPositionGuidance(signal); cefKm.position_size = cefPos.position_size; cefKm.position_pct = cefPos.position_pct; cefKm.market_regime = regime.label; cefKm.regime_score = regime.score; cefKm.is_duration_sensitive = isDurationSensitive;
      signals.push(cefSigRow);
    }
    addLog("  CEFs scored: " + signals.filter(s => (s as any).asset_class === "CEF").length + " (L2: " + cefL2Active + ", L3: " + cefL3Active + ", L4: " + cefL4Active + ")");

    for (const row of bdcData) {
      if (!row.price || !row.nav || row.price <= 0 || row.nav <= 0) continue;
      const discount = row.discount_pct ?? ((row.price - row.nav) / row.nav);
      const sector = bdcSectors[row.ticker] || "Diversified";
      const peer = bdcPeerMap[sector] || { peer_median_discount: -0.05, peer_avg_yield: 0, peer_count: 0 };
      const mom = bdcMomMap[row.ticker] || { momentum_60d: 0, discount_slope_30d: 0, rsi_14: 50, volume_ratio: 1, current_yield: 0 };
      const peerZ1yValues = bdcData.filter(r => bdcSectors[r.ticker] === sector && r.z1y != null).map(r => r.z1y as number);
      const percentile5yr = peerZ1yValues.length > 0 ? (peerZ1yValues.filter(v => v < (row.z1y ?? 0)).length / peerZ1yValues.length) * 100 : 50;
      const yieldDecimal = mom.current_yield || (row.yield_pct ? Number(row.yield_pct) : 0);
      const yieldPct = yieldDecimal * 100; const peerAvgYieldPct = (peer.peer_avg_yield || 0) * 100;
      const vol30d = bdcVolMap[row.ticker] ?? DEFAULT_VOL;
      const dp = scoreDiscountPremium(row.z1y ?? null, row.z3y ?? null, row.z5y ?? null, discount, peer.peer_median_discount || 0, percentile5yr);
      const mt = scoreMomentum(mom.momentum_60d, mom.discount_slope_30d, mom.volume_ratio, mom.rsi_14);
      const yv = scoreYieldVol(yieldPct, peerAvgYieldPct, vol30d);
      const l1 = scoreL1(dp, mt, yv);
      const l2 = scoreBDCL2(bdcFundMap[row.ticker] || null, bdcNavMap[row.ticker] || []); if (l2.has_data) bdcL2Active++;
      const l3 = scoreL3("BDC");
      const l4 = scoreBDCL4(bdcHoldMap[row.ticker] || null); if (l4.has_data) bdcL4Active++;
      const l6 = scoreL6("BDC");
      const allLayers: Record<string, LayerScore> = { L1: l1, L2: l2, L3: l3, L4: l4, L6: l6 };
      const { composite, layers } = computeComposite(allLayers, BDC_LAYER_WEIGHTS);
      const vetoes = buildBDCVetoes(row.ticker, bdcVetoData); if (vetoes.length > 0) vetoedCount++;
      let { signal, adjustedScore, riskPenalty } = determineSignal(composite, vetoes, mom.momentum_60d, mom.discount_slope_30d, mom.volume_ratio, "BDC");
      // Marks: tighten BDC momentum gate in risk-off
      if ((regime.label === "rate_shock" || regime.label === "risk_off") && (signal === "BUY" || signal === "STRONG BUY") && !checkMomentumConfirmation(mom.momentum_60d, mom.discount_slope_30d, mom.volume_ratio)) signal = "ACCUMULATE";
      const confidence = determineConfidence(layers, vetoes);
      const conviction = buildConviction(row.ticker, signal, composite, discount, row.z1y ?? null, yieldPct, mom.momentum_60d, vetoes, layers);
      signalCounts[signal] = (signalCounts[signal] || 0) + 1;
      const bdcSR = buildSignalRow(row, "BDC", sector, today, signal, composite, adjustedScore, confidence, discount, mom, yieldPct, vol30d, peer, layers, vetoes, riskPenalty, conviction);
      const bdcPos = getPositionGuidance(signal); (bdcSR.key_metrics as Record<string, unknown>).position_size = bdcPos.position_size; (bdcSR.key_metrics as Record<string, unknown>).position_pct = bdcPos.position_pct; (bdcSR.key_metrics as Record<string, unknown>).market_regime = regime.label;
      (bdcSR.key_metrics as Record<string, unknown>).regime_score = regime.score;
      signals.push(bdcSR);
    }
    addLog("  BDCs scored: " + signals.filter(s => (s as any).asset_class === "BDC").length + " (L2: " + bdcL2Active + ", L4: " + bdcL4Active + ")");

    let midstreamCount = 0, reitCount = 0, reitL2Active = 0, reitHardStopped = 0, reitL2FundActive = 0, midL2FundActive = 0, midL3Active = 0, midL4Active = 0, reitL3Active = 0, reitL4Active = 0;
    for (const row of midstreamRows) {
      const l1 = scoreEIL1(row);
      // Quality-adjusted spread: yield vs treasury + tier premium
      const tier = tierMap[row.ticker] || "T3";
      const tierPrem = MIDSTREAM_TIER_PREMIUM[tier] || 0.0375;
      const yieldDecimal = row.yield_pct ? Number(row.yield_pct) : 0;
      const requiredYield = treasury10y / 100 + tierPrem;
      const qualitySpread = yieldDecimal - requiredYield;
      // Boost/penalize L1 based on spread: +/-10 pts per 1% spread
      const spreadAdj = clamp(l1.raw_score + qualitySpread * 1000, 0, 100);
      l1.raw_score = r2(spreadAdj);
      l1.components.quality_spread = r2(qualitySpread * 100);
      l1.components.required_yield = r2(requiredYield * 100);
      l1.components.tier = tier === "T1" ? 1 : tier === "T2" ? 2 : 3;
      const l2 = scoreMidstreamL2(midFundMap[row.ticker] || null, tier); if (l2.has_data) midL2FundActive++;
      const l3 = scoreMidstreamL3(midMgmtMap[row.ticker] || null);
      const l4 = scoreMidstreamL4(midMgmtMap[row.ticker] || null);
      if (l3.has_data) midL3Active++; if (l4.has_data) midL4Active++;
      const l6 = scoreL6("MIDSTREAM");
      const allLayers: Record<string, LayerScore> = { L1: l1, L2: l2, L3: l3, L4: l4, L6: l6 };
      const { composite, layers } = computeComposite(allLayers, MIDSTREAM_LAYER_WEIGHTS);
      const vetoes = buildEIVetoes(row, distSummaryMap); if (vetoes.length > 0) vetoedCount++;
      const mom60d = Number(row.momentum_60d) || 0; const slope30d = Number(row.discount_slope_30d) || 0; const volRatio = Number(row.volume_ratio) || 1;
      const { signal, adjustedScore, riskPenalty } = determineSignal(composite, vetoes, mom60d, slope30d, volRatio, "MIDSTREAM");
      const confidence = determineConfidence(layers, vetoes);
      const yieldPct = yieldDecimal * 100;
      const conviction = buildConviction(row.ticker, signal, composite, 0, null, yieldPct, mom60d, vetoes, layers);
      signalCounts[signal] = (signalCounts[signal] || 0) + 1;
      // Add treasury spread data to key_metrics via custom signal row
      const sigRow = buildEISignalRow(row, "MIDSTREAM", today, signal, composite, adjustedScore, confidence, layers, vetoes, riskPenalty, conviction);
      const km = sigRow.key_metrics as Record<string, unknown>;
      km.treasury_10y = treasury10y;
      km.quality_adjusted_spread = r2(qualitySpread * 100);
      km.tier_premium = r2(tierPrem * 100);
      km.quality_tier = tier;
      const midPos = getPositionGuidance(signal); km.position_size = midPos.position_size; km.position_pct = midPos.position_pct; km.required_yield = r2(requiredYield * 100); km.market_regime = regime.label; km.regime_score = regime.score;
      const mFund = midFundMap[row.ticker];
      if (mFund?.debt_to_ebitda != null) km.debt_to_ebitda = Number(mFund.debt_to_ebitda);
      if (mFund?.dcf_coverage_ratio != null) km.dcf_coverage_ratio = Number(mFund.dcf_coverage_ratio);
      if (mFund?.consecutive_growth_quarters != null) km.consecutive_growth_years = Number(mFund.consecutive_growth_quarters) / 4;
      if (mFund?.distribution_cut_flag) km.distribution_cut_flag = true;
      const mDist = distSummaryMap[row.ticker];
      if (mDist?.consecutive_growth_years != null) km.consecutive_growth_years = mDist.consecutive_growth_years;
      if (mDist?.last_cut_date) km.last_cut_date = mDist.last_cut_date;
      if (mDist?.days_since_cut != null) km.days_since_cut = mDist.days_since_cut;
      if (mDist?.cut_count_5yr != null) km.cut_count_5yr = mDist.cut_count_5yr;
      signals.push(sigRow);
      midstreamCount++;
    }
    addLog("  Midstream scored: " + midstreamCount + " (L2: " + midL2FundActive + ", L3: " + midL3Active + ", L4: " + midL4Active + ")");

    for (const row of reitRows) {
      const l1 = scoreEIL1(row); const sector = row.sector || "Diversified";
      const l2 = scoreREITL2(row, payoutMap[sector], capRateMap[sector], reitFundMap[row.ticker] || null); if (l2.has_data) reitL2Active++; if (reitFundMap[row.ticker]?.debt_to_ebitda != null) reitL2FundActive++;
      const l3 = scoreREITL3(reitMgmtMap[row.ticker] || null);
      const l4 = scoreREITL4(reitMgmtMap[row.ticker] || null);
      if (l3.has_data) reitL3Active++;
      if (l4.has_data) reitL4Active++;
      const l6 = scoreL6("REIT");
      const allLayers: Record<string, LayerScore> = { L1: l1, L2: l2, L3: l3, L4: l4, L6: l6 };
      const { composite, layers } = computeComposite(allLayers, REIT_LAYER_WEIGHTS);
      const vetoes = buildEIVetoes(row); if (vetoes.length > 0) vetoedCount++;
      if (vetoes.some(v => v.name === "sector_hard_stop")) reitHardStopped++;
      const mom60d = Number(row.momentum_60d) || 0; const slope30d = Number(row.discount_slope_30d) || 0; const volRatio = Number(row.volume_ratio) || 1;
      let { signal, adjustedScore, riskPenalty } = determineSignal(composite, vetoes, mom60d, slope30d, volRatio, "REIT");
      // Zell: sector-specific spread gating
      const entryBps = sectorEntryBps[sector] || 100;
      const compellingBps = sectorCompellingBps[sector] || 200;
      const capSpreadBps = (layers.L2?.components?.cap_rate_spread || 0) * 100;
      const supplyScore = sectorSupply[sector] || 50;
      const supplySens = sectorSupplySens[sector] || "medium";
      let supplyGated = false;
      if (supplySens === "high" && supplyScore < 35) supplyGated = true;
      else if (supplySens === "medium" && supplyScore < 25) supplyGated = true;
      // Clamp BUY signals if sector spread insufficient
      if (capSpreadBps < entryBps && (signal === "BUY" || signal === "STRONG BUY")) signal = "HOLD";
      // Clamp if supply pipeline is glutted (Zell: cheap + supply glut = value trap)
      if (supplyGated && (signal === "BUY" || signal === "STRONG BUY")) signal = "HOLD";
      const confidence = determineConfidence(layers, vetoes);
      const yieldPct = row.yield_pct ? Number(row.yield_pct) * 100 : 0;
      const conviction = buildConviction(row.ticker, signal, composite, 0, null, yieldPct, mom60d, vetoes, layers);
      if (capSpreadBps > compellingBps) conviction.bull_case += ". Compelling sector spread: " + capSpreadBps.toFixed(0) + "bp vs " + compellingBps + "bp threshold";
      signalCounts[signal] = (signalCounts[signal] || 0) + 1;
      const sigRow = buildEISignalRow(row, "REIT", today, signal, composite, adjustedScore, confidence, layers, vetoes, riskPenalty, conviction);
      const km = sigRow.key_metrics as Record<string, unknown>;
      km.sector_spread_entry_bps = entryBps;
      km.sector_spread_compelling_bps = compellingBps;
      km.cap_rate_spread_bps = r2(capSpreadBps);
      km.supply_pipeline_score = supplyScore;
      const reitPos = getPositionGuidance(signal); km.position_size = reitPos.position_size; km.position_pct = reitPos.position_pct; km.supply_gated = supplyGated; km.market_regime = regime.label; km.regime_score = regime.score;
      const rFund = reitFundMap[row.ticker];
      if (rFund?.debt_to_ebitda != null) km.debt_to_ebitda = Number(rFund.debt_to_ebitda);
      if (rFund?.ffo_payout_ratio != null) km.ffo_payout_ratio = Number(rFund.ffo_payout_ratio);
      const rDist = distSummaryMap[row.ticker];
      if (rDist?.consecutive_growth_years != null) km.consecutive_growth_years = rDist.consecutive_growth_years;
      if (rDist?.last_cut_date) km.last_cut_date = rDist.last_cut_date;
      if (rDist?.days_since_cut != null) km.days_since_cut = rDist.days_since_cut;
      signals.push(sigRow);
      reitCount++;
    }
    addLog("  REITs scored: " + reitCount + " (L2 cap rate: " + reitL2Active + ", hard-stopped: " + reitHardStopped + ")");

    addLog("  Signal distribution: " + JSON.stringify(signalCounts));

    addLog("[11/12] Writing to signal_log...");
    let inserted = 0, errors = 0;
    for (let i = 0; i < signals.length; i += 50) {
      const chunk = signals.slice(i, i + 50);
      const { error: upsertErr } = await sb.from("signal_log").upsert(chunk as any[], { onConflict: "ticker,signal_date", ignoreDuplicates: false });
      if (upsertErr) { addLog("  Batch error: " + upsertErr.message); for (const sig of chunk) { const { error: singleErr } = await sb.from("signal_log").upsert([sig] as any[], { onConflict: "ticker,signal_date", ignoreDuplicates: false }); if (singleErr) errors++; else inserted++; } }
      else inserted += chunk.length;
    }
    addLog("  Inserted/updated: " + inserted + ", errors: " + errors);

    addLog("  Updating outcome_tracker...");
    let outcomesUpdated = 0;
    const { data: pendingOutcomes } = await sb.from("outcome_tracker").select("id,signal_log_id,ticker,signal_date,return_30d,return_60d,return_90d,return_180d").eq("all_horizons_filled", false).is("signal_source", null).limit(500);
    if (pendingOutcomes && pendingOutcomes.length > 0) {
      for (const oc of pendingOutcomes) {
        const signalDate = new Date(oc.signal_date);
        const daysSince = Math.floor((Date.now() - signalDate.getTime()) / 86400000);
        const updates: Record<string, unknown> = {};
        let horizonsFilled = 0;
        for (const h of HORIZONS) {
          const field = "return_" + h + "d"; const priceField = "price_" + h + "d";
          if (oc[field] !== null) { horizonsFilled++; continue; }
          if (daysSince < h) continue;
          const horizonDate = new Date(signalDate); horizonDate.setDate(horizonDate.getDate() + h);
          const hDateStr = horizonDate.toISOString().split("T")[0];
          let { data: hPrice } = await sb.from("cef_daily").select("price").eq("ticker", oc.ticker).lte("trade_date", hDateStr).order("trade_date", { ascending: false }).limit(1);
          if (!hPrice || hPrice.length === 0) { const { data: bPrice } = await sb.from("bdc_daily").select("price").eq("ticker", oc.ticker).lte("trade_date", hDateStr).order("trade_date", { ascending: false }).limit(1); hPrice = bPrice; }
          if (!hPrice || hPrice.length === 0) { const { data: ePrice } = await sb.from("equity_income_daily").select("price").eq("ticker", oc.ticker).lte("trade_date", hDateStr).order("trade_date", { ascending: false }).limit(1); hPrice = ePrice; }
          if (hPrice && hPrice.length > 0) {
            const { data: sigRow } = await sb.from("signal_log").select("price_at_signal").eq("id", oc.signal_log_id).single();
            if (sigRow && sigRow.price_at_signal > 0) { const ret = ((hPrice[0].price - sigRow.price_at_signal) / sigRow.price_at_signal) * 100; updates[field] = r2(ret); updates[priceField] = hPrice[0].price; horizonsFilled++; }
          }
        }
        for (const h of HORIZONS) { if (oc["return_" + h + "d"] !== null) horizonsFilled++; }
        if (Object.keys(updates).length > 0) { updates.horizons_complete = horizonsFilled; updates.all_horizons_filled = horizonsFilled >= HORIZONS.length; updates.last_updated = new Date().toISOString(); await sb.from("outcome_tracker").update(updates).eq("id", oc.id); outcomesUpdated++; }
      }
    }
    const { data: todaySignals } = await sb.from("signal_log").select("id,ticker,signal_date").eq("signal_date", signals[0] ? (signals[0] as any).signal_date : today);
    if (todaySignals && todaySignals.length > 0) {
      const existingOutcomes = await sb.from("outcome_tracker").select("signal_log_id").in("signal_log_id", todaySignals.map(s => s.id));
      const existingIds = new Set((existingOutcomes.data || []).map(o => o.signal_log_id));
      const newOcRows = todaySignals.filter(s => !existingIds.has(s.id)).map(s => ({ signal_log_id: s.id, ticker: s.ticker, signal_date: s.signal_date }));
      if (newOcRows.length > 0) { for (let i = 0; i < newOcRows.length; i += 50) { await sb.from("outcome_tracker").insert(newOcRows.slice(i, i + 50)); } addLog("  New outcome_tracker rows: " + newOcRows.length); }
    }
    addLog("  Outcomes updated: " + outcomesUpdated);

    // Log regime to market_regime_log
    try { await sb.from("market_regime_log").upsert({ as_of_date: today, regime_label: regime.label, regime_score: regime.score, indicators: { ust_10y: treasury10y, ust_2y: treasury2y, cpi_yoy: cpiYoy, curve: treasury10y - treasury2y }, detected_at: new Date().toISOString() }, { onConflict: "as_of_date" }); } catch (_e) { /* non-fatal */ }

    addLog("[12/12] Uploading to storage bucket...");
    const storagePayload = { generated_at: new Date().toISOString(), parameter_version: PARAMETER_VERSION, all_signals: signals, signal_counts: signalCounts };
    const blob = new Blob([JSON.stringify(storagePayload)], { type: "application/json" });
    const { error: storageErr } = await sb.storage.from("engine-output").upload("latest_signals.json", blob, { upsert: true, contentType: "application/json" });
    if (storageErr) addLog("  Storage upload error: " + storageErr.message);
    else addLog("  Storage bucket updated: latest_signals.json");

    const summary = { status: "success", version: PARAMETER_VERSION, date: today, cef_count: signals.filter(s => (s as any).asset_class === "CEF").length, bdc_count: signals.filter(s => (s as any).asset_class === "BDC").length, midstream_count: signals.filter(s => (s as any).asset_class === "MIDSTREAM").length, reit_count: signals.filter(s => (s as any).asset_class === "REIT").length, total_signals: signals.length, signal_distribution: signalCounts, signals_written: inserted, bdc_l2_active: bdcL2Active, bdc_l4_active: bdcL4Active, cef_l2_active: cefL2Active, cef_l3_active: cefL3Active, cef_l4_active: cefL4Active, reit_l2_active: reitL2Active, reit_l2_fund: reitL2FundActive, reit_l3_active: reitL3Active, reit_l4_active: reitL4Active, mid_l2_fund: midL2FundActive, mid_l3_active: midL3Active, mid_l4_active: midL4Active, reit_hard_stopped: reitHardStopped, vetoed_tickers: vetoedCount, outcomes_updated: outcomesUpdated, errors, market_regime: regime.label, regime_score: regime.score, elapsed_ms: Date.now() - startTime };
    addLog("Done in " + summary.elapsed_ms + "ms. " + summary.total_signals + " signals.");
    // Telegram daily summary
    const actionableTg = (signalCounts["STRONG BUY"] || 0) + (signalCounts["BUY"] || 0) + (signalCounts["ACCUMULATE"] || 0);
    let tgMsg = "\ud83d\udcca <b>Gridiron Engine " + PARAMETER_VERSION + "</b>\n";
    tgMsg += "\ud83d\udcc5 " + today + " \u00b7 " + summary.total_signals + " signals \u00b7 " + summary.elapsed_ms + "ms\n\n";
    tgMsg += "\ud83d\udfe2 " + actionableTg + " actionable";
    if (signalCounts["STRONG BUY"]) tgMsg += " (" + signalCounts["STRONG BUY"] + " SB)";
    if (signalCounts["BUY"]) tgMsg += " (" + signalCounts["BUY"] + " Buy)";
    if (signalCounts["ACCUMULATE"]) tgMsg += " (" + signalCounts["ACCUMULATE"] + " Acc)";
    tgMsg += "\n\ud83d\udd34 " + ((signalCounts["SELL"] || 0) + (signalCounts["STRONG SELL"] || 0)) + " sell";
    tgMsg += " \u00b7 \u26d4 " + (signalCounts["AVOID"] || 0) + " avoid";
    tgMsg += "\n\nCEF:" + summary.cef_count + " BDC:" + summary.bdc_count + " MID:" + (summary.midstream_count || 0) + " REIT:" + summary.reit_count;
    tgMsg += "\nRegime: " + regime.label + " (" + regime.score + ")";
    if (summary.errors > 0) tgMsg += "\n\u26a0\ufe0f " + summary.errors + " errors";
    await sendBoth(tgMsg);
    await writeLog("success", summary);
    return new Response(JSON.stringify({ ...summary, log }, null, 2), { headers: { "Content-Type": "application/json" } });
  } catch (e) {
    const err = e as Error;
    addLog("FATAL: " + err.message);
    try { await sendBoth("\u274c <b>Engine Error</b>\nrun-signal-engine: " + err.message); } catch (_te) {}
    await writeLog("error", { error: err.message, stack: err.stack });
    return new Response(JSON.stringify({ status: "error", error: err.message, log }), { status: 500, headers: { "Content-Type": "application/json" } });
  }
});

function getPositionGuidance(signal: string): { position_size: string; position_pct: number } {
  if (signal === "STRONG BUY") return { position_size: "Full position", position_pct: 100 };
  if (signal === "BUY") return { position_size: "Standard position", position_pct: 75 };
  if (signal === "ACCUMULATE") return { position_size: "Quarter position - add on momentum confirmation", position_pct: 25 };
  if (signal === "HOLD") return { position_size: "Hold current - no action", position_pct: 0 };
  if (signal === "SELL") return { position_size: "Reduce position", position_pct: -50 };
  if (signal === "STRONG SELL") return { position_size: "Exit position", position_pct: -100 };
  if (signal === "AVOID") return { position_size: "Do not hold - fundamental risk", position_pct: -100 };
  return { position_size: "No action", position_pct: 0 };
}

function buildSignalRow(row: ZRow, assetClass: string, sector: string, today: string, signal: string, composite: number, adjustedScore: number, confidence: string, discount: number, mom: MomRow, yieldPct: number, vol30d: number, peer: PeerRow, layers: Record<string, LayerScore>, vetoes: VetoTrigger[], riskPenalty: number, conviction: { bull_case: string; bear_case: string; key_assumptions: string[]; invalidation_triggers: string[] }): Record<string, unknown> {
  const layerScoresObj: Record<string, unknown> = {};
  for (const [key, ls] of Object.entries(layers)) {
    layerScoresObj[key] = { raw_score: ls.raw_score, weight: ls.weight, weighted_score: ls.weighted_score, has_data: ls.has_data, components: ls.components, ...(ls.notes?.length ? { notes: ls.notes } : {}) };
  }
  return {
    ticker: row.ticker, asset_class: assetClass, sector, signal_date: row.trade_date || today,
    signal, composite_score: composite, adjusted_score: adjustedScore, confidence,
    price_at_signal: row.price, nav_at_signal: row.nav, discount_at_signal: discount,
    layer_scores: layerScoresObj,
    veto_triggers: vetoes.length > 0 ? vetoes.map(v => ({ name: v.name, severity: v.severity, penalty: v.penalty, description: v.description, data_point: v.data_point })) : [],
    risk_penalties: riskPenalty,
    key_metrics: { price: row.price, nav: row.nav, discount_pct: r4(discount), z1y: row.z1y, z3y: row.z3y, z5y: row.z5y, momentum_60d: mom.momentum_60d, rsi_14: mom.rsi_14, yield_pct: yieldPct, volatility_30d: vol30d, volume_ratio: mom.volume_ratio, peer_median_discount: peer.peer_median_discount, peer_avg_yield: (peer.peer_avg_yield || 0) * 100 },
    bull_case: conviction.bull_case, bear_case: conviction.bear_case, key_assumptions: conviction.key_assumptions, invalidation_triggers: conviction.invalidation_triggers,
    parameter_version: PARAMETER_VERSION, generated_at: new Date().toISOString(),
  };
}
