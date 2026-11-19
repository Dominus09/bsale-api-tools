/**
 * Política de selección Offline V1 — un despacho prioritario por ciclo.
 * Sin imports de API (testeable con node --experimental-strip-types).
 * No altera estados ni fechas. No asume que status del plan = picking listo.
 */

import {
  bucketPlanDate,
  partitionPlansByDate,
  santiagoTodayYmd,
  type PlanDateBucket,
} from "./plan-buckets"

/** Máximo de probes de progreso/picking por ciclo (evita N+1 sobre 50). */
export const MAX_PROGRESS_PROBES = 6
export const MAX_TODAY_IN_PROBES = 3
export const MAX_HISTORY_IN_PROBES = 3

/**
 * Retención (documentada; no borrar automáticamente en este cambio):
 * - Mantener snapshots de planes con gestiones pendientes.
 * - Candidatos a purga futura: complete + remaining=0 + >14 días + no active.
 * - Logout sigue limpiando todo el namespace del userId.
 */
export const OFFLINE_RETENTION_POLICY = {
  autoDelete: false as const,
  proposePurgeDays: 14,
  keepPendingAlways: true as const,
  note:
    "No borrar IDB automáticamente en Offline V1 smart-sync; proponer purga en cambio aparte.",
}

export type PriorityTier = "A" | "B" | "C" | "D" | "none"

export type PlanOpsSignals = {
  pickingReady: boolean
  remainingClients: number
  inProgress: number
}

export type PriorityPlanLike = {
  id: number
  planning_date?: string | null
  document_count?: number | null
  planning_code?: string | null
  planning_name?: string | null
}

export type PriorityDecision<T extends PriorityPlanLike = PriorityPlanLike> = {
  tier: PriorityTier
  plan: T | null
  planId: number | null
  needsExplicitSelection: boolean
  sameTierPlans: T[]
  reason: string
  probePlanIds: number[]
}

export function planLikelyHasPicking(plan: PriorityPlanLike): boolean {
  return typeof plan.document_count === "number" && plan.document_count > 0
}

/**
 * Candidatos a consultar progreso/picking (acotado).
 * Usa fecha + document_count del listado + recorrido activo.
 */
export function selectProgressProbeCandidates<T extends PriorityPlanLike>(
  plans: T[],
  opts?: {
    todayYmd?: string
    activePlanId?: number | null
    maxProbes?: number
  },
): T[] {
  const todayYmd = opts?.todayYmd ?? santiagoTodayYmd()
  const maxProbes = Math.max(1, opts?.maxProbes ?? MAX_PROGRESS_PROBES)
  const buckets = partitionPlansByDate(plans, todayYmd)
  const byId = new Map(plans.map((p) => [p.id, p]))
  const out: T[] = []
  const seen = new Set<number>()

  function push(p: T | undefined) {
    if (!p || seen.has(p.id) || out.length >= maxProbes) return
    seen.add(p.id)
    out.push(p)
  }

  if (opts?.activePlanId != null) {
    push(byId.get(opts.activePlanId))
  }

  for (const p of buckets.today.slice(0, MAX_TODAY_IN_PROBES)) {
    push(p)
  }

  const historyLikely = buckets.history.filter(planLikelyHasPicking)
  const historyFallback = historyLikely.length
    ? historyLikely
    : buckets.history
  for (const p of historyFallback.slice(0, MAX_HISTORY_IN_PROBES)) {
    push(p)
  }

  if (out.length < maxProbes) {
    const unk = buckets.unknown.find(
      (p) => planLikelyHasPicking(p) && !seen.has(p.id),
    )
    push(unk)
  }

  return out
}

export function classifyPriorityTier(
  plan: PriorityPlanLike,
  signals: PlanOpsSignals | null | undefined,
  todayYmd: string = santiagoTodayYmd(),
): PriorityTier {
  if (!signals?.pickingReady) return "D"
  const bucket: PlanDateBucket = bucketPlanDate(plan.planning_date, todayYmd)
  const inProgress = Math.max(0, signals.inProgress || 0)
  const remaining = Math.max(0, signals.remainingClients || 0)
  const pendingOps = remaining > 0 || inProgress > 0

  if (inProgress > 0) return "A"
  if (bucket === "today" && pendingOps) return "B"
  if ((bucket === "history" || bucket === "unknown") && pendingOps) return "C"
  return "D"
}

/**
 * Elige como máximo un plan auto-descargable (A/B/C).
 * Empate en el mismo nivel: activePlanId o needsExplicitSelection.
 */
export function resolvePriorityPlan<T extends PriorityPlanLike>(
  plans: T[],
  signalsById: Record<number, PlanOpsSignals | undefined>,
  opts?: {
    todayYmd?: string
    activePlanId?: number | null
    probePlanIds?: number[]
  },
): PriorityDecision<T> {
  const todayYmd = opts?.todayYmd ?? santiagoTodayYmd()
  const probePlanIds = opts?.probePlanIds ?? plans.map((p) => p.id)
  const activePlanId = opts?.activePlanId ?? null

  const ranked: { plan: T; tier: PriorityTier }[] = []
  for (const plan of plans) {
    if (!probePlanIds.includes(plan.id) && plan.id !== activePlanId) continue
    const signals = signalsById[plan.id]
    if (!signals) continue
    const tier = classifyPriorityTier(plan, signals, todayYmd)
    if (tier === "A" || tier === "B" || tier === "C") {
      ranked.push({ plan, tier })
    }
  }

  const order: PriorityTier[] = ["A", "B", "C"]
  for (const tier of order) {
    const group = ranked.filter((r) => r.tier === tier).map((r) => r.plan)
    if (group.length === 0) continue
    if (group.length === 1) {
      return {
        tier,
        plan: group[0],
        planId: group[0].id,
        needsExplicitSelection: false,
        sameTierPlans: group,
        reason: reasonForTier(tier),
        probePlanIds,
      }
    }
    const preferred = activePlanId
      ? group.find((p) => p.id === activePlanId)
      : undefined
    if (preferred) {
      return {
        tier,
        plan: preferred,
        planId: preferred.id,
        needsExplicitSelection: false,
        sameTierPlans: group,
        reason: `${reasonForTier(tier)} · recorrido activo #${preferred.id}`,
        probePlanIds,
      }
    }
    return {
      tier,
      plan: null,
      planId: null,
      needsExplicitSelection: true,
      sameTierPlans: group,
      reason: `Varios despachos nivel ${tier}: elige cuál preparar`,
      probePlanIds,
    }
  }

  return {
    tier: "none",
    plan: null,
    planId: null,
    needsExplicitSelection: false,
    sameTierPlans: [],
    reason: "Sin despacho prioritario auto-elegible",
    probePlanIds,
  }
}

function reasonForTier(tier: PriorityTier): string {
  if (tier === "A") return "Visita en curso"
  if (tier === "B") return "Despacho de hoy con gestiones pendientes"
  if (tier === "C") return "Despacho anterior pendiente"
  return "Preparación manual"
}

export function signalsFromProgress(input: {
  picking_ready?: boolean
  progress?: {
    remaining_clients?: number
    by_result?: { in_progress?: number }
  } | null
}): PlanOpsSignals {
  return {
    pickingReady: Boolean(input.picking_ready),
    remainingClients: Math.max(0, Number(input.progress?.remaining_clients) || 0),
    inProgress: Math.max(
      0,
      Number(input.progress?.by_result?.in_progress) || 0,
    ),
  }
}
