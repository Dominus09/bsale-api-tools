/**
 * Estado compartido de sync offline (evita descargas duplicadas entre pantallas).
 * Solo lectura. No depende de background iOS.
 */

import {
  fetchMe,
  getMyDespachoProgress,
  type DriverDespacho,
} from "./api"
import { getActivePlanId, setActivePlanId } from "./active-plan"
import { fingerprintKey } from "./offline-fingerprint"
import {
  resolvePriorityPlan,
  selectProgressProbeCandidates,
  signalsFromProgress,
  type PlanOpsSignals,
  type PriorityDecision,
} from "./offline-priority"
import {
  getOfflinePlanReady,
  listOfflineReadyPlans,
  type OfflinePlanReady,
} from "./offline-store"
import {
  reconcileOfflineNavForCurrentBuild,
  syncPlanForOffline,
  syncSinglePriorityPlanForOffline,
  type SyncPlanProgress,
} from "./offline-sync"
import { isOfflineUiReady, loadOfflineBuildInfo } from "./offline-build"
import { santiagoTodayYmd } from "./plan-buckets"

export type ConnKind = "api" | "declared_offline" | "declared_online_unreachable" | "unknown"

export type IndicatorTone = "green" | "orange" | "yellow" | "red" | "gray"

export type OfflineSyncRuntimeState = {
  phase:
    | "idle"
    | "probing"
    | "selecting"
    | "syncing"
    | "done"
    | "error"
    | "needs_selection"
  conn: ConnKind
  apiReachable: boolean | null
  tone: IndicatorTone
  priority: PriorityDecision | null
  plan: DriverDespacho | null
  progress: SyncPlanProgress | null
  ready: OfflinePlanReady | null
  lastError: string | null
  showPrepModal: boolean
  dismissedModalKey: string | null
  selectionPlans: DriverDespacho[]
  message: string
}

type Listener = (s: OfflineSyncRuntimeState) => void

const DISMISS_PREFIX = "lq:ops:prepDismiss:"

function readDismissed(userId: string): string | null {
  if (typeof window === "undefined") return null
  try {
    return window.sessionStorage.getItem(`${DISMISS_PREFIX}${userId}`)
  } catch {
    return null
  }
}

function writeDismissed(userId: string, key: string) {
  if (typeof window === "undefined") return
  try {
    window.sessionStorage.setItem(`${DISMISS_PREFIX}${userId}`, key)
  } catch {
    /* ignore */
  }
}

function initialState(): OfflineSyncRuntimeState {
  return {
    phase: "idle",
    conn: "unknown",
    apiReachable: null,
    tone: "gray",
    priority: null,
    plan: null,
    progress: null,
    ready: null,
    lastError: null,
    showPrepModal: false,
    dismissedModalKey: null,
    selectionPlans: [],
    message: "",
  }
}

function computeTone(s: OfflineSyncRuntimeState): IndicatorTone {
  if (s.phase === "syncing" || s.phase === "probing" || s.phase === "selecting") {
    return "orange"
  }
  if (s.conn === "declared_offline" || s.apiReachable === false) {
    return s.ready?.complete ? "yellow" : "red"
  }
  if (s.apiReachable && s.ready?.complete) return "green"
  if (s.apiReachable) return "gray"
  if (s.ready?.complete) return "yellow"
  return "red"
}

class OfflineSyncRuntime {
  private state: OfflineSyncRuntimeState = initialState()
  private listeners = new Set<Listener>()
  private inflightByPlan = new Map<number, Promise<OfflinePlanReady>>()
  private cycleInflight: Promise<void> | null = null
  private plansCache: DriverDespacho[] = []
  private userId: string | null = null

  subscribe(fn: Listener): () => void {
    this.listeners.add(fn)
    fn(this.state)
    return () => this.listeners.delete(fn)
  }

  getState(): OfflineSyncRuntimeState {
    return this.state
  }

  private set(partial: Partial<OfflineSyncRuntimeState>) {
    this.state = { ...this.state, ...partial }
    this.state.tone = computeTone(this.state)
    for (const fn of this.listeners) fn(this.state)
  }

  setUserId(userId: string | null) {
    this.userId = userId
    if (userId) {
      this.set({ dismissedModalKey: readDismissed(userId) })
    }
  }

  rememberPlans(plans: DriverDespacho[]) {
    this.plansCache = plans
  }

  dismissPrepModal() {
    const fp = this.state.ready?.fingerprint
    const planId = this.state.plan?.id ?? this.state.ready?.planId
    if (this.userId && planId != null && fp) {
      const key = `${planId}:${fingerprintKey(fp)}`
      writeDismissed(this.userId, key)
      this.set({ showPrepModal: false, dismissedModalKey: key })
      return
    }
    this.set({ showPrepModal: false })
  }

  choosePlanForPrep(planId: number) {
    if (this.userId) setActivePlanId(this.userId, planId)
    const plan = this.plansCache.find((p) => p.id === planId) || null
    if (plan && this.userId) {
      void this.syncPlan(planId, { force: false, manual: true, plan })
    }
  }

  /** Marca API alcanzable tras un listado exitoso (evita probe extra). */
  markApiReachable(ok: boolean) {
    if (ok) {
      this.set({ conn: "api", apiReachable: true })
      return
    }
    if (typeof navigator !== "undefined" && navigator.onLine === false) {
      this.set({ conn: "declared_offline", apiReachable: false })
      return
    }
    this.set({ conn: "declared_online_unreachable", apiReachable: false })
  }

  async probeApiReachability(): Promise<boolean> {
    if (typeof navigator !== "undefined" && navigator.onLine === false) {
      this.set({ conn: "declared_offline", apiReachable: false })
      return false
    }
    try {
      await fetchMe()
      this.set({ conn: "api", apiReachable: true })
      return true
    } catch {
      if (typeof navigator !== "undefined" && navigator.onLine === false) {
        this.set({ conn: "declared_offline", apiReachable: false })
        return false
      }
      this.set({ conn: "declared_online_unreachable", apiReachable: false })
      return false
    }
  }

  private async refreshReady(planId: number | null) {
    if (!this.userId || planId == null) {
      this.set({ ready: null })
      return
    }
    const build = await loadOfflineBuildInfo()
    const row = await getOfflinePlanReady(this.userId, planId)
    if (!row) {
      this.set({ ready: null })
      return
    }
    const complete = isOfflineUiReady(row, build.navBuildId)
    this.set({
      ready: {
        ...row,
        complete,
        dataComplete: Boolean(row.dataComplete ?? row.complete),
        navReady: complete || Boolean(row.navReady && row.navBuildId === build.navBuildId),
        navBuildId: row.navBuildId,
      },
    })
  }

  private shouldShowModal(ready: OfflinePlanReady | null, planId: number): boolean {
    if (!ready || !ready.complete || !ready.fingerprint) return false
    const key = `${planId}:${fingerprintKey(ready.fingerprint)}`
    return this.state.dismissedModalKey !== key
  }

  async syncPlan(
    planId: number,
    opts?: { force?: boolean; manual?: boolean; plan?: DriverDespacho | null },
  ): Promise<OfflinePlanReady | null> {
    if (!this.userId) return null
    const existing = this.inflightByPlan.get(planId)
    if (existing) return existing

    const plan =
      opts?.plan ||
      this.plansCache.find((p) => p.id === planId) ||
      this.state.plan
    const run = (async () => {
      this.set({
        phase: "syncing",
        plan: plan || this.state.plan,
        lastError: null,
        message: "Sincronizando…",
        showPrepModal:
          opts?.manual ||
          this.shouldShowModal(this.state.ready, planId) ||
          !this.state.ready?.complete,
      })
      try {
        const ready = await syncPlanForOffline(this.userId!, planId, {
          force: opts?.force,
          onProgress: (p) => {
            this.set({
              progress: p,
              message: p.message,
              phase: p.phase === "skip" || p.phase === "done" ? this.state.phase : "syncing",
            })
          },
        })
        this.set({
          ready,
          progress: {
            planId,
            phase: ready.complete ? "done" : "error",
            clientsTotal: ready.clientsCount,
            detailsDone: ready.detailsCount,
            documentsDone: ready.documentsCount,
            documentsTotal: ready.fingerprint?.documents,
            message: ready.complete
              ? "Listo sin conexión"
              : "Preparación incompleta",
            skipped: false,
          },
          phase: ready.complete ? "done" : "error",
          lastError: ready.complete ? null : "Preparación incompleta",
          message: ready.complete
            ? "¡Tu recorrido está listo para usar sin internet!"
            : "No se pudo completar la preparación",
          showPrepModal: ready.complete
            ? this.shouldShowModal(ready, planId)
            : true,
        })
        return ready
      } catch (err) {
        const msg = err instanceof Error ? err.message : "Error de sincronización"
        this.set({
          phase: "error",
          lastError: msg,
          message: msg,
          showPrepModal: true,
        })
        throw err
      } finally {
        this.inflightByPlan.delete(planId)
      }
    })()

    this.inflightByPlan.set(planId, run)
    try {
      return await run
    } catch {
      return null
    }
  }

  /**
   * Ciclo principal al abrir PWA / Mis despachos con red.
   * Máximo un despacho prioritario auto-descargado.
   */
  async runPriorityCycle(plans: DriverDespacho[]): Promise<void> {
    if (!this.userId) return
    if (this.cycleInflight) return this.cycleInflight

    this.plansCache = plans
    this.cycleInflight = this._runPriorityCycle(plans).finally(() => {
      this.cycleInflight = null
    })
    return this.cycleInflight
  }

  private async _runPriorityCycle(plans: DriverDespacho[]): Promise<void> {
    this.set({ phase: "probing", message: "Comprobando conexión…" })
    const reachable = await this.probeApiReachability()
    if (!reachable) {
      const active = this.userId ? getActivePlanId(this.userId) : null
      const build = await loadOfflineBuildInfo()
      const readyRows = await listOfflineReadyPlans(this.userId!, {
        includeDataOnly: true,
      })
      const preferred =
        readyRows.find((r) => r.planId === active && isOfflineUiReady(r, build.navBuildId)) ||
        readyRows.find((r) => isOfflineUiReady(r, build.navBuildId)) ||
        null
      this.set({
        phase: preferred ? "done" : "idle",
        ready: preferred,
        plan:
          plans.find((p) => p.id === preferred?.planId) ||
          this.state.plan,
        message: preferred
          ? "Sin conexión · despacho disponible para consulta"
          : "Sin conexión · sin despacho preparado",
      })
      return
    }

    try {
      await reconcileOfflineNavForCurrentBuild(this.userId!)
    } catch {
      /* best effort */
    }

    this.set({ phase: "selecting", message: "Eligiendo despacho prioritario…" })
    const todayYmd = santiagoTodayYmd()
    const activePlanId = getActivePlanId(this.userId)
    const knownLocal = await listOfflineReadyPlans(this.userId!, {
      includeDataOnly: true,
    })
    const probes = selectProgressProbeCandidates(plans, {
      todayYmd,
      activePlanId,
      // Conserva recorridos ya sincronizados / en curso conocidos en el cupo de 6.
      preferPlanIds: knownLocal.map((r) => r.planId),
    })
    const signalsById: Record<number, PlanOpsSignals | undefined> = {}

    await Promise.all(
      probes.map(async (p) => {
        try {
          const res = await getMyDespachoProgress(p.id)
          signalsById[p.id] = signalsFromProgress(res)
        } catch {
          /* ignore probe failure */
        }
      }),
    )

    const decision = resolvePriorityPlan(plans, signalsById, {
      todayYmd,
      activePlanId,
      probePlanIds: probes.map((p) => p.id),
    })

    this.set({
      priority: decision,
      selectionPlans: decision.sameTierPlans,
    })

    if (decision.needsExplicitSelection) {
      this.set({
        phase: "needs_selection",
        plan: null,
        message: decision.reason,
        showPrepModal: false,
      })
      return
    }

    if (!decision.plan) {
      // Refrescar ready del active si existe
      await this.refreshReady(activePlanId)
      this.set({
        phase: "idle",
        plan: null,
        message: decision.reason,
        tone: "gray",
      })
      return
    }

    setActivePlanId(this.userId, decision.plan.id)
    this.set({ plan: decision.plan })

    const before = await getOfflinePlanReady(this.userId, decision.plan.id)
    const build = await loadOfflineBuildInfo()
    const alreadyUiReady = before ? isOfflineUiReady(before, build.navBuildId) : false
    const openModal = !alreadyUiReady
    if (openModal) this.set({ showPrepModal: true, phase: "syncing" })

    const ready = await syncSinglePriorityPlanForOffline(
      this.userId,
      decision.plan,
      {
        onProgress: (p) => {
          this.set({
            phase: "syncing",
            progress: p,
            message: p.message,
            showPrepModal: openModal && !p.skipped ? true : this.state.showPrepModal,
          })
        },
      },
    )

    if (ready.complete && (alreadyUiReady || ready.fingerprint)) {
      // Skip silencioso si ya estaba listo; si es prep nueva, ofrecer "Entendido".
      const showDone = openModal && this.shouldShowModal(ready, decision.plan.id)
      this.set({
        ready,
        phase: "done",
        lastError: null,
        message: "¡Tu recorrido está listo para usar sin internet!",
        showPrepModal: showDone,
        progress: {
          planId: decision.plan.id,
          phase: "done",
          clientsTotal: ready.clientsCount,
          detailsDone: ready.detailsCount,
          documentsDone: ready.documentsCount,
          documentsTotal: ready.fingerprint?.documents,
          message: "Listo",
          skipped: !openModal,
        },
      })
      return
    }

    this.set({
      ready,
      phase: "error",
      lastError: ready.fingerprint?.truncated
        ? "Más de 80 clientes: no se puede marcar completo en V1"
        : "Preparación incompleta",
      message: ready.fingerprint?.truncated
        ? "Más de 80 clientes: no se puede marcar completo en V1"
        : "Preparación incompleta",
      showPrepModal: true,
    })
  }

  async syncNow() {
    const planId = this.state.plan?.id || this.state.priority?.planId
    if (planId == null) {
      if (this.plansCache.length) {
        await this.runPriorityCycle(this.plansCache)
      }
      return
    }
    await this.syncPlan(planId, { force: true, manual: true })
  }
}

export const offlineSyncRuntime = new OfflineSyncRuntime()
