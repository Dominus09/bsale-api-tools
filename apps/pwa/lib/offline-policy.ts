/** Reexportas de política offline (compatibilidad de imports). */
export {
  isDeviceOffline,
  offlineBannerLabel,
  plansBundleUsable,
  shouldUseOfflineSnapshot,
} from "./offline-store"

export {
  MAX_PROGRESS_PROBES,
  OFFLINE_RETENTION_POLICY,
  classifyPriorityTier,
  resolvePriorityPlan,
  selectProgressProbeCandidates,
} from "./offline-priority"
