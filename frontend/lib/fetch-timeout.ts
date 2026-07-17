/** fetch con timeout y composición de AbortSignal externo. */

export const DEFAULT_FETCH_TIMEOUT_MS = 90_000
export const DISPATCH_PREP_FETCH_TIMEOUT_MS = 90_000
export const ORS_FETCH_TIMEOUT_MS = 180_000
export const ORDERS_PURCHASE_FETCH_TIMEOUT_MS = 30_000

/** Timeout de fetch expirado (distinto de una cancelación externa). */
export class FetchTimeoutError extends Error {
  constructor(timeoutMs: number) {
    super(`La solicitud superó el tiempo máximo de ${Math.round(timeoutMs / 1000)} s.`)
    this.name = "FetchTimeoutError"
  }
}

/**
 * fetch con timeout que preserva la distinción entre:
 * - timeout expirado → lanza ``FetchTimeoutError``
 * - aborto externo (AbortController del caller) → re-lanza el ``AbortError`` original
 */
export async function fetchDistinguishingTimeout(
  input: RequestInfo | URL,
  init?: RequestInit,
  timeoutMs: number = DEFAULT_FETCH_TIMEOUT_MS,
): Promise<Response> {
  const controller = new AbortController()
  let timedOut = false
  const timeoutId = setTimeout(() => {
    timedOut = true
    controller.abort()
  }, timeoutMs)
  const parentSignal = init?.signal
  if (parentSignal) {
    if (parentSignal.aborted) controller.abort()
    else parentSignal.addEventListener("abort", () => controller.abort(), { once: true })
  }
  try {
    return await fetch(input, { ...init, signal: controller.signal })
  } catch (e: unknown) {
    if (e instanceof Error && e.name === "AbortError" && timedOut) {
      throw new FetchTimeoutError(timeoutMs)
    }
    throw e
  } finally {
    clearTimeout(timeoutId)
  }
}

export async function fetchWithTimeout(
  input: RequestInfo | URL,
  init?: RequestInit,
  timeoutMs: number = DEFAULT_FETCH_TIMEOUT_MS,
): Promise<Response> {
  const controller = new AbortController()
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs)
  const parentSignal = init?.signal
  if (parentSignal) {
    if (parentSignal.aborted) controller.abort()
    else parentSignal.addEventListener("abort", () => controller.abort(), { once: true })
  }
  try {
    return await fetch(input, { ...init, signal: controller.signal })
  } catch (e: unknown) {
    if (e instanceof Error && e.name === "AbortError") {
      throw new Error("La solicitud tardó demasiado o fue cancelada.")
    }
    throw e
  } finally {
    clearTimeout(timeoutId)
  }
}
