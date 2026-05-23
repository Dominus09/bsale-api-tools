/** fetch con timeout y composición de AbortSignal externo. */

export const DEFAULT_FETCH_TIMEOUT_MS = 90_000
export const ORS_FETCH_TIMEOUT_MS = 180_000

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
