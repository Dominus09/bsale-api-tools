"use client"

import { useCallback, useEffect, useState } from "react"

import { getAuthMe, type AuthMeResponse } from "@/lib/api"
import type { StaffUserLike } from "@/lib/permissions"

export type AuthMeLoadState = "loading" | "ok" | "unauthorized" | "unavailable"

export function staffFromAuthMe(me: AuthMeResponse): StaffUserLike {
  return {
    id: me.id,
    role: me.role,
    email: me.email,
    is_owner: me.is_owner === true,
    management_access: me.management_access,
    admin_access: me.admin_access,
    permissions: me.permissions,
  }
}

/** Cache compartida: sidebar + página no disparan /auth/me en paralelo. */
const AUTH_ME_TTL_MS = 30_000
let cachedMe: { at: number; data: AuthMeResponse } | null = null
let inFlightMe: Promise<AuthMeResponse> | null = null

function clearAuthMeCache() {
  cachedMe = null
  inFlightMe = null
}

async function fetchAuthMeShared(force = false): Promise<AuthMeResponse> {
  if (!force && cachedMe && Date.now() - cachedMe.at < AUTH_ME_TTL_MS) {
    return cachedMe.data
  }
  if (!force && inFlightMe) {
    return inFlightMe
  }
  const request = getAuthMe()
    .then((data) => {
      cachedMe = { at: Date.now(), data }
      return data
    })
    .catch((err) => {
      clearAuthMeCache()
      throw err
    })
    .finally(() => {
      if (inFlightMe === request) inFlightMe = null
    })
  inFlightMe = request
  return request
}

/**
 * Carga GET /auth/me para autorización UI (Bearer desde localStorage.token).
 * En loading/unauthorized/unavailable no se deben mostrar acciones sensibles.
 */
export function useAuthMe() {
  const [meState, setMeState] = useState<AuthMeLoadState>("loading")
  const [staff, setStaff] = useState<StaffUserLike | null>(null)

  const reload = useCallback(async (force = false) => {
    setMeState("loading")
    setStaff(null)
    try {
      const me = await fetchAuthMeShared(force)
      setStaff(staffFromAuthMe(me))
      setMeState("ok")
    } catch (e) {
      const msg = e instanceof Error ? e.message : ""
      if (/401|sesión|autentic/i.test(msg)) {
        setMeState("unauthorized")
      } else {
        setMeState("unavailable")
      }
      setStaff(null)
    }
  }, [])

  useEffect(() => {
    void reload(false)
  }, [reload])

  const reloadForced = useCallback(() => reload(true), [reload])

  return { meState, staff, reload: reloadForced }
}
