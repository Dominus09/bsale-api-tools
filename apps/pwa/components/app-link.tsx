"use client"

import Link from "next/link"
import type { ComponentProps, MouseEvent } from "react"
import { isDeviceOffline } from "@/lib/offline-store"

type Props = ComponentProps<typeof Link>

/**
 * Offline: navegación documento completa (SW navigate + shell cacheado).
 * Evita soft-nav RSC/Flight que falla sin red y rompe la UI.
 */
export function AppLink({ href, onClick, prefetch, ...rest }: Props) {
  function handleClick(e: MouseEvent<HTMLAnchorElement>) {
    onClick?.(e)
    if (e.defaultPrevented) return
    if (!isDeviceOffline()) return
    const url = typeof href === "string" ? href : href.pathname || ""
    if (!url || url.startsWith("http") || url.startsWith("mailto:") || url.startsWith("tel:")) {
      return
    }
    e.preventDefault()
    window.location.assign(url)
  }

  return (
    <Link
      {...rest}
      href={href}
      prefetch={isDeviceOffline() ? false : prefetch}
      onClick={handleClick}
    />
  )
}
