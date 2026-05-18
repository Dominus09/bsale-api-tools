"use client"

import { useEffect, useState } from "react"
import { ImageIcon, Loader2 } from "lucide-react"

import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
import { getApiBaseUrl } from "@/lib/api-base"
import { getAuthHeaders } from "@/lib/api"
import { cn } from "@/lib/utils"

type Props = {
  visitaId: number
  fotoUrl: string | null
  tieneFoto?: boolean
  alt?: string
  className?: string
}

function resolveFetchUrl(fotoUrl: string | null, visitaId: number, tieneFoto?: boolean): string | null {
  if (fotoUrl?.startsWith("data:") || fotoUrl?.startsWith("http://") || fotoUrl?.startsWith("https://")) {
    return fotoUrl
  }
  if (!fotoUrl && !tieneFoto) return null
  const base = getApiBaseUrl().replace(/\/$/, "")
  const path = fotoUrl?.startsWith("/") ? fotoUrl : `/operaciones/foto/${visitaId}`
  return `${base}${path}`
}

export function IncidenciaFoto({ visitaId, fotoUrl, tieneFoto, alt = "Evidencia", className }: Props) {
  const [blobUrl, setBlobUrl] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(false)

  const directUrl =
    fotoUrl?.startsWith("data:") || fotoUrl?.startsWith("http://") || fotoUrl?.startsWith("https://")
      ? fotoUrl
      : null

  const needsAuthFetch = Boolean((tieneFoto || fotoUrl) && !directUrl)

  useEffect(() => {
    if (!needsAuthFetch) {
      setBlobUrl(null)
      return
    }
    let cancelled = false
    const url = resolveFetchUrl(fotoUrl, visitaId, tieneFoto)
    if (!url) return

    setLoading(true)
    setError(false)
    fetch(url, { headers: getAuthHeaders() })
      .then((res) => {
        if (!res.ok) throw new Error(String(res.status))
        return res.blob()
      })
      .then((blob) => {
        if (cancelled) return
        setBlobUrl(URL.createObjectURL(blob))
      })
      .catch(() => {
        if (!cancelled) setError(true)
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })

    return () => {
      cancelled = true
      setBlobUrl((prev) => {
        if (prev) URL.revokeObjectURL(prev)
        return null
      })
    }
  }, [needsAuthFetch, fotoUrl, visitaId, tieneFoto])

  const displaySrc = directUrl || blobUrl

  if (!tieneFoto && !fotoUrl) {
    return (
      <span className="inline-flex items-center gap-1 text-xs text-muted-foreground">
        <ImageIcon className="h-3.5 w-3.5" />
        Sin foto
      </span>
    )
  }

  if (loading) {
    return <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
  }

  if (error || !displaySrc) {
    return (
      <span className="inline-flex items-center gap-1 text-xs text-muted-foreground">
        <ImageIcon className="h-3.5 w-3.5" />
        No disponible
      </span>
    )
  }

  return (
    <Dialog>
      <DialogTrigger asChild>
        <button type="button" className={cn("block overflow-hidden rounded-md border", className)}>
          {/* eslint-disable-next-line @next/next/no-img-element */}
          <img src={displaySrc} alt={alt} className="h-12 w-12 object-cover" />
        </button>
      </DialogTrigger>
      <DialogContent className="max-w-3xl">
        <DialogHeader>
          <DialogTitle>{alt}</DialogTitle>
        </DialogHeader>
        {/* eslint-disable-next-line @next/next/no-img-element */}
        <img src={displaySrc} alt={alt} className="max-h-[70vh] w-full rounded-md object-contain" />
      </DialogContent>
    </Dialog>
  )
}
