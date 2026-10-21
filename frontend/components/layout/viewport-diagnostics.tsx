"use client"

/**
 * Diagnóstico temporal de viewport (solo modo de prueba).
 *
 * Se activa navegando a cualquier ruta del dashboard con `?lqdiag=1`
 * (queda activo durante la sesión) y se desactiva con el botón «Cerrar»
 * o quitando el parámetro con `?lqdiag=0`. Muestra las medidas reales que
 * el dispositivo reporta, para validar el responsive en un iPhone real.
 * No muestra datos comerciales ni de sesión. RETIRAR tras la validación.
 */
import { useEffect, useState } from "react"
import { X } from "lucide-react"

type Diag = {
  innerWidth: number
  innerHeight: number
  clientWidth: number
  visualWidth: number | null
  screenWidth: number
  dpr: number
  mq767: boolean
  mq1023: boolean
  standalone: boolean
  orientation: string
}

function measure(): Diag {
  const vv = window.visualViewport
  return {
    innerWidth: window.innerWidth,
    innerHeight: window.innerHeight,
    clientWidth: document.documentElement.clientWidth,
    visualWidth: vv ? Math.round(vv.width) : null,
    screenWidth: window.screen.width,
    dpr: window.devicePixelRatio,
    mq767: window.matchMedia("(max-width: 767px)").matches,
    mq1023: window.matchMedia("(max-width: 1023px)").matches,
    standalone:
      window.matchMedia("(display-mode: standalone)").matches ||
      (window.navigator as unknown as { standalone?: boolean }).standalone === true,
    orientation: window.innerWidth >= window.innerHeight ? "horizontal" : "vertical",
  }
}

export function ViewportDiagnostics() {
  const [active, setActive] = useState(false)
  const [diag, setDiag] = useState<Diag | null>(null)

  useEffect(() => {
    const qs = new URLSearchParams(window.location.search)
    if (qs.get("lqdiag") === "1") sessionStorage.setItem("lq-diag", "1")
    if (qs.get("lqdiag") === "0") sessionStorage.removeItem("lq-diag")
    setActive(sessionStorage.getItem("lq-diag") === "1")
  }, [])

  useEffect(() => {
    if (!active) return
    const update = () => setDiag(measure())
    update()
    window.addEventListener("resize", update)
    window.addEventListener("orientationchange", update)
    window.visualViewport?.addEventListener("resize", update)
    return () => {
      window.removeEventListener("resize", update)
      window.removeEventListener("orientationchange", update)
      window.visualViewport?.removeEventListener("resize", update)
    }
  }, [active])

  if (!active || !diag) return null

  const rows: Array<[string, string]> = [
    ["window.innerWidth × H", `${diag.innerWidth} × ${diag.innerHeight}`],
    ["documentElement.clientWidth", String(diag.clientWidth)],
    ["visualViewport.width", diag.visualWidth != null ? String(diag.visualWidth) : "—"],
    ["screen.width", String(diag.screenWidth)],
    ["devicePixelRatio", String(diag.dpr)],
    ["matchMedia ≤767px", diag.mq767 ? "SÍ (móvil)" : "NO"],
    ["matchMedia ≤1023px", diag.mq1023 ? "SÍ (drawer)" : "NO (sidebar PC)"],
    ["Modo", diag.standalone ? "PWA instalada" : "Safari/navegador"],
    ["Orientación", diag.orientation],
  ]

  return (
    <div className="fixed inset-x-2 bottom-2 z-[200] rounded-xl border border-amber-300 bg-amber-50/95 p-3 text-[12px] leading-5 text-amber-950 shadow-xl backdrop-blur dark:border-amber-800 dark:bg-amber-950/90 dark:text-amber-100">
      <div className="mb-1 flex items-center justify-between gap-2">
        <strong>Diagnóstico de viewport (temporal)</strong>
        <button
          type="button"
          aria-label="Cerrar diagnóstico"
          className="flex size-8 items-center justify-center rounded-md hover:bg-amber-200/60 dark:hover:bg-amber-900"
          onClick={() => {
            sessionStorage.removeItem("lq-diag")
            setActive(false)
          }}
        >
          <X className="size-4" aria-hidden />
        </button>
      </div>
      <dl className="grid grid-cols-2 gap-x-3">
        {rows.map(([k, v]) => (
          <div key={k} className="flex justify-between gap-2">
            <dt className="opacity-70">{k}</dt>
            <dd className="font-mono font-medium">{v}</dd>
          </div>
        ))}
      </dl>
      <p className="mt-1 opacity-70">
        En iPhone vertical se espera: ancho 360–440, ≤767 SÍ, ≤1023 SÍ.
        Gira el teléfono y vuelve a leer los valores.
      </p>
    </div>
  )
}
