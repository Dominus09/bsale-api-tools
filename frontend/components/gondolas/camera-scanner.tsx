"use client"

import { useEffect, useRef, useState } from "react"
import { Camera, CameraOff, Loader2, Volume2, VolumeX } from "lucide-react"

import { Button } from "@/components/ui/button"
import { cn } from "@/lib/utils"

type ScannerControls = { stop: () => void }

const DEDUPE_MS = 2500

function beep(enabled: boolean) {
  if (!enabled) return
  try {
    const ctx = new AudioContext()
    const osc = ctx.createOscillator()
    const gain = ctx.createGain()
    osc.frequency.value = 880
    gain.gain.value = 0.06
    osc.connect(gain).connect(ctx.destination)
    osc.start()
    osc.stop(ctx.currentTime + 0.09)
    osc.onended = () => void ctx.close()
  } catch {
    // El sonido es opcional; su ausencia no bloquea el escaneo.
  }
}

/**
 * Escáner con cámara trasera usando @zxing/browser (carga dinámica).
 * EAN-13, EAN-8, UPC-A/UPC-E y Code 128. Deduplica lecturas consecutivas del
 * mismo código y libera la cámara al pausar, cerrar o desmontar.
 */
export function CameraScanner({
  onCode,
}: {
  onCode: (code: string) => void
}) {
  const videoRef = useRef<HTMLVideoElement | null>(null)
  const controlsRef = useRef<ScannerControls | null>(null)
  const lastRef = useRef<{ code: string; at: number }>({ code: "", at: 0 })
  const onCodeRef = useRef(onCode)
  onCodeRef.current = onCode
  const [active, setActive] = useState(false)
  const [starting, setStarting] = useState(false)
  const [sound, setSound] = useState(true)
  const soundRef = useRef(sound)
  soundRef.current = sound
  const [error, setError] = useState<string | null>(null)

  function stopCamera() {
    try {
      controlsRef.current?.stop()
    } catch {
      // Liberación defensiva: también se detienen las pistas del elemento.
    }
    controlsRef.current = null
    const video = videoRef.current
    const stream = (video?.srcObject as MediaStream | null) ?? null
    stream?.getTracks().forEach((track) => track.stop())
    if (video) video.srcObject = null
    setActive(false)
  }

  async function startCamera() {
    setError(null)
    setStarting(true)
    try {
      if (!navigator.mediaDevices?.getUserMedia) {
        throw new Error("Este navegador no permite usar la cámara. Ingrese el código manualmente.")
      }
      const zxing = await import("@zxing/browser")
      const reader = new zxing.BrowserMultiFormatReader()
      reader.possibleFormats = [
        zxing.BarcodeFormat.EAN_13,
        zxing.BarcodeFormat.EAN_8,
        zxing.BarcodeFormat.UPC_A,
        zxing.BarcodeFormat.UPC_E,
        zxing.BarcodeFormat.CODE_128,
      ]
      if (!videoRef.current) throw new Error("No se pudo preparar la vista de cámara.")
      const controls = await reader.decodeFromConstraints(
        { video: { facingMode: { ideal: "environment" } }, audio: false },
        videoRef.current,
        (result) => {
          if (!result) return
          const code = result.getText().trim()
          if (!code) return
          const now = Date.now()
          if (lastRef.current.code === code && now - lastRef.current.at < DEDUPE_MS) return
          lastRef.current = { code, at: now }
          beep(soundRef.current)
          onCodeRef.current(code)
        },
      )
      controlsRef.current = controls as unknown as ScannerControls
      setActive(true)
    } catch (err) {
      const name = err instanceof DOMException ? err.name : ""
      setError(
        name === "NotAllowedError" || name === "SecurityError"
          ? "Permiso de cámara denegado. Puede autorizarla en el navegador o ingresar el código manualmente."
          : err instanceof Error
            ? err.message
            : "No se pudo iniciar la cámara. Ingrese el código manualmente.",
      )
      stopCamera()
    } finally {
      setStarting(false)
    }
  }

  useEffect(() => {
    return () => {
      try {
        controlsRef.current?.stop()
      } catch {
        // Sin efecto al desmontar.
      }
      controlsRef.current = null
    }
    // Solo limpieza al desmontar.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  return (
    <div className="space-y-3">
      <div
        className={cn(
          "relative overflow-hidden rounded-xl border bg-slate-950",
          active ? "border-slate-800" : "border-slate-200 bg-slate-100",
        )}
      >
        <video
          ref={videoRef}
          className={cn("aspect-[4/3] w-full object-cover", active ? "opacity-100" : "opacity-0")}
          muted
          playsInline
          aria-label="Vista de cámara para escanear códigos de barras"
        />
        {!active ? (
          <div className="absolute inset-0 flex flex-col items-center justify-center gap-2 p-4 text-center">
            <Camera className="h-8 w-8 text-slate-400" aria-hidden="true" />
            <p className="text-sm text-slate-500">
              Cámara detenida. Ábrala para escanear productos en la góndola.
            </p>
          </div>
        ) : (
          <div className="pointer-events-none absolute inset-x-8 top-1/2 h-16 -translate-y-1/2 rounded-lg border-2 border-white/70" />
        )}
      </div>
      {error ? (
        <p role="alert" className="rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-900">
          {error}
        </p>
      ) : null}
      <div className="flex flex-wrap gap-2">
        {active ? (
          <Button type="button" variant="outline" onClick={stopCamera} className="min-h-11">
            <CameraOff className="mr-2 h-4 w-4" /> Pausar cámara
          </Button>
        ) : (
          <Button type="button" onClick={() => void startCamera()} disabled={starting} className="min-h-11">
            {starting ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Camera className="mr-2 h-4 w-4" />}
            Escanear con cámara
          </Button>
        )}
        <Button
          type="button"
          variant="ghost"
          onClick={() => setSound((v) => !v)}
          className="min-h-11"
          aria-pressed={sound}
        >
          {sound ? <Volume2 className="mr-2 h-4 w-4" /> : <VolumeX className="mr-2 h-4 w-4" />}
          Sonido {sound ? "activado" : "silenciado"}
        </Button>
      </div>
    </div>
  )
}
