"use client"

import type { ComponentProps } from "react"
import { ClientLocationCapture } from "@/components/client-location-capture"

type CaptureProps = Omit<ComponentProps<typeof ClientLocationCapture>, "menuDriven">

/**
 * Registra las herramientas del local (GPS, foto, historial, navegar) en el menú
 * sin ocupar el cuerpo de la página: solo aparece la hoja de la herramienta pedida.
 */
export function DeliveryLocalToolsHost(props: CaptureProps) {
  return <ClientLocationCapture {...props} menuDriven />
}
