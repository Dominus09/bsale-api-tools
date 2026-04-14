"use client"

import type { ReactNode } from "react"

import { DistribuidoraPlanningProvider } from "@/context/distribuidora-planning-selection"

export default function DistribuidoraSectionLayout({
  children,
}: {
  children: ReactNode
}) {
  return <DistribuidoraPlanningProvider>{children}</DistribuidoraPlanningProvider>
}
