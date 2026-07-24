"use client"

import type { ReactNode } from "react"
import { Loader2 } from "lucide-react"

import { Button } from "@/components/ui/button"
import { cn } from "@/lib/utils"

type Action = {
  label: string
  onClick: () => void
  variant?: "default" | "outline" | "secondary" | "ghost"
  loading?: boolean
  disabled?: boolean
  icon?: ReactNode
}

export function AnalyticsPageHeader({
  title,
  subtitle,
  meta,
  actions,
  className,
}: {
  title: string
  subtitle?: string
  meta?: ReactNode
  actions?: Action[]
  className?: string
}) {
  return (
    <header
      className={cn(
        "flex flex-col gap-4 border-b border-border/60 pb-4 sm:flex-row sm:items-start sm:justify-between",
        className,
      )}
    >
      <div className="min-w-0 space-y-1">
        <h1 className="text-2xl font-semibold tracking-tight text-foreground">{title}</h1>
        {subtitle ? (
          <p className="max-w-2xl text-sm text-muted-foreground">{subtitle}</p>
        ) : null}
        {meta ? <div className="pt-1 text-xs text-muted-foreground">{meta}</div> : null}
      </div>
      {actions?.length ? (
        <div className="flex flex-wrap items-center gap-2">
          {actions.map((a) => (
            <Button
              key={a.label}
              type="button"
              variant={a.variant ?? "outline"}
              size="sm"
              disabled={a.disabled || a.loading}
              onClick={a.onClick}
            >
              {a.loading ? <Loader2 className="mr-1.5 h-3.5 w-3.5 animate-spin" /> : a.icon}
              {a.label}
            </Button>
          ))}
        </div>
      ) : null}
    </header>
  )
}
