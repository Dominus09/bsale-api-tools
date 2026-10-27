import { cn } from "@/lib/utils"

/** Identificación visual OWNER (Graphite Premium) — solo si el backend lo confirma. */
export function OwnerBadge({
  className,
  compact = false,
}: {
  className?: string
  compact?: boolean
}) {
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-sm border border-white/15 bg-white/[0.06] font-medium tracking-[0.12em] text-lq-on-graphite",
        compact
          ? "px-1.5 py-0.5 text-[9px] uppercase text-lq-ink/80 dark:text-lq-on-graphite"
          : "px-2 py-0.5 text-[10px] uppercase text-lq-ink/85 dark:text-lq-on-graphite",
        "border-lq-line bg-lq-canvas text-lq-muted dark:border-white/15 dark:bg-white/[0.06] dark:text-lq-on-graphite-muted",
        className,
      )}
      title="Propietario del ERP"
    >
      Owner
    </span>
  )
}
