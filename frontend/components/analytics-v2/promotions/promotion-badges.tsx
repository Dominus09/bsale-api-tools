import { cn } from "@/lib/utils"
import {
  APPROVAL_LABELS,
  APPROVAL_TONES,
  EXCEPTION_LABELS,
  KIND_LABELS,
  TIMELINE_LABELS,
  TIMELINE_TONES,
  VERIFICATION_LABELS,
  VERIFICATION_TONES,
} from "@/lib/analytics-v2/promotions-labels"
import type {
  CampaignApproval,
  CampaignKind,
  CampaignTimeline,
  ItemExceptionCheck,
  PriceVerification,
} from "@/lib/analytics-v2/promotions-types"

const chip = "inline-block whitespace-nowrap rounded-md border px-1.5 py-0.5 text-[11px] font-medium"

export function KindBadge({ kind }: { kind: CampaignKind }) {
  return (
    <span
      className={cn(
        chip,
        kind === "remate" ? "border-orange-300 bg-orange-50 text-orange-800 dark:border-orange-800 dark:bg-orange-950/40 dark:text-orange-300" : "border-fuchsia-300 bg-fuchsia-50 text-fuchsia-800 dark:border-fuchsia-800 dark:bg-fuchsia-950/40 dark:text-fuchsia-300",
      )}
    >
      {KIND_LABELS[kind]}
    </span>
  )
}

export function ApprovalBadge({ status }: { status: CampaignApproval }) {
  return <span className={cn(chip, APPROVAL_TONES[status])}>{APPROVAL_LABELS[status]}</span>
}

export function TimelineBadge({ timeline }: { timeline: CampaignTimeline }) {
  return <span className={cn(chip, TIMELINE_TONES[timeline])}>{TIMELINE_LABELS[timeline]}</span>
}

export function VerificationBadge({ verification }: { verification: PriceVerification }) {
  return <span className={cn(chip, VERIFICATION_TONES[verification])}>{VERIFICATION_LABELS[verification]}</span>
}

export function ExceptionBadge({ check }: { check: ItemExceptionCheck }) {
  const tone =
    check === "excepcion_verificada"
      ? "border-emerald-300 bg-emerald-50 text-emerald-800"
      : check === "no_vigente" || check === "no_participa" || check === "sin_excepcion"
        ? "border-border bg-muted text-muted-foreground"
        : "border-orange-300 bg-orange-50 text-orange-800 dark:border-orange-800 dark:bg-orange-950/40 dark:text-orange-300"
  return <span className={cn(chip, "whitespace-normal", tone)}>{EXCEPTION_LABELS[check] ?? check}</span>
}
