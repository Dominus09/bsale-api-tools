"use client"

import { Info } from "lucide-react"

import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import { cn } from "@/lib/utils"

export function CostInfoHint({
  title,
  text,
  className,
}: {
  title?: string
  text: string
  className?: string
}) {
  return (
    <TooltipProvider delayDuration={150}>
      <Tooltip>
        <TooltipTrigger asChild>
          <button
            type="button"
            className={cn(
              "inline-flex h-4 w-4 items-center justify-center rounded-full text-muted-foreground hover:text-foreground",
              className,
            )}
            aria-label={title || "Ayuda"}
          >
            <Info className="h-3.5 w-3.5" />
          </button>
        </TooltipTrigger>
        <TooltipContent className="max-w-xs text-xs leading-relaxed">
          {title ? <p className="mb-0.5 font-medium">{title}</p> : null}
          <p>{text}</p>
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  )
}
