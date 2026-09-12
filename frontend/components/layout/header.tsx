"use client"

import { useEffect, useState } from "react"
import { LogOut, Building2, User, WifiOff, PanelLeft, PanelLeftClose } from "lucide-react"
import { Button } from "@/components/ui/button"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import { Badge } from "@/components/ui/badge"
import { logout, getStoredEmail, getStoredCompanyName, getIsDemoMode, initDemoMode } from "@/lib/api"

type HeaderProps = {
  /** Si se pasa, muestra botón para colapsar/expandir el sidebar del dashboard. */
  sidebarOpen?: boolean
  onToggleSidebar?: () => void
}

export function Header({ sidebarOpen, onToggleSidebar }: HeaderProps) {
  const [email, setEmail] = useState<string | null>(null)
  const [companyName, setCompanyName] = useState<string | null>(null)
  const [isDemoMode, setIsDemoMode] = useState(false)

  useEffect(() => {
    initDemoMode()
    setEmail(getStoredEmail())
    setCompanyName(getStoredCompanyName())
    setIsDemoMode(getIsDemoMode())

    const interval = setInterval(() => {
      setIsDemoMode(getIsDemoMode())
    }, 1000)

    return () => clearInterval(interval)
  }, [])

  const handleLogout = () => {
    logout()
    window.location.href = "/login"
  }

  const handleChangeCompany = () => {
    localStorage.removeItem("company_id")
    localStorage.removeItem("company_name")
    window.location.href = "/company-selector"
  }

  return (
    <header className="flex h-14 shrink-0 items-center justify-between gap-2 border-b border-border bg-card px-3 sm:h-16 sm:px-6">
      <div className="flex min-w-0 flex-1 items-center gap-2 sm:gap-3">
        {onToggleSidebar ? (
          <Button
            type="button"
            variant="ghost"
            size="icon"
            className="h-10 w-10 shrink-0"
            onClick={onToggleSidebar}
            aria-label={sidebarOpen ? "Ocultar menú lateral" : "Mostrar menú lateral"}
          >
            {sidebarOpen ? (
              <PanelLeftClose className="h-5 w-5" />
            ) : (
              <PanelLeft className="h-5 w-5" />
            )}
          </Button>
        ) : null}
        <Building2 className="hidden h-5 w-5 shrink-0 text-muted-foreground sm:block" />
        <span className="min-w-0 truncate text-sm font-medium text-foreground sm:text-base">
          {companyName || "Empresa"}
        </span>
        {isDemoMode && (
          <Badge
            variant="outline"
            className="ml-1 hidden shrink-0 border-amber-500 bg-amber-50 text-amber-700 sm:inline-flex"
          >
            <WifiOff className="mr-1 h-3 w-3" />
            Modo demo
          </Badge>
        )}
      </div>

      <div className="flex shrink-0 items-center">
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button
              variant="ghost"
              className="flex h-10 max-w-[40vw] items-center gap-2 px-2 sm:max-w-none sm:px-3"
            >
              <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-full bg-primary text-primary-foreground">
                <User className="h-4 w-4" />
              </div>
              <span className="hidden truncate text-sm text-muted-foreground md:inline">
                {email}
              </span>
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end" className="w-56">
            <div className="px-2 py-1.5">
              <p className="truncate text-sm font-medium">{email}</p>
              <p className="truncate text-xs text-muted-foreground">{companyName}</p>
              {isDemoMode && (
                <p className="mt-1 text-xs text-amber-600">
                  Usando datos de demostración
                </p>
              )}
            </div>
            <DropdownMenuSeparator />
            <DropdownMenuItem onClick={handleChangeCompany}>
              <Building2 className="mr-2 h-4 w-4" />
              Cambiar empresa
            </DropdownMenuItem>
            <DropdownMenuSeparator />
            <DropdownMenuItem onClick={handleLogout} className="text-destructive">
              <LogOut className="mr-2 h-4 w-4" />
              Cerrar sesión
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      </div>
    </header>
  )
}
