"use client"

import { useEffect, useState } from "react"
import { LogOut, Building2, User, Wifi, WifiOff } from "lucide-react"
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

export function Header() {
  const [email, setEmail] = useState<string | null>(null)
  const [companyName, setCompanyName] = useState<string | null>(null)
  const [isDemoMode, setIsDemoMode] = useState(false)

  useEffect(() => {
    initDemoMode()
    setEmail(getStoredEmail())
    setCompanyName(getStoredCompanyName())
    setIsDemoMode(getIsDemoMode())

    // Check for demo mode changes periodically
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
    <header className="flex h-16 items-center justify-between border-b border-border bg-card px-6">
      <div className="flex items-center gap-3">
        <Building2 className="h-5 w-5 text-muted-foreground" />
        <span className="font-medium text-foreground">
          {companyName || "Selecciona una empresa"}
        </span>
        {isDemoMode && (
          <Badge variant="outline" className="ml-2 border-amber-500 bg-amber-50 text-amber-700">
            <WifiOff className="mr-1 h-3 w-3" />
            Modo demo
          </Badge>
        )}
      </div>

      <div className="flex items-center gap-4">
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button variant="ghost" className="flex items-center gap-2">
              <div className="flex h-8 w-8 items-center justify-center rounded-full bg-primary text-primary-foreground">
                <User className="h-4 w-4" />
              </div>
              <span className="text-sm text-muted-foreground">{email}</span>
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end" className="w-56">
            <div className="px-2 py-1.5">
              <p className="text-sm font-medium">{email}</p>
              <p className="text-xs text-muted-foreground">{companyName}</p>
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
