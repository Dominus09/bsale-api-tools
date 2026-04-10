"use client"

import { useEffect, useState } from "react"
import Link from "next/link"
import { usePathname } from "next/navigation"
import {
  LayoutDashboard,
  TrendingUp,
  AlertTriangle,
  Package,
  ShoppingCart,
  Warehouse,
  DollarSign,
  Users,
  Settings,
  ChevronRight,
  Plus,
  Minus,
  ClipboardList,
  Tag,
  ScanLine,
  Printer,
  BarChart3,
  Building2,
  Boxes,
  FileSpreadsheet,
  ScrollText,
  Store,
  MapPin,
  MapPinOff,
  Route,
  ClipboardList,
  Wallet,
  ChevronsLeft,
  ChevronsRight,
} from "lucide-react"
import { cn } from "@/lib/utils"
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip"

type NavItem = {
  href: string
  label: string
  icon: React.ElementType
  disabled?: boolean
  children?: NavItem[]
}

const navSections: { title: string; items: NavItem[] }[] = [
  {
    title: "Principal",
    items: [{ href: "/dashboard", label: "Dashboard", icon: LayoutDashboard }],
  },
  {
    title: "Analítica",
    items: [
      { href: "/margins", label: "Márgenes", icon: TrendingUp },
      { href: "/alerts", label: "Alertas", icon: AlertTriangle },
    ],
  },
  {
    title: "Sucursales",
    items: [
      { href: "/sucursales/recepciones", label: "Recepción de Mercadería", icon: ClipboardList },
      { href: "/sucursales/ofertas", label: "Ofertas y Remates", icon: Tag },
      { href: "/sucursales/trazabilidad", label: "Revisión de Góndola", icon: ScanLine },
      { href: "/sucursales/etiquetas", label: "Generador de Etiquetas", icon: Printer },
    ],
  },
  {
    title: "Compras",
    items: [
      { href: "/compras/generar-oc", label: "Generar OC", icon: FileSpreadsheet },
      { href: "/compras/registros-oc", label: "Registros OC", icon: ScrollText },
      { href: "/compras/proveedores", label: "Proveedores", icon: Store },
      { href: "/compras/productos", label: "Productos", icon: Boxes },
    ],
  },
  {
    title: "Distribuidora",
    items: [
      { href: "/distribuidora/mapa", label: "Mapa rutero", icon: MapPin },
      { href: "/distribuidora/rutero", label: "Rutero", icon: Route },
      { href: "/distribuidora/pendientes", label: "Pendientes", icon: ClipboardList },
      { href: "/distribuidora/sin-georef", label: "Sin georef", icon: MapPinOff },
      { href: "/distribuidora/viaticos", label: "Viáticos", icon: Wallet },
    ],
  },
  {
    title: "Operaciones",
    items: [
      { href: "/orders", label: "Pedidos", icon: Package },
      { href: "#", label: "Stock", icon: Warehouse, disabled: true },
      { href: "#", label: "Compras", icon: ShoppingCart, disabled: true },
      { href: "#", label: "Ventas", icon: DollarSign, disabled: true },
    ],
  },
  {
    title: "Finanzas",
    items: [{ href: "#", label: "Finanzas", icon: BarChart3, disabled: true }],
  },
  {
    title: "Administración",
    items: [
      { href: "#", label: "Usuarios", icon: Users, disabled: true },
      { href: "#", label: "Empresas", icon: Building2, disabled: true },
      { href: "#", label: "Configuración", icon: Settings, disabled: true },
    ],
  },
]

type SidebarProps = {
  compact?: boolean
  onToggleCompact?: () => void
}

function sectionContainsPath(
  section: (typeof navSections)[number],
  pathname: string,
): boolean {
  return section.items.some((item) => {
    if (item.disabled || !item.href || item.href === "#") return false
    return pathname === item.href || pathname.startsWith(item.href + "/")
  })
}

export function Sidebar({ compact = false, onToggleCompact }: SidebarProps) {
  const pathname = usePathname()
  const [openSection, setOpenSection] = useState<string | null>("Distribuidora")

  useEffect(() => {
    const active = navSections.find((s) => sectionContainsPath(s, pathname))
    if (active) setOpenSection(active.title)
  }, [pathname])

  return (
    <aside
      className={cn(
        "flex h-full shrink-0 flex-col border-r border-border bg-card transition-[width] duration-200 ease-out",
        compact ? "w-[4.25rem]" : "w-64",
      )}
    >
      <div
        className={cn(
          "flex shrink-0 items-center border-b border-border",
          compact ? "flex-col gap-2 py-3" : "h-16 gap-2 px-4",
        )}
      >
        <div className={cn("flex items-center gap-2", compact ? "justify-center" : "min-w-0 flex-1")}>
          <img
            src="https://hebbkx1anhila5yf.public.blob.vercel-storage.com/Emblema%20auxiliar%20sin%20sucursal-3muphOJR8q7mpoZPwKQhJb7RbLYvdu.png"
            alt="Quillotana"
            className={cn("shrink-0 object-contain", compact ? "h-8 w-8" : "h-10 w-10")}
          />
          {!compact ? (
            <span className="min-w-0 truncate font-semibold text-foreground">Quillotana ERP</span>
          ) : null}
        </div>
        {onToggleCompact ? (
          <button
            type="button"
            onClick={onToggleCompact}
            className={cn(
              "flex size-9 shrink-0 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-accent hover:text-foreground",
              compact ? "" : "ml-auto",
            )}
            aria-label={compact ? "Expandir menú lateral" : "Modo compacto (solo iconos)"}
            title={compact ? "Expandir menú" : "Solo iconos"}
          >
            {compact ? <ChevronsRight className="h-5 w-5" /> : <ChevronsLeft className="h-5 w-5" />}
          </button>
        ) : null}
      </div>

      <nav className="flex-1 overflow-y-auto overflow-x-hidden p-2">
        {navSections.map((section) => {
          const renderItem = (item: NavItem) => {
            const isActive =
              !item.disabled &&
              item.href !== "#" &&
              (pathname === item.href || pathname.startsWith(item.href + "/"))
            const isDisabled = item.disabled

            if (isDisabled) {
              const disabledNode = (
                <div
                  className={cn(
                    "flex cursor-not-allowed items-center rounded-lg text-sm font-medium text-muted-foreground/50",
                    compact ? "justify-center px-2 py-2.5" : "gap-3 px-3 py-2",
                  )}
                >
                  <item.icon className="h-4 w-4 shrink-0" />
                  {!compact ? item.label : null}
                </div>
              )
              if (compact) {
                return (
                  <Tooltip key={item.label}>
                    <TooltipTrigger asChild>{disabledNode}</TooltipTrigger>
                    <TooltipContent side="right">{item.label} (próximamente)</TooltipContent>
                  </Tooltip>
                )
              }
              return <div key={item.label}>{disabledNode}</div>
            }

            const linkInner = (
              <Link
                href={item.href}
                className={cn(
                  "flex items-center rounded-lg text-sm font-medium transition-colors",
                  compact ? "justify-center px-2 py-2.5" : "gap-3 px-3 py-2",
                  isActive
                    ? "bg-primary text-primary-foreground"
                    : "text-muted-foreground hover:bg-accent hover:text-accent-foreground",
                )}
              >
                <item.icon className="h-4 w-4 shrink-0" />
                {!compact ? (
                  <>
                    <span className="min-w-0 flex-1 truncate">{item.label}</span>
                    {isActive ? <ChevronRight className="ml-auto h-4 w-4 shrink-0" /> : null}
                  </>
                ) : null}
              </Link>
            )

            if (compact) {
              return (
                <Tooltip key={item.href}>
                  <TooltipTrigger asChild>{linkInner}</TooltipTrigger>
                  <TooltipContent side="right">{item.label}</TooltipContent>
                </Tooltip>
              )
            }

            return <div key={item.href}>{linkInner}</div>
          }

          if (compact) {
            return (
              <div key={section.title} className="mb-4">
                <div className="sr-only">{section.title}</div>
                <div className="space-y-1">{section.items.map((item) => renderItem(item))}</div>
              </div>
            )
          }

          const isOpen = openSection === section.title

          return (
            <div key={section.title} className="mb-1">
              <button
                type="button"
                aria-expanded={isOpen}
                onClick={() =>
                  setOpenSection((prev) => (prev === section.title ? null : section.title))
                }
                className={cn(
                  "flex w-full cursor-pointer items-center justify-between rounded-lg px-3 py-2 text-left text-xs font-medium uppercase tracking-wider text-muted-foreground transition-colors",
                  "hover:bg-accent hover:text-accent-foreground",
                )}
              >
                <span className="truncate">{section.title}</span>
                {isOpen ? (
                  <Minus className="h-4 w-4 shrink-0 text-muted-foreground" aria-hidden />
                ) : (
                  <Plus className="h-4 w-4 shrink-0 text-muted-foreground" aria-hidden />
                )}
              </button>
              {isOpen ? (
                <div className="mt-1 space-y-1 border-l border-border pl-2 ml-2">
                  {section.items.map((item) => renderItem(item))}
                </div>
              ) : null}
            </div>
          )
        })}
      </nav>

      <div className={cn("border-t border-border", compact ? "p-2" : "p-4")}>
        <p
          className={cn(
            "text-xs text-muted-foreground",
            compact && "sr-only",
          )}
        >
          Grupo Quillotana ERP v1.0
        </p>
      </div>
    </aside>
  )
}
