"use client"

import { useEffect, useMemo, useState } from "react"
import Link from "next/link"
import { usePathname } from "next/navigation"
import {
  LayoutDashboard,
  TrendingUp,
  AlertTriangle,
  Package,
  ShoppingCart,
  Warehouse,
  Users,
  Settings,
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
  Route,
  CalendarDays,
  History,
  Scale,
  CircleDollarSign,
  ChevronsLeft,
  ChevronsRight,
  Percent,
  UserCircle2,
  Stethoscope,
  PackageCheck,
  Crosshair,
  UsersRound,
  FileWarning,
} from "lucide-react"
import { cn } from "@/lib/utils"
import { canAccessDiagnostics, staffUserFromLocalStorage } from "@/lib/permissions"
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
      { href: "/politica-margenes", label: "Política de Márgenes", icon: Scale },
      { href: "/costos", label: "Control de costos", icon: CircleDollarSign },
      { href: "/promotions", label: "Promociones", icon: Percent },
      {
        href: "/analitica/comercial-vendedores",
        label: "Comercial Vendedores",
        icon: UsersRound,
      },
      {
        href: "/analitica/notas-credito",
        label: "Notas de Crédito",
        icon: FileWarning,
      },
    ],
  },
  {
    title: "Logística",
    items: [
      {
        href: "/distribuidora/maestro-logistico",
        label: "Productos Logísticos",
        icon: Package,
      },
      { href: "/logistica/peso-ordenes", label: "Peso de Órdenes", icon: PackageCheck },
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
      { href: "/distribuidora/dashboard", label: "Dashboard comercial", icon: BarChart3 },
      { href: "/distribuidora/ordenes-compra", label: "Órdenes de compra", icon: ShoppingCart },
      { href: "/distribuidora/orders", label: "Pre‑despacho OC", icon: PackageCheck },
      { href: "/distribuidora/planificacion", label: "Planif. mapa ORS", icon: MapPin },
      { href: "/distribuidora/planificaciones", label: "Planificaciones", icon: History },
      { href: "/distribuidora/cuadraturas", label: "Cuadraturas", icon: Scale },
      { href: "/distribuidora/rutero", label: "Rutero vendedores", icon: Route },
      { href: "/distribuidora/mapa", label: "Mapa rutero", icon: MapPin },
      {
        href: "/distribuidora/resumen-vendedor",
        label: "Resumen vendedor",
        icon: CalendarDays,
      },
    ],
  },
  {
    title: "Operaciones",
    items: [
      { href: "/operaciones/dashboard", label: "Panel operaciones", icon: LayoutDashboard },
      { href: "/operaciones/vendedores", label: "Vendedores en ruta", icon: UserCircle2 },
      { href: "/operaciones/mapa", label: "Mapa operacional", icon: MapPin },
      { href: "/operaciones/incidencias", label: "Incidencias", icon: AlertTriangle },
      { href: "/operaciones/georreferencias", label: "Georreferencias", icon: Crosshair },
      { href: "/orders", label: "Pedidos", icon: Package },
      { href: "#", label: "Stock", icon: Warehouse, disabled: true },
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

function buildNavSectionsForRole(role: string | null) {
  const user = staffUserFromLocalStorage()
  if (role) user.role = role
  if (!canAccessDiagnostics(user)) return navSections
  return navSections.map((section) => {
    if (section.title !== "Administración") return section
    return {
      ...section,
      items: [
        ...section.items,
        { href: "/admin/diagnostico", label: "Diagnóstico ERP", icon: Stethoscope },
      ],
    }
  })
}

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
  const [staffRole, setStaffRole] = useState<string | null>(null)

  useEffect(() => {
    if (typeof window === "undefined") return
    setStaffRole(localStorage.getItem("role"))
  }, [])

  const sections = useMemo(() => buildNavSectionsForRole(staffRole), [staffRole])

  useEffect(() => {
    const active = sections.find((s) => sectionContainsPath(s, pathname))
    if (active) setOpenSection(active.title)
  }, [pathname, sections])

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

      <nav className="flex-1 overflow-y-auto overflow-x-hidden px-3 py-3">
        {sections.map((section) => {
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
                    "flex cursor-not-allowed items-center rounded-md text-sm font-medium text-muted-foreground/50",
                    compact ? "justify-center px-2.5 py-2.5" : "gap-3 px-3 py-2.5",
                  )}
                >
                  <item.icon className="h-[1.125rem] w-[1.125rem] shrink-0" />
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
                  "flex items-center rounded-md text-sm font-medium transition-colors duration-150",
                  compact ? "justify-center px-2.5 py-2.5" : "gap-3 px-3 py-2.5",
                  isActive
                    ? "bg-primary/10 text-primary shadow-none"
                    : "text-muted-foreground hover:bg-muted/70 hover:text-foreground",
                )}
              >
                <item.icon className="h-[1.125rem] w-[1.125rem] shrink-0" />
                {!compact ? (
                  <span className="min-w-0 flex-1 truncate leading-snug">{item.label}</span>
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
            <div key={section.title} className="mb-2">
              <button
                type="button"
                aria-expanded={isOpen}
                onClick={() =>
                  setOpenSection((prev) => (prev === section.title ? null : section.title))
                }
                className={cn(
                  "flex w-full cursor-pointer items-center justify-between rounded-md px-3 py-2.5 text-left text-xs font-semibold uppercase tracking-wider text-muted-foreground transition-colors duration-150",
                  "hover:bg-muted/60 hover:text-foreground",
                )}
              >
                <span className="truncate">{section.title}</span>
                {isOpen ? (
                  <Minus className="h-3.5 w-3.5 shrink-0 opacity-70" aria-hidden />
                ) : (
                  <Plus className="h-3.5 w-3.5 shrink-0 opacity-70" aria-hidden />
                )}
              </button>
              {isOpen ? (
                <div className="mt-1 space-y-0.5 border-l-2 border-border/70 pl-3 ml-3">
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
