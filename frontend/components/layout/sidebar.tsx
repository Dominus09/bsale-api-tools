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
  ScanBarcode,
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
  Truck,
  Crosshair,
  UsersRound,
  FileWarning,
  BookOpen,
  X,
} from "lucide-react"
import { cn } from "@/lib/utils"
import { ANALYTICS_V2_COMPANY_IDS, ANALYTICS_V2_DASHBOARD_PATH } from "@/lib/home-path"
import {
  canAccessDiagnostics,
  canViewUserAdmin,
  hasManagementAccess,
  hasMarginViewAccess,
  hasPromotionsV2ViewAccess,
  staffUserFromLocalStorage,
  type StaffUserLike,
} from "@/lib/permissions"
import { useAuthMe } from "@/lib/use-auth-me"
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
      { href: "/logistica/cargas", label: "Cargas", icon: Truck },
    ],
  },
  {
    title: "Sucursales",
    items: [
      { href: "/sucursales/recepciones", label: "Recepción de Mercadería", icon: ClipboardList },
      { href: "/sucursales/ofertas", label: "Ofertas y Remates", icon: Tag },
      { href: "/sucursales/trazabilidad", label: "Revisión de Góndola", icon: ScanLine },
      { href: "/gondolas", label: "Control de Góndolas", icon: ScanBarcode },
      { href: "/sucursales/etiquetas", label: "Generador de Etiquetas", icon: Printer },
      { href: "/sucursales/etiquetas2", label: "Etiquetas Socios (Beta)", icon: Users },
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
      {
        href: "/logistica/mapa-entregas/PLAN-00044",
        label: "Mapa de entregas",
        icon: MapPin,
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
      { href: "#", label: "Empresas", icon: Building2, disabled: true },
      { href: "#", label: "Configuración", icon: Settings, disabled: true },
    ],
  },
]

function buildNavSectionsForRole(
  role: string | null,
  /** Permisos reales de /auth/me; sin esto no se muestra Usuarios y accesos. */
  authMeStaff: StaffUserLike | null,
) {
  const user = staffUserFromLocalStorage()
  if (role) user.role = role
  const diagnostics = canAccessDiagnostics(user)
  // Solo flags de GET /auth/me — nunca localStorage.role.
  const userAdmin = canViewUserAdmin(authMeStaff)
  const management = hasManagementAccess(user)
  const marginViewer = hasMarginViewAccess(user)
  const promotionsViewer = hasPromotionsV2ViewAccess(user)

  // Analítica V2 solo se ofrece en empresas con BSALE RAW completo; en las
  // demás se conserva el inicio y la navegación actuales. Sin sufijos "V2":
  // las rutas antiguas siguen existiendo, pero ya no duplican el menú.
  const storedCompany = typeof window !== "undefined" ? localStorage.getItem("company_id") : null
  const companyId = storedCompany ? Number(storedCompany) : null
  const v2Company = companyId != null && ANALYTICS_V2_COMPANY_IDS.has(companyId)

  const home: NavItem =
    v2Company && marginViewer
      ? { href: ANALYTICS_V2_DASHBOARD_PATH, label: "Dashboard comercial", icon: LayoutDashboard }
      : { href: "/dashboard", label: "Dashboard", icon: LayoutDashboard }

  const commercial: NavItem[] = []
  if (v2Company) {
    if (management) commercial.push({ href: "/analitica-v2/costos", label: "Control de costos", icon: CircleDollarSign })
    if (marginViewer) commercial.push({ href: "/analitica-v2/margenes", label: "Análisis de márgenes", icon: TrendingUp })
    if (marginViewer) commercial.push({ href: "/analitica-v2/packs", label: "Rentabilidad de packs", icon: Boxes })
    if (marginViewer) commercial.push({ href: "/inteligencia-comercial/catalogo", label: "Catálogo Maestro", icon: BookOpen })
    if (promotionsViewer) commercial.push({ href: "/analitica-v2/promociones", label: "Promociones y ofertas", icon: Percent })
  }

  // Configuración comercial: la política sigue siendo el editor vigente de las
  // reglas que lee Analítica V2; las promociones anteriores alimentan las
  // etiquetas de sucursal, por eso se conservan claramente diferenciadas.
  const configuration: NavItem[] = [
    { href: "/politica-margenes", label: "Política de márgenes", icon: Scale },
    { href: "/promotions", label: "Promociones anteriores · etiquetas", icon: Tag },
  ]

  return navSections.flatMap((section) => {
    if (section.title === "Principal") {
      return [{ ...section, items: [home] }]
    }
    if (section.title === "Analítica") {
      const out: { title: string; items: NavItem[] }[] = []
      if (commercial.length) out.push({ title: "Inteligencia comercial", items: commercial })
      out.push(section, { title: "Configuración comercial", items: configuration })
      return out
    }
    if (section.title === "Administración") {
      const adminItems: NavItem[] = []
      if (userAdmin) {
        adminItems.push({
          href: "/admin/usuarios-accesos",
          label: "Usuarios y accesos",
          icon: Users,
        })
      }
      adminItems.push(...section.items)
      if (diagnostics) {
        adminItems.push({
          href: "/admin/diagnostico",
          label: "Diagnóstico ERP",
          icon: Stethoscope,
        })
      }
      if (!adminItems.length) return []
      return [{ ...section, items: adminItems }]
    }
    return [section]
  })
}

type SidebarProps = {
  compact?: boolean
  onToggleCompact?: () => void
  /**
   * Contexto drawer (móvil/tablet <1024 px): si se pasa, el encabezado
   * muestra SOLO el botón X de cierre (44 px) y jamás la flecha de
   * colapso de escritorio. Ambos controles son mutuamente excluyentes.
   */
  onCloseDrawer?: () => void
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

export function Sidebar({ compact = false, onToggleCompact, onCloseDrawer }: SidebarProps) {
  const pathname = usePathname()
  const [openSection, setOpenSection] = useState<string | null>("Distribuidora")
  const [staffRole, setStaffRole] = useState<string | null>(null)
  const { meState, staff: authMeStaff } = useAuthMe()

  useEffect(() => {
    if (typeof window === "undefined") return
    setStaffRole(localStorage.getItem("role"))
  }, [])

  const sections = useMemo(
    () => buildNavSectionsForRole(staffRole, meState === "ok" ? authMeStaff : null),
    [staffRole, meState, authMeStaff],
  )

  useEffect(() => {
    const active = sections.find((s) => sectionContainsPath(s, pathname))
    if (active) setOpenSection(active.title)
  }, [pathname, sections])

  return (
    <aside
      className={cn(
        "flex h-full shrink-0 flex-col border-r border-sidebar-border bg-sidebar text-sidebar-foreground transition-[width] duration-200 ease-out",
        compact ? "w-[4.25rem]" : "w-64",
      )}
    >
      <div
        className={cn(
          "flex shrink-0 items-center border-b border-white/10",
          compact ? "flex-col gap-2 py-3" : "h-16 gap-2 px-4",
        )}
      >
        <div className={cn("flex items-center gap-2.5", compact ? "justify-center" : "min-w-0 flex-1")}>
          <img
            src="https://hebbkx1anhila5yf.public.blob.vercel-storage.com/Emblema%20auxiliar%20sin%20sucursal-3muphOJR8q7mpoZPwKQhJb7RbLYvdu.png"
            alt="Quillotana"
            className={cn("shrink-0 object-contain", compact ? "h-8 w-8" : "h-10 w-10")}
          />
          {!compact ? (
            <span className="min-w-0">
              <span className="block truncate text-[15px] font-semibold leading-tight text-white">Quillotana ERP</span>
              <span className="block truncate text-[11px] leading-tight text-white/45">Grupo Quillotana</span>
            </span>
          ) : null}
        </div>
        {onCloseDrawer ? (
          // Drawer: único control del encabezado, dentro del flujo (sin
          // superposición) y con área táctil de 44 px.
          <button
            type="button"
            onClick={onCloseDrawer}
            className="ml-auto flex size-11 shrink-0 items-center justify-center rounded-md text-white/70 transition-colors hover:bg-white/10 hover:text-white focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/70"
            aria-label="Cerrar menú lateral"
            title="Cerrar menú"
          >
            <X className="h-5 w-5" aria-hidden />
          </button>
        ) : onToggleCompact ? (
          // Escritorio: único control, flecha de colapso/expansión.
          <button
            type="button"
            onClick={onToggleCompact}
            className={cn(
              "flex size-9 shrink-0 items-center justify-center rounded-md text-white/55 transition-colors hover:bg-white/10 hover:text-white focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/70",
              compact ? "" : "ml-auto",
            )}
            aria-label={compact ? "Expandir menú lateral" : "Modo compacto (solo iconos)"}
            title={compact ? "Expandir menú" : "Solo iconos"}
          >
            {compact ? <ChevronsRight className="h-5 w-5" /> : <ChevronsLeft className="h-5 w-5" />}
          </button>
        ) : null}
      </div>

      <nav className="flex-1 overflow-y-auto overflow-x-hidden px-3 py-4 [scrollbar-color:rgb(255_255_255/0.18)_transparent]">
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
                    "flex cursor-not-allowed items-center rounded-md text-sm font-medium text-white/30",
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
                title={item.label}
                aria-current={isActive ? "page" : undefined}
                className={cn(
                  "relative flex items-center rounded-md text-sm font-medium transition-colors duration-150 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/70",
                  compact ? "justify-center px-2.5 py-2.5" : "gap-3 px-3 py-2.5",
                  isActive
                    ? "bg-white/[0.09] text-white"
                    : "text-[#B7BFC9] hover:bg-white/[0.06] hover:text-white",
                )}
              >
                {isActive ? (
                  <span className="absolute left-0 top-1/2 h-5 w-[3px] -translate-y-1/2 rounded-full bg-primary" aria-hidden="true" />
                ) : null}
                <item.icon className={cn("h-[1.125rem] w-[1.125rem] shrink-0", isActive ? "text-white" : "text-current opacity-70")} />
                {!compact ? (
                  // Sin truncate: las etiquetas largas se leen completas
                  // (pueden ocupar dos líneas) y el `title` nativo las
                  // refuerza; en modo compacto ya existe Tooltip.
                  <span className="min-w-0 flex-1 leading-snug">{item.label}</span>
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
              <div key={section.title} className="mb-3 border-t border-white/[0.07] pt-3 first:border-t-0 first:pt-0">
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
                  "flex w-full cursor-pointer items-center justify-between rounded-md px-3 py-2.5 text-left text-[11px] font-semibold uppercase tracking-[0.14em] text-white/40 transition-colors duration-150",
                  "hover:bg-white/[0.05] hover:text-white/80 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/70",
                )}
              >
                <span>{section.title}</span>
                {isOpen ? (
                  <Minus className="h-3.5 w-3.5 shrink-0 opacity-70" aria-hidden />
                ) : (
                  <Plus className="h-3.5 w-3.5 shrink-0 opacity-70" aria-hidden />
                )}
              </button>
              {isOpen ? (
                <div className="ml-3 mt-1 space-y-0.5 border-l border-white/10 pl-3">
                  {section.items.map((item) => renderItem(item))}
                </div>
              ) : null}
            </div>
          )
        })}
      </nav>

      <div className={cn("border-t border-white/10", compact ? "p-2" : "p-4")}>
        <p
          className={cn(
            "flex items-center gap-2 text-xs text-white/40",
            compact && "sr-only",
          )}
        >
          <span className="h-1.5 w-1.5 shrink-0 rounded-full bg-primary" aria-hidden="true" />
          Grupo Quillotana ERP v1.0
        </p>
      </div>
    </aside>
  )
}
