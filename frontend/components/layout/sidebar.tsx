"use client"

import Link from "next/link"
import { usePathname } from "next/navigation"
import { useState } from "react"
import {
  LayoutDashboard,
  TrendingUp,
  AlertTriangle,
  Store,
  Package,
  ShoppingCart,
  Warehouse,
  DollarSign,
  Users,
  Settings,
  ChevronRight,
  ChevronDown,
  ClipboardList,
  Tag,
  ScanLine,
  Printer,
  BarChart3,
  Building2,
  Boxes,
  Brain,
} from "lucide-react"
import { cn } from "@/lib/utils"

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
      { href: "/compras/inteligencia", label: "Compras inteligentes", icon: Brain },
      { href: "/compras/proveedores", label: "Proveedores", icon: Store },
      { href: "/compras/productos", label: "Productos", icon: Boxes },
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

export function Sidebar() {
  const pathname = usePathname()

  return (
    <aside className="flex h-full w-64 flex-col border-r border-border bg-card">
      <div className="flex h-16 items-center gap-2 border-b border-border px-4">
        <img
          src="https://hebbkx1anhila5yf.public.blob.vercel-storage.com/Emblema%20auxiliar%20sin%20sucursal-3muphOJR8q7mpoZPwKQhJb7RbLYvdu.png"
          alt="Quillotana"
          className="h-10 w-10 object-contain"
        />
        <span className="font-semibold text-foreground">Quillotana ERP</span>
      </div>

      <nav className="flex-1 overflow-y-auto p-3">
        {navSections.map((section) => (
          <div key={section.title} className="mb-4">
            <div className="mb-2 px-3 text-xs font-medium uppercase tracking-wider text-muted-foreground">
              {section.title}
            </div>
            <div className="space-y-1">
              {section.items.map((item) => {
                const isActive = pathname === item.href || pathname.startsWith(item.href + "/")
                const isDisabled = item.disabled

                if (isDisabled) {
                  return (
                    <div
                      key={item.label}
                      className="flex cursor-not-allowed items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium text-muted-foreground/50"
                    >
                      <item.icon className="h-4 w-4" />
                      {item.label}
                    </div>
                  )
                }

                return (
                  <Link
                    key={item.href}
                    href={item.href}
                    className={cn(
                      "flex items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium transition-colors",
                      isActive
                        ? "bg-primary text-primary-foreground"
                        : "text-muted-foreground hover:bg-accent hover:text-accent-foreground"
                    )}
                  >
                    <item.icon className="h-4 w-4" />
                    {item.label}
                    {isActive && <ChevronRight className="ml-auto h-4 w-4" />}
                  </Link>
                )
              })}
            </div>
          </div>
        ))}
      </nav>

      <div className="border-t border-border p-4">
        <p className="text-xs text-muted-foreground">Grupo Quillotana ERP v1.0</p>
      </div>
    </aside>
  )
}
