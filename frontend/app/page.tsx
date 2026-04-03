"use client"

import { useEffect } from "react"
import { useRouter } from "next/navigation"
import { Loader2 } from "lucide-react"
import { isAuthenticated } from "@/lib/api"

export default function Home() {
  const router = useRouter()

  useEffect(() => {
    if (isAuthenticated()) {
      const companyId = localStorage.getItem("company_id")
      if (companyId) {
        router.push("/dashboard")
      } else {
        router.push("/company-selector")
      }
    } else {
      router.push("/login")
    }
  }, [router])

  return (
    <div className="flex min-h-screen items-center justify-center bg-background">
      <div className="flex flex-col items-center gap-4">
        <img
          src="https://hebbkx1anhila5yf.public.blob.vercel-storage.com/Emblema%20auxiliar%20sin%20sucursal-3muphOJR8q7mpoZPwKQhJb7RbLYvdu.png"
          alt="Quillotana"
          className="h-16 w-16 object-contain"
        />
        <Loader2 className="h-6 w-6 animate-spin text-primary" />
        <p className="text-sm text-muted-foreground">Cargando...</p>
      </div>
    </div>
  )
}
