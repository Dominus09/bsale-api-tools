"use client"

import { useEffect, useState } from "react"
import { useRouter } from "next/navigation"
import { Building2, Loader2, ArrowRight } from "lucide-react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { getCompanies, isAuthenticated, type Company } from "@/lib/api"
import { preferredHomePath } from "@/lib/home-path"

export default function CompanySelectorPage() {
  const router = useRouter()
  const [companies, setCompanies] = useState<Company[]>([])
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!isAuthenticated()) {
      router.push("/login")
      return
    }

    async function loadCompanies() {
      try {
        const data = await getCompanies()
        setCompanies(data)
      } catch (err) {
        setError(err instanceof Error ? err.message : "Error al cargar empresas")
      } finally {
        setIsLoading(false)
      }
    }

    loadCompanies()
  }, [router])

  const handleSelectCompany = (company: Company) => {
    localStorage.setItem("company_id", company.company_id.toString())
    localStorage.setItem("company_name", company.name)
    const user = { role: localStorage.getItem("role"), email: localStorage.getItem("email") }
    router.push(preferredHomePath(company.company_id, user))
  }

  if (isLoading) {
    return (
      <div className="flex min-h-dvh items-center justify-center bg-background">
        <div className="flex flex-col items-center gap-4">
          <Loader2 className="h-8 w-8 animate-spin text-primary" />
          <p className="text-muted-foreground">Cargando empresas...</p>
        </div>
      </div>
    )
  }

  return (
    <div
      className="flex min-h-dvh flex-col items-center justify-center bg-background px-4 py-4"
      style={{
        paddingTop: "max(1rem, env(safe-area-inset-top))",
        paddingBottom: "max(1rem, env(safe-area-inset-bottom))",
      }}
    >
      <div className="mb-8 flex flex-col items-center gap-4 text-center">
        <img
          src="https://hebbkx1anhila5yf.public.blob.vercel-storage.com/Emblema%20auxiliar%20sin%20sucursal-3muphOJR8q7mpoZPwKQhJb7RbLYvdu.png"
          alt="Quillotana"
          className="h-20 w-20 object-contain"
        />
        <h1 className="text-[28px] font-semibold leading-8 tracking-[-0.02em] text-foreground sm:text-[32px] sm:leading-9">Selecciona una empresa</h1>
        <p className="text-muted-foreground">Elige la empresa con la que deseas trabajar</p>
      </div>

      {error && (
        <div className="mb-6 rounded-lg bg-destructive/10 p-4 text-destructive">
          {error}
        </div>
      )}

      <div className="grid w-full max-w-3xl gap-4 md:grid-cols-2 lg:grid-cols-3">
        {companies.map((company) => (
          <Card
            key={company.company_id}
            className="cursor-pointer transition-all hover:border-primary hover:shadow-md"
            onClick={() => handleSelectCompany(company)}
          >
            <CardHeader className="pb-3">
              <div className="flex h-12 w-12 items-center justify-center rounded-lg bg-primary/10">
                <Building2 className="h-6 w-6 text-primary" />
              </div>
            </CardHeader>
            <CardContent>
              <CardTitle className="mb-1 text-lg">{company.name}</CardTitle>
              <CardDescription className="flex items-center gap-1">
                Ingresar <ArrowRight className="h-3 w-3" />
              </CardDescription>
            </CardContent>
          </Card>
        ))}
      </div>

      {companies.length === 0 && !error && (
        <Card className="w-full max-w-md">
          <CardContent className="flex flex-col items-center py-8">
            <Building2 className="mb-4 h-12 w-12 text-muted-foreground" />
            <p className="text-center text-muted-foreground">
              No tienes empresas asignadas.
              <br />
              Contacta al administrador.
            </p>
          </CardContent>
        </Card>
      )}
    </div>
  )
}
