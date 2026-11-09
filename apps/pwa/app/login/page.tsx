"use client"

import { FormEvent, Suspense, useMemo, useState } from "react"
import { useRouter, useSearchParams } from "next/navigation"
import { ApiError, fetchMe, login } from "@/lib/api"
import { clearSession, setSession } from "@/lib/auth"

function LoginForm() {
  const router = useRouter()
  const params = useSearchParams()
  const [email, setEmail] = useState("")
  const [password, setPassword] = useState("")
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const hint = useMemo(() => {
    if (params.get("expired")) return "Tu sesión expiró. Ingresa de nuevo."
    if (params.get("denied") === "pwa") {
      return "Esta cuenta no tiene acceso a la app operativa."
    }
    if (params.get("denied") === "profile") {
      return "Se requiere perfil chofer para continuar."
    }
    return null
  }, [params])

  async function onSubmit(e: FormEvent) {
    e.preventDefault()
    setBusy(true)
    setError(null)
    try {
      clearSession()
      const res = await login(email.trim(), password)
      setSession(res.token, res.email, res.id)
      const me = await fetchMe()
      if (!me.has_pwa_access) {
        clearSession()
        setError("Cuenta sin acceso PWA. El ERP administrativo no se otorga aquí.")
        return
      }
      if (!me.profiles.includes("chofer")) {
        clearSession()
        setError("Tu usuario no tiene perfil chofer activo.")
        return
      }
      router.replace("/despachos")
    } catch (err) {
      clearSession()
      if (err instanceof ApiError) {
        setError(err.status === 401 ? "Credenciales inválidas" : err.message)
      } else {
        setError("No se pudo iniciar sesión. Revisa la conexión.")
      }
    } finally {
      setBusy(false)
    }
  }

  return (
    <main className="main">
      <div className="card">
        <h2>Quillotana Operativa</h2>
        <p className="muted">Ingreso para choferes · app.quillotana.cl</p>
        {hint ? <div className="banner banner-info" style={{ marginTop: 12 }}>{hint}</div> : null}
        {error ? <div className="banner banner-error" style={{ marginTop: 12 }}>{error}</div> : null}
        <form onSubmit={onSubmit} style={{ marginTop: 16 }}>
          <div className="field">
            <label htmlFor="email">Correo</label>
            <input
              id="email"
              type="email"
              autoComplete="username"
              value={email}
              onChange={(ev) => setEmail(ev.target.value)}
              required
            />
          </div>
          <div className="field">
            <label htmlFor="password">Contraseña</label>
            <input
              id="password"
              type="password"
              autoComplete="current-password"
              value={password}
              onChange={(ev) => setPassword(ev.target.value)}
              required
            />
          </div>
          <button className="btn btn-primary" type="submit" disabled={busy}>
            {busy ? "Ingresando…" : "Ingresar"}
          </button>
        </form>
      </div>
    </main>
  )
}

export default function LoginPage() {
  return (
    <Suspense fallback={<div className="empty">Cargando…</div>}>
      <LoginForm />
    </Suspense>
  )
}
