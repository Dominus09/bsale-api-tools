"use client"

import { FormEvent, Suspense, useMemo, useState } from "react"
import { useRouter, useSearchParams } from "next/navigation"
import { ApiError, fetchMe, login } from "@/lib/api"
import { clearSession, setSession } from "@/lib/auth"
import { BRAND, QUILLOTANA_LOGO_GRUPO_URL } from "@/lib/brand"

function IconMail() {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" aria-hidden>
      <rect x="3" y="5" width="18" height="14" rx="2" />
      <path d="m4 7 8 6 8-6" />
    </svg>
  )
}

function IconLock() {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" aria-hidden>
      <rect x="5" y="11" width="14" height="10" rx="2" />
      <path d="M8 11V8a4 4 0 0 1 8 0v3" />
    </svg>
  )
}

function IconEye({ off }: { off?: boolean }) {
  if (off) {
    return (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" aria-hidden>
        <path d="M3 3l18 18" />
        <path d="M10.6 10.6a2 2 0 0 0 2.8 2.8" />
        <path d="M9.9 5.1A10.4 10.4 0 0 1 12 5c5 0 9.3 3.1 11 7-0.5 1.2-1.2 2.3-2.1 3.2" />
        <path d="M6.1 6.1C4.2 7.4 2.7 9.2 2 12c1.7 3.9 6 7 10 7 1.4 0 2.7-.3 3.9-.8" />
      </svg>
    )
  }
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" aria-hidden>
      <path d="M2 12s3.5-7 10-7 10 7 10 7-3.5 7-10 7S2 12 2 12Z" />
      <circle cx="12" cy="12" r="3" />
    </svg>
  )
}

function IconArrow() {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden>
      <path d="M5 12h14" />
      <path d="m13 6 6 6-6 6" />
    </svg>
  )
}

function LoginForm() {
  const router = useRouter()
  const params = useSearchParams()
  const [email, setEmail] = useState("")
  const [password, setPassword] = useState("")
  const [showPass, setShowPass] = useState(false)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [logoOk, setLogoOk] = useState(true)

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
    <main className="login-shell">
      <div className="card login-card">
        <div className="login-brand">
          {logoOk ? (
            // eslint-disable-next-line @next/next/no-img-element
            <img
              className="login-logo"
              src={QUILLOTANA_LOGO_GRUPO_URL}
              alt="Grupo La Quillotana"
              onError={() => setLogoOk(false)}
            />
          ) : (
            <p className="muted" style={{ margin: "0 0 8px", fontSize: "0.8rem" }}>
              Logo no disponible en este dispositivo
            </p>
          )}
          <h1>{BRAND.name}</h1>
          <p className="login-sub">{BRAND.subtitle}</p>
        </div>

        {hint ? <div className="banner banner-info">{hint}</div> : null}
        {error ? <div className="banner banner-error">{error}</div> : null}

        <form onSubmit={onSubmit} style={{ marginTop: 16 }}>
          <div className="field">
            <label htmlFor="email">Correo</label>
            <div className="field-icon-wrap">
              <span className="field-icon">
                <IconMail />
              </span>
              <input
                id="email"
                type="email"
                autoComplete="username"
                inputMode="email"
                enterKeyHint="next"
                placeholder="correo@empresa.cl"
                value={email}
                onChange={(ev) => setEmail(ev.target.value)}
                required
                disabled={busy}
              />
            </div>
          </div>
          <div className="field">
            <label htmlFor="password">Contraseña</label>
            <div className="field-icon-wrap password-wrap">
              <span className="field-icon">
                <IconLock />
              </span>
              <input
                id="password"
                type={showPass ? "text" : "password"}
                autoComplete="current-password"
                enterKeyHint="go"
                placeholder="••••••••"
                value={password}
                onChange={(ev) => setPassword(ev.target.value)}
                required
                disabled={busy}
              />
              <button
                type="button"
                className="password-toggle"
                onClick={() => setShowPass((v) => !v)}
                aria-pressed={showPass}
                aria-label={showPass ? "Ocultar contraseña" : "Mostrar contraseña"}
              >
                <IconEye off={showPass} />
              </button>
            </div>
          </div>
          <button className="btn btn-primary btn-with-arrow" type="submit" disabled={busy}>
            {busy ? "Ingresando…" : (
              <>
                Ingresar
                <IconArrow />
              </>
            )}
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
