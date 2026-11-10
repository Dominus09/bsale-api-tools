import { NextRequest, NextResponse } from "next/server"
import {
  CONNECT_ERROR_ES,
  getApiProxyTarget,
  isLikelyPublicApiHost,
} from "@/lib/api-proxy-target"

export const runtime = "nodejs"
export const dynamic = "force-dynamic"

const HOP_BY_HOP = new Set([
  "connection",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailers",
  "transfer-encoding",
  "upgrade",
  "host",
  "content-length",
])

function buildUpstreamUrl(req: NextRequest, pathParts: string[]): string {
  const base = getApiProxyTarget()
  const path = pathParts.map(encodeURIComponent).join("/")
  const qs = req.nextUrl.search || ""
  return `${base}/${path}${qs}`
}

function forwardRequestHeaders(req: NextRequest): Headers {
  const out = new Headers()
  req.headers.forEach((value, key) => {
    const k = key.toLowerCase()
    if (HOP_BY_HOP.has(k)) return
    // Evitar que el upstream vea el host de la PWA.
    if (k === "host") return
    out.set(key, value)
  })
  return out
}

function forwardResponseHeaders(res: Response): Headers {
  const out = new Headers()
  res.headers.forEach((value, key) => {
    const k = key.toLowerCase()
    if (HOP_BY_HOP.has(k)) return
    // No cachear respuestas de API en el edge/browser vía proxy.
    if (k === "set-cookie") {
      out.append(key, value)
      return
    }
    out.set(key, value)
  })
  return out
}

let warnedPublicUpstream = false

async function proxy(req: NextRequest, pathParts: string[]): Promise<NextResponse> {
  const upstream = buildUpstreamUrl(req, pathParts)
  if (!warnedPublicUpstream && isLikelyPublicApiHost(upstream)) {
    warnedPublicUpstream = true
    console.warn(
      "[pwa:api-upstream] API_PROXY_TARGET ausente o público; riesgo EAI_AGAIN. Configure URL interna Docker.",
    )
  }
  const method = req.method.toUpperCase()
  const headers = forwardRequestHeaders(req)

  let body: ArrayBuffer | undefined
  if (method !== "GET" && method !== "HEAD") {
    body = await req.arrayBuffer()
  }

  try {
    // Un solo intento: no reintentar POST (entrega/cobranza/firma) sin idempotencia.
    const res = await fetch(upstream, {
      method,
      headers,
      body: body && body.byteLength > 0 ? body : undefined,
      redirect: "manual",
      cache: "no-store",
    })
    return new NextResponse(res.body, {
      status: res.status,
      statusText: res.statusText,
      headers: forwardResponseHeaders(res),
    })
  } catch (err) {
    const code =
      err && typeof err === "object" && "code" in err
        ? String((err as { code?: string }).code || "")
        : ""
    const msg = err instanceof Error ? err.message : String(err)
    console.error("[pwa:api-upstream]", {
      method,
      upstream_host: (() => {
        try {
          return new URL(upstream).host
        } catch {
          return "invalid"
        }
      })(),
      error_code: code || undefined,
      error_kind: /EAI_AGAIN|ENOTFOUND|getaddrinfo/i.test(msg + code)
        ? "dns"
        : /ECONNREFUSED|ECONNRESET|ETIMEDOUT|fetch failed/i.test(msg + code)
          ? "connect"
          : "unknown",
      // Sin JWT, body ni paths con PII.
    })
    return NextResponse.json(
      { detail: CONNECT_ERROR_ES },
      { status: 503, headers: { "Cache-Control": "no-store" } },
    )
  }
}

type Ctx = { params: Promise<{ path: string[] }> }

async function handle(req: NextRequest, ctx: Ctx) {
  const { path } = await ctx.params
  const parts = Array.isArray(path) ? path : [path]
  return proxy(req, parts.filter(Boolean))
}

export const GET = handle
export const POST = handle
export const PUT = handle
export const PATCH = handle
export const DELETE = handle
export const OPTIONS = handle
export const HEAD = handle
