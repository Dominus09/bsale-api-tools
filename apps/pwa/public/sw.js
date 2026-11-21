/* Quillotana Operativa PWA — SW offline-safe (Safari).
 * - NUNCA responde null/undefined a respondWith.
 * - NUNCA cachea /api-upstream, /api, /login ni Authorization.
 * - NUNCA sirve HTML de otra ruta profunda como APP_SHELL (Next hidrata mal).
 * - NUNCA guarda ni devuelve Responses con redirección (Safari:
 *   "Response served by service worker has redirections").
 * - NUNCA sirve HTML genérico como JS / RSC.
 * - Navegación: network → HTML exacto limpio en cache → offline.html → sintético.
 */
const VERSION = "lq-pwa-v9-safari-no-redirect";
const STATIC_CACHE = `${VERSION}-static`;
const PAGE_CACHE = `${VERSION}-pages`;
const ASSET_CACHE = `${VERSION}-assets`;
const OFFLINE_HTML = "/offline.html";
const OFFLINE_ROUTE = "/offline";
/** Shell de app para arranque frío SOLO cuando la URL pedida es /despachos o /. */
const APP_SHELL = "/despachos";

const PRECACHE = [
  OFFLINE_HTML,
  OFFLINE_ROUTE,
  APP_SHELL,
  "/offline-build.json",
  "/manifest.webmanifest",
  "/icons/pwa/icon-192.png",
  "/icons/pwa/icon-512.png",
  "/icons/pwa/icon-maskable-512.png",
  "/icons/pwa/apple-touch-icon.png",
];

const FALLBACK_HTML = `<!doctype html><html lang="es"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/><title>Sin conexión</title><style>body{margin:0;background:#0b0d12;color:#f3f4f6;font-family:system-ui,sans-serif;padding:24px}a{color:#ffb4bc}</style></head><body><h1>Sin conexión</h1><p>No hay red. Si ya sincronizaste un despacho, abre <a href="/despachos">Mis despachos</a> para consultar fichas (Offline V1). Las capturas Offline V2 usan datos ya autorizados en este dispositivo; la sincronización requiere internet.</p></body></html>`;

function isApiPath(pathname) {
  return (
    pathname.startsWith("/api-upstream") ||
    pathname.startsWith("/api/") ||
    pathname === "/api" ||
    pathname === "/login" ||
    pathname.startsWith("/login/")
  );
}

function isLoginPath(pathname) {
  return pathname === "/login" || pathname.startsWith("/login/");
}

function syntheticOfflineHtml() {
  return new Response(FALLBACK_HTML, {
    status: 200,
    statusText: "OK",
    headers: {
      "Content-Type": "text/html; charset=utf-8",
      "Cache-Control": "no-store",
      "X-LQ-Offline-Fallback": "synthetic",
    },
  });
}

function syntheticOfflineText() {
  return new Response("Sin conexión", {
    status: 503,
    statusText: "Offline",
    headers: {
      "Content-Type": "text/plain; charset=utf-8",
      "Cache-Control": "no-store",
    },
  });
}

function missingOfflineChunkResponse() {
  return new Response(null, {
    status: 503,
    statusText: "Offline chunk missing",
    headers: {
      "Cache-Control": "no-store",
      "X-LQ-Offline-Chunk-Missing": "1",
    },
  });
}

/**
 * Safari WebKit: una Response con response.redirected === true NO puede
 * devolverse desde el SW en navegación. Reconstruimos cuerpo+headers limpios.
 */
async function asCleanResponse(response, extraHeaders) {
  if (!response) return null;
  if (response.type === "opaqueredirect" || response.type === "error") {
    return null;
  }
  if (response.status >= 300 && response.status < 400) {
    return null;
  }
  try {
    const buf = await response.clone().arrayBuffer();
    const headers = new Headers(response.headers);
    if (extraHeaders) {
      for (const [k, v] of Object.entries(extraHeaders)) {
        headers.set(k, v);
      }
    }
    headers.delete("Content-Encoding");
    headers.delete("Content-Length");
    return new Response(buf, {
      status: response.status === 0 ? 200 : response.status,
      statusText: response.statusText || "OK",
      headers,
    });
  } catch {
    return null;
  }
}

function contentTypeOf(response) {
  return (response.headers.get("content-type") || "").toLowerCase();
}

function isHtmlResponse(response) {
  const ct = contentTypeOf(response);
  return ct.includes("text/html") || ct.includes("application/xhtml");
}

function finalPathname(response) {
  try {
    return new URL(response.url || "", self.location.origin).pathname;
  } catch {
    return "";
  }
}

/** ¿Se puede cachear esta respuesta como HTML de página de app? */
function isCacheableAppHtml(request, response) {
  if (!response || !response.ok) return false;
  if (request.method !== "GET") return false;
  if (response.redirected) return false;
  if (response.status >= 300 && response.status < 400) return false;
  if (!isHtmlResponse(response)) return false;
  const reqPath = new URL(request.url).pathname;
  const finalPath = finalPathname(response);
  if (isLoginPath(finalPath) || isLoginPath(reqPath)) return false;
  // Tras follow, la URL final debe ser la pedida (salvo / → /despachos).
  if (finalPath && finalPath !== reqPath) {
    if (!(reqPath === "/" && finalPath === APP_SHELL)) return false;
  }
  return true;
}

async function matchFirst(urls) {
  for (const u of urls) {
    try {
      const hit = await caches.match(u, { ignoreSearch: false });
      if (hit) {
        const clean = await asCleanResponse(hit);
        if (clean) return clean;
      }
      const hitIgnore = await caches.match(u, { ignoreSearch: true });
      if (hitIgnore) {
        const clean = await asCleanResponse(hitIgnore);
        if (clean) return clean;
      }
    } catch {
      /* continue */
    }
  }
  return null;
}

async function matchExactPage(request) {
  const url = new URL(request.url);
  const candidates = [request, request.url, url.pathname, url.origin + url.pathname];
  for (const c of candidates) {
    try {
      const hit = await caches.match(c, { ignoreSearch: false });
      if (hit) {
        const clean = await asCleanResponse(hit);
        if (clean && isHtmlResponse(clean)) return clean;
      }
      const hitIgnore = await caches.match(c, { ignoreSearch: true });
      if (hitIgnore) {
        const clean = await asCleanResponse(hitIgnore);
        if (clean && isHtmlResponse(clean)) return clean;
      }
    } catch {
      /* continue */
    }
  }
  return null;
}

/**
 * Fallback offline para navegación.
 * NUNCA devolver APP_SHELL para una ruta distinta: Next hidrata mal.
 */
async function safeOfflineNavigation(request) {
  const path = new URL(request.url).pathname;
  if (path === APP_SHELL || path === "/") {
    const hit = await matchFirst([APP_SHELL, OFFLINE_HTML, OFFLINE_ROUTE]);
    return hit || syntheticOfflineHtml();
  }
  // Ruta profunda: intentar HTML exacto ya se hizo; fallback estático (no shell ajeno).
  const offline = await matchFirst([OFFLINE_HTML, OFFLINE_ROUTE]);
  return offline || syntheticOfflineHtml();
}

async function putClean(cacheName, request, cleanResponse) {
  try {
    if (!cleanResponse || !cleanResponse.ok) return;
    if (request.method !== "GET") return;
    if (cleanResponse.redirected) return;
    const cache = await caches.open(cacheName);
    const toStore = await asCleanResponse(cleanResponse);
    if (!toStore) return;
    await cache.put(request, toStore);
  } catch {
    /* quota / opaque */
  }
}

self.addEventListener("install", (event) => {
  event.waitUntil(
    (async () => {
      const cache = await caches.open(STATIC_CACHE);
      // No precachear "/" (redirect a /despachos → Response redirected en Safari).
      await Promise.all(
        PRECACHE.map(async (url) => {
          try {
            const res = await fetch(url, {
              credentials: "same-origin",
              redirect: "follow",
              headers: { Accept: "text/html,application/xhtml+xml,*/*" },
            });
            if (!res.ok || res.redirected) return;
            if (isLoginPath(finalPathname(res))) return;
            const clean = await asCleanResponse(res);
            if (!clean) return;
            await cache.put(url, clean);
          } catch {
            /* un recurso no debe tumbar el install */
          }
        }),
      );
      try {
        const existing = await cache.match(OFFLINE_HTML);
        if (!existing) {
          await cache.put(OFFLINE_HTML, syntheticOfflineHtml());
        }
      } catch {
        /* ignore */
      }
      await self.skipWaiting();
    })(),
  );
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    (async () => {
      const keys = await caches.keys();
      await Promise.all(
        keys
          .filter((key) => !key.startsWith(VERSION))
          .map((key) => caches.delete(key)),
      );
      await self.clients.claim();
    })(),
  );
});

self.addEventListener("fetch", (event) => {
  const { request } = event;
  if (request.method !== "GET") return;

  let url;
  try {
    url = new URL(request.url);
  } catch {
    return;
  }
  if (url.origin !== self.location.origin) return;
  if (isApiPath(url.pathname)) return;

  const isRsc =
    request.headers.get("RSC") === "1" ||
    request.headers.get("Next-Router-State-Tree") != null ||
    url.searchParams.has("_rsc");

  if (request.mode === "navigate") {
    event.respondWith(handleNavigate(request));
    return;
  }

  if (isRsc) {
    event.respondWith(
      (async () => {
        try {
          const fresh = await fetch(request);
          if (fresh.redirected) {
            const clean = await asCleanResponse(fresh);
            return clean || new Response("", { status: 503 });
          }
          return fresh;
        } catch {
          const cached = await caches.match(request);
          if (cached) {
            const clean = await asCleanResponse(cached);
            if (clean) return clean;
          }
          return new Response("", {
            status: 503,
            statusText: "Offline RSC",
            headers: {
              "Content-Type": "text/plain; charset=utf-8",
              "Cache-Control": "no-store",
              "X-LQ-Offline-RSC": "1",
            },
          });
        }
      })(),
    );
    return;
  }

  event.respondWith(handleAsset(request));
});

async function handleNavigate(request) {
  const path = new URL(request.url).pathname;

  try {
    const fresh = await fetch(request, { redirect: "follow" });
    const finalPath = finalPathname(fresh);

    // Login redirect: no cachear ni fingir ficha.
    if (isLoginPath(finalPath)) {
      const cleanLogin = await asCleanResponse(fresh);
      if (cleanLogin) return cleanLogin;
      const cached = await matchExactPage(request);
      if (cached) return cached;
      return safeOfflineNavigation(request);
    }

    if (fresh && fresh.ok) {
      const clean = await asCleanResponse(fresh, {
        "X-LQ-SW-Clean": "1",
      });
      if (clean) {
        if (isCacheableAppHtml(request, fresh) || (path === "/" && finalPath === APP_SHELL)) {
          // Cachear bajo la URL pedida solo si el HTML es de la misma ruta
          // (o / → shell). Siempre Response limpia (sin redirected).
          if (path === finalPath || (path === "/" && finalPath === APP_SHELL)) {
            const cacheKey =
              path === "/" && finalPath === APP_SHELL
                ? new Request(new URL(APP_SHELL, self.location.origin), {
                    method: "GET",
                  })
                : request;
            if (path === finalPath) {
              await putClean(PAGE_CACHE, request, clean);
            } else {
              await putClean(PAGE_CACHE, cacheKey, clean);
            }
          }
        }
        return clean;
      }
    }

    const cached = await matchExactPage(request);
    if (cached) return cached;
    if (path === "/") {
      const shell = await matchFirst([APP_SHELL]);
      if (shell) return shell;
    }
    return safeOfflineNavigation(request);
  } catch {
    const cached = await matchExactPage(request);
    if (cached) return cached;
    if (path === "/") {
      const shell = await matchFirst([APP_SHELL]);
      if (shell) return shell;
    }
    return safeOfflineNavigation(request);
  }
}

async function handleAsset(request) {
  const path = new URL(request.url).pathname;
  const cached = await caches.match(request);
  if (cached) {
    // Nunca servir HTML cacheado como si fuera un chunk JS.
    if (path.startsWith("/_next/static/") && path.endsWith(".js")) {
      const ct = contentTypeOf(cached);
      if (ct.includes("text/html") || cached.redirected) {
        return missingOfflineChunkResponse();
      }
    }
    const clean = await asCleanResponse(cached);
    if (clean) return clean;
  }

  try {
    const fresh = await fetch(request, { redirect: "follow" });
    if (fresh.redirected || (fresh.status >= 300 && fresh.status < 400)) {
      // Assets no deben seguir redirects a HTML/login.
      if (path.startsWith("/_next/static/") && path.endsWith(".js")) {
        return missingOfflineChunkResponse();
      }
      const clean = await asCleanResponse(fresh);
      return clean || syntheticOfflineText();
    }
    if (fresh && fresh.ok) {
      const ct = contentTypeOf(fresh);
      if (ct.includes("text/html")) {
        if (
          !fresh.redirected &&
          (path === "/offline" ||
            path === "/offline.html" ||
            path === APP_SHELL ||
            path.startsWith("/despachos"))
        ) {
          const clean = await asCleanResponse(fresh);
          if (clean) {
            await putClean(PAGE_CACHE, request, clean);
            return clean;
          }
        }
        // HTML inesperado para asset path → no cachear como JS.
        if (path.startsWith("/_next/static/")) {
          return missingOfflineChunkResponse();
        }
      } else if (path.startsWith("/_next/static/") || path.startsWith("/icons/")) {
        const clean = await asCleanResponse(fresh);
        if (clean) {
          await putClean(ASSET_CACHE, request, clean);
          return clean;
        }
      }
      const clean = await asCleanResponse(fresh);
      return clean || fresh;
    }
    if (path.startsWith("/_next/static/") && path.endsWith(".js")) {
      return missingOfflineChunkResponse();
    }
    return (await asCleanResponse(fresh)) || syntheticOfflineText();
  } catch {
    const again = await caches.match(request);
    if (again) {
      const clean = await asCleanResponse(again);
      if (clean) {
        if (path.startsWith("/_next/static/") && path.endsWith(".js")) {
          const ct = contentTypeOf(clean);
          if (ct.includes("text/html")) return missingOfflineChunkResponse();
        }
        return clean;
      }
    }
    if (path.startsWith("/_next/static/") && path.endsWith(".js")) {
      return missingOfflineChunkResponse();
    }
    return syntheticOfflineText();
  }
}
