# Diseño — Administración centralizada ERP + app.quillotana.cl

Estado: **diseño aprobado para implementación futura**. Sin migraciones aplicadas en esta fase.

## Objetivo

Un único directorio de cuentas en `analytics` (`bsale.users` + tablas `app.*`) que autorice:

| Superficie | Host típico | Quién entra |
|---|---|---|
| ERP | work / test.quillotana.cl | OWNER, GERENCIA, ADM (+ operativos con app `erp` si se otorga) |
| PWA operativa | app.quillotana.cl | Perfiles operativos (vendedor, chofer, bodega, …) **sin** ERP por defecto |

## Separación de conceptos

1. **Jerarquía** (ya existe): OWNER / GERENCIA / ADM / OPERATIVO — controla administración de usuarios y módulos gerenciales.
2. **Aplicaciones autorizadas**: `erp`, `pwa` (extensible).
3. **Empresas**: reutilizar `app.user_companies (user_id, company_id)`.
4. **Oficinas**: siempre `company_id + office_id` (= `bsale.offices.bsale_id`).
5. **Perfil operativo**: vendedor | chofer | bodega | cajero | reponedor | jefe_sucursal.
6. **Alcances extra** (fases posteriores): clientes, rutas, planificaciones.

Regla: **OPERATIVO no recibe app `erp` automáticamente**.

## Tablas propuestas (`app`)

### `app.user_apps`
| Columna | Tipo | Notas |
|---|---|---|
| user_id | bigint | FK lógica a `bsale.users.id` |
| app_code | text | `erp` \| `pwa` |
| active | boolean | default true |
| PK | (user_id, app_code) | |

### `app.user_operational_profiles`
| Columna | Tipo | Notas |
|---|---|---|
| user_id | bigint | |
| profile_code | text | vendedor, chofer, bodega, cajero, reponedor, jefe_sucursal |
| active | boolean | |
| PK | (user_id, profile_code) | Un usuario puede tener varios perfiles |

### `app.user_offices`
| Columna | Tipo | Notas |
|---|---|---|
| user_id | bigint | |
| company_id | bigint | |
| office_id | bigint | = `bsale.offices.bsale_id` |
| PK | (user_id, company_id, office_id) | |
| CHECK | office pertenece a company | validar en servicio contra `bsale.offices` |

Índices: `(company_id, office_id)`, `(user_id)`.

### Futuro (no en v1)
- `app.user_client_scopes`, `app.user_route_scopes` — cuando se unifiquen vendedores/choferes.

## Endpoints (ampliación de `/admin/users`)

| Método | Ruta | Uso |
|---|---|---|
| GET | `/admin/users/{id}` | Incluir `apps`, `profiles`, `companies`, `offices` |
| PATCH | `/admin/users/{id}` | Actualizar jerarquía + apps + perfiles + empresas + oficinas |
| GET | `/admin/users/meta/offices?company_id=` | Oficinas filtradas por empresa(s) autorizadas del actor |
| GET | `/auth/me` | Añadir `apps`, `profiles`, `company_ids`, `office_scopes` |

Mutaciones siguen OWNER / allowlists / jerarquía actuales.

## Pantallas (Graphite Premium)

1. Listado Usuarios: columnas Rol · Apps · Empresas · Perfiles.
2. Ficha (Sheet):
   - Datos + rol jerárquico.
   - Apps (checks ERP / PWA).
   - Empresas (existente).
   - Oficinas multi-select **filtradas por empresas marcadas**.
   - Perfiles operativos (solo si app PWA).
3. No mostrar controles ERP a quien solo tenga PWA en su propio `/auth/me` (sidebar).

## Enforcement FastAPI

- Cada router sensible: `require_app("erp")` o `require_app("pwa")` + perfil.
- Filtro de datos: `company_id` / `office_id` del request ⊆ alcance del usuario (nunca confiar en el frontend).
- Escrituras: mismas comprobaciones que lecturas.
- `AUTH_REQUIRE_USER_COMPANIES` permanece **off** hasta backfill + auditoría de las 13 cuentas (+ futuras PWA).

## Unificación gradual vendedores / choferes

1. Mantener mecanismos actuales (tablas/syncs existentes) en lectura.
2. Mapear IDs externos → `bsale.users` + perfiles en `app.user_operational_profiles`.
3. Cutover por módulo (rutero, cargas, picking) cuando el alcance oficina/empresa esté poblado.
4. No romper login bcrypt ni Costos V2.

## Orden de implementación sugerido

1. Migraciones aditivas `064` apps / `065` profiles / `066` offices (vacías).
2. Ampliar `GET/PATCH /admin/users` + meta offices.
3. UI ficha multi-select oficinas.
4. `/auth/me` con apps/scopes.
5. Guards en endpoints PWA piloto.
6. Backfill cuentas ERP actuales → `user_apps(erp)`.
7. Validación completa → recién entonces considerar `AUTH_REQUIRE_USER_COMPANIES`.

## Riesgos

- Oficinas duplicadas entre empresas → clave compuesta obligatoria.
- Usuarios sin filas en `user_apps` durante transición → default temporal: rol gerencia/adm ⇒ erp; resto sin erp.
- No mezclar OWNER con perfiles PWA sin revisión.
