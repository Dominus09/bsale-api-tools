# FRONTEND HANDOFF DOCUMENTATION
## Grupo Quillotana ERP

---

## 1. PROJECT STRUCTURE

```
/app                          # Next.js App Router pages
  ├── (dashboard)/            # Dashboard layout group
  │   ├── layout.tsx          # Shared dashboard layout (sidebar + header)
  │   ├── dashboard/          # Main dashboard page
  │   ├── margins/            # Margin analysis page
  │   ├── alerts/             # Margin alerts page
  │   ├── products-without-cost/  # Products without cost page
  │   └── sucursales/         # Branch operations module
  │       ├── recepciones/    # Merchandise reception
  │       ├── ofertas/        # Offers and clearance
  │       ├── trazabilidad/   # Shelf review (gondola)
  │       └── etiquetas/      # Label generator
  ├── login/                  # Login page
  ├── company-selector/       # Company selection page
  ├── layout.tsx              # Root layout
  ├── page.tsx                # Home redirect
  └── globals.css             # Global styles and Tailwind config

/components                   # Reusable UI components
  ├── layout/                 # Layout components
  │   ├── sidebar.tsx         # Main navigation sidebar
  │   └── header.tsx          # Top header with user info + demo badge
  └── ui/                     # shadcn/ui components

/lib                          # Utilities and services
  ├── api.ts                  # Centralized API client (hybrid mode)
  └── utils.ts                # Helper functions (cn, etc.)

/hooks                        # Custom React hooks
  └── use-mobile.tsx          # Mobile detection hook
```

### Folder Purposes

| Folder | Purpose |
|--------|---------|
| `/app` | Next.js 16 App Router pages and layouts |
| `/components` | Reusable React components |
| `/components/layout` | Layout-specific components (sidebar, header) |
| `/components/ui` | shadcn/ui primitive components |
| `/lib` | Utilities, API client, helper functions |
| `/hooks` | Custom React hooks |

---

## 2. ROUTES

| Route | Description |
|-------|-------------|
| `/` | Redirects to `/login` |
| `/login` | User authentication page |
| `/company-selector` | Company selection after login |
| `/dashboard` | Main dashboard with statistics and charts |
| `/margins` | Detailed margin analysis table |
| `/alerts` | Margin alerts overview |
| `/products-without-cost` | Products missing cost data |
| `/sucursales/recepciones` | Merchandise reception management |
| `/sucursales/ofertas` | Offers and clearance management |
| `/sucursales/trazabilidad` | Shelf review / gondola scanning |
| `/sucursales/etiquetas` | Price label generator |

---

## 3. SIDEBAR STRUCTURE (SPANISH)

The sidebar is organized into sections:

### Dashboard
- Dashboard principal

### Analítica
- Márgenes
- Alertas

### Sucursales
- Recepción de Mercadería
- Ofertas y Remates
- Revisión de Góndola
- Generador de Etiquetas

### Operaciones (Próximamente)
- Stock
- Compras
- Ventas

### Finanzas (Próximamente)
- Finanzas

### Administración (Próximamente)
- Usuarios
- Empresas
- Configuración

---

## 4. COMPONENTS CREATED

### Layout Components

| Component | Path | Description |
|-----------|------|-------------|
| Sidebar | `/components/layout/sidebar.tsx` | Main navigation with sections |
| Header | `/components/layout/header.tsx` | Top bar with user info + demo mode badge |

### Page Components

| Page | Components Used |
|------|-----------------|
| Dashboard | StatCards, PieChart, BarChart, AlertsTable |
| Margins | DataTable with filters, search, badges |
| Alerts | Color-coded alerts table |
| Recepciones | Editable Excel-like table, barcode scanner dialog |
| Ofertas | CRUD table with dialog forms, filters |
| Trazabilidad | Barcode scanner, comparison table |
| Etiquetas | Product scanner, label preview, print dialog |

### UI Patterns Used

- **Cards**: Statistics display with icons
- **Tables**: Data tables with sorting/filtering
- **Badges**: Status indicators (colored by margin status)
- **Dialogs**: Modal forms for CRUD operations
- **Select**: Dropdown filters
- **Input**: Text/number/date inputs
- **Button**: Action buttons with variants

---

## 5. API INTEGRATION POINTS

All API calls are centralized in `/lib/api.ts`.

### Base URL

```typescript
const API_URL = "https://api.quillotana.cl"
```

### Endpoints

| Endpoint | Method | Page | Component | Expected Response |
|----------|--------|------|-----------|-------------------|
| `/login` | POST | `/login` | LoginPage | `{ token, email, role }` |
| `/companies` | GET | `/company-selector` | CompanySelectorPage | `Company[]` |
| `/margin-summary` | GET | `/dashboard` | DashboardPage | `MarginSummary` |
| `/margin-alerts` | GET | `/dashboard`, `/alerts` | AlertsTable | `MarginAlert[]` |
| `/margin-analysis` | GET | `/margins` | MarginsPage | `MarginProduct[]` |
| `/products-without-cost` | GET | `/products-without-cost` | ProductsPage | `ProductWithoutCost[]` |

### Future Endpoints (Sucursales)

| Endpoint | Method | Page | Description |
|----------|--------|------|-------------|
| `/recepciones` | GET/POST | `/sucursales/recepciones` | Merchandise receptions |
| `/ofertas` | GET/POST/PUT/DELETE | `/sucursales/ofertas` | Offers and clearance |
| `/trazabilidad` | GET/POST | `/sucursales/trazabilidad` | Shelf review scans |
| `/etiquetas` | POST | `/sucursales/etiquetas` | Generate labels |

### Data Types

```typescript
interface LoginResponse {
  token: string
  email: string
  role: string
}

interface Company {
  company_id: number
  name: string
}

interface MarginSummary {
  total_products: number
  low_margin_count: number
  ok_count: number
  high_margin_count: number
  ultra_high_margin_count: number
  average_margin: number
}

interface MarginProduct {
  id: number
  product_name: string
  cost: number
  price: number
  margin: number
  status: "LOW_MARGIN" | "OK" | "HIGH_MARGIN" | "ULTRA_HIGH_MARGIN"
  suggested_price: number
}

interface MarginAlert {
  id: number
  product_name: string
  current_margin: number
  expected_margin: number
  alert_type: string
}

interface ProductWithoutCost {
  id: number
  product_name: string
  sku?: string
  category?: string
}
```

---

## 6. STATE MANAGEMENT

### LocalStorage Keys

| Key | Description | Set By | Used By |
|-----|-------------|--------|---------|
| `token` | JWT authentication token | Login page | API headers |
| `company_id` | Selected company ID | Company selector | API queries |
| `company_name` | Selected company name | Company selector | Header display |
| `email` | User email | Login page | Header display |
| `role` | User role | Login page | Future permissions |

### SessionStorage Keys

| Key | Description |
|-----|-------------|
| `demo_mode` | Tracks if fallback data is being used |

### API Headers

All authenticated requests include:
```typescript
{
  "Authorization": "Bearer ${token}",
  "Content-Type": "application/json"
}
```

---

## 7. AUTHENTICATION FLOW

```
1. User enters credentials on /login
2. POST to /login endpoint
3. If successful:
   - Store token, email, role in localStorage
   - Redirect to /company-selector
4. User selects company
   - Store company_id, company_name in localStorage
   - Redirect to /dashboard
5. On logout:
   - Clear all localStorage keys
   - Clear sessionStorage
   - Redirect to /login
```

### Protected Routes

All routes under `/(dashboard)/` require authentication. The dashboard layout should check for token presence and redirect to login if missing.

---

## 8. API CLIENT - HYBRID MODE

The API client (`/lib/api.ts`) implements a hybrid approach:

### How It Works

1. **Try Real API First**: Every request attempts the real API at `https://api.quillotana.cl`
2. **Fallback on Network Error**: If the fetch fails due to network issues (common in preview sandboxes), return mock data
3. **Track Demo Mode**: Set `isDemoMode = true` when using fallback data
4. **Show Indicator**: Header displays "Modo demo" badge when fallback is active

### Code Pattern

```typescript
export async function getCompanies(): Promise<Company[]> {
  try {
    const res = await fetch(`${API_URL}/companies`, {
      headers: getAuthHeaders(),
    })
    if (!res.ok) throw new Error("Error")
    return res.json()
  } catch (error) {
    if (isNetworkError(error)) {
      console.warn("[API] Network error, using fallback data")
      setDemoMode(true)
      return mockCompanies
    }
    throw error
  }
}
```

### Demo Credentials

When API is unreachable, login accepts:
- Email: `prueba.q@gmail.com`
- Password: `123456`
- Or any non-empty credentials

### Demo Mode Indicator

The header shows a badge when demo mode is active:
- Orange badge with "Modo demo" text
- WifiOff icon
- Tooltip in dropdown: "Usando datos de demostración"

---

## 9. WHERE TO MODIFY CODE

### To Integrate Backend

| File | Purpose |
|------|---------|
| `/lib/api.ts` | All API requests - modify endpoints, add new functions |
| `/app/login/page.tsx` | Login form submission |
| `/app/company-selector/page.tsx` | Company list loading |
| `/app/(dashboard)/dashboard/page.tsx` | Dashboard data fetching |
| `/app/(dashboard)/margins/page.tsx` | Margin analysis data |
| `/app/(dashboard)/alerts/page.tsx` | Alerts data |
| `/app/(dashboard)/products-without-cost/page.tsx` | Products without cost |
| `/app/(dashboard)/sucursales/*` | Branch module pages |

### To Add New API Endpoint

1. Add type definition in `/lib/api.ts`
2. Add mock data (for fallback)
3. Create async function with try/catch pattern
4. Export the function
5. Import and use in page component

---

## 10. FUTURE MODULE EXPANSION

### Adding a New Route

1. Create folder: `/app/(dashboard)/new-module/`
2. Create page: `/app/(dashboard)/new-module/page.tsx`
3. The dashboard layout (sidebar + header) is automatically applied

### Adding to Sidebar

Edit `/components/layout/sidebar.tsx`:

```typescript
const navSections = [
  // ... existing sections
  {
    title: "Nueva Sección",
    items: [
      { href: "/new-module", label: "Nuevo Módulo", icon: NewIcon },
    ],
  },
]
```

### Adding API Function

```typescript
// In /lib/api.ts

const mockNewData: NewType[] = [/* fallback data */]

export async function getNewData(): Promise<NewType[]> {
  try {
    const companyId = getCompanyId()
    const res = await fetch(`${API_URL}/new-endpoint?company_id=${companyId}`, {
      headers: getAuthHeaders(),
    })
    if (!res.ok) throw new Error("Error")
    return res.json()
  } catch (error) {
    if (isNetworkError(error)) {
      setDemoMode(true)
      return mockNewData
    }
    throw error
  }
}
```

---

## 11. TECH STACK

| Technology | Version | Purpose |
|------------|---------|---------|
| Next.js | 16 | React framework |
| React | 19 | UI library |
| Tailwind CSS | 4 | Styling |
| shadcn/ui | Latest | UI components |
| Recharts | Latest | Charts |
| Lucide React | Latest | Icons |
| TypeScript | 5 | Type safety |

---

## 12. DESIGN SYSTEM

### Brand Colors

- **Primary**: Red (Quillotana brand) - `oklch(0.5 0.2 25)`
- **Background**: Light gray - `oklch(0.98 0 0)`
- **Cards**: White - `oklch(1 0 0)`
- **Text**: Dark gray - `oklch(0.145 0 0)`
- **Muted**: Medium gray - `oklch(0.5 0 0)`

### Status Colors

| Status | Color | Usage |
|--------|-------|-------|
| LOW_MARGIN | Red | Critical margins |
| OK | Green | Healthy margins |
| HIGH_MARGIN | Yellow/Amber | Above target |
| ULTRA_HIGH_MARGIN | Purple | Very high margins |

### Typography

- **Font**: Geist Sans (system fallback)
- **Headings**: Semibold
- **Body**: Regular

---

## 13. DEPLOYMENT

### Build Command

```bash
pnpm build
```

### Start Command

```bash
pnpm start
```

### Environment Variables

None required - API URL is configured in `/lib/api.ts`.

---

## Contact

For questions about this frontend implementation, refer to:
- Codebase comments
- API documentation at https://api.quillotana.cl/docs
- Development team
