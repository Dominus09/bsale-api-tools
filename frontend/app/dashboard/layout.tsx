"use client"

import Link from "next/link"
import { useRouter } from "next/navigation"

export default function DashboardLayout({
  children,
}:{
  children: React.ReactNode
}){

  const router = useRouter()

  function logout(){

    document.cookie = "qa_auth=; path=/; expires=Thu, 01 Jan 1970 00:00:00"
    router.push("/login")

  }

  return(

    <div style={{
      display:"flex",
      height:"100vh",
      background:"#0f172a",
      color:"white"
    }}>

      {/* SIDEBAR */}

      <aside style={{
        width:"240px",
        background:"#020617",
        padding:"20px"
      }}>

        <h2 style={{marginBottom:"30px"}}>
          Quillotana
        </h2>

        <nav style={{display:"flex",flexDirection:"column",gap:"10px"}}>

          <Link href="/dashboard">Dashboard</Link>

          <Link href="/dashboard/margins">
            Margins
          </Link>

          <Link href="/dashboard/problems">
            Margin Problems
          </Link>

          <Link href="/dashboard/export">
            Export Prices
          </Link>

        </nav>

        <div style={{marginTop:"40px"}}>

          <button
            onClick={logout}
            style={{
              background:"#dc2626",
              border:"none",
              padding:"10px",
              width:"100%",
              borderRadius:"6px",
              color:"white"
            }}
          >
            Logout
          </button>

        </div>

      </aside>


      {/* CONTENIDO */}

      <main style={{
        flex:1,
        padding:"40px"
      }}>

        {children}

      </main>

    </div>

  )

}
