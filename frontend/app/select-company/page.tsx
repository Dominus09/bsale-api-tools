"use client"

import { useEffect,useState } from "react"
import { useRouter } from "next/navigation"

export default function SelectCompany(){

  const [companies,setCompanies] = useState<any[]>([])
  const router = useRouter()

  useEffect(()=>{

    async function load(){

      const res = await fetch("https://api.quillotana.cl/companies/")

      const data = await res.json()

      setCompanies(data)

    }

    load()

  },[])

  function chooseCompany(id:number){

    localStorage.setItem("company_id",String(id))

    router.push("/dashboard")

  }

  return(

    <main style={{
      height:"100vh",
      display:"flex",
      justifyContent:"center",
      alignItems:"center",
      background:"#0f172a",
      color:"white"
    }}>

      <div style={{
        background:"#1e293b",
        padding:"40px",
        borderRadius:"12px",
        width:"400px"
      }}>

        <h2 style={{marginBottom:"20px"}}>
          Seleccionar empresa
        </h2>

        {companies.map(c=>(
          
          <button
            key={c.id}
            onClick={()=>chooseCompany(c.id)}
            style={{
              width:"100%",
              padding:"12px",
              marginBottom:"10px",
              borderRadius:"8px",
              border:"none",
              background:"#334155",
              color:"white",
              cursor:"pointer"
            }}
          >

            {c.name}

          </button>

        ))}

      </div>

    </main>

  )

}
