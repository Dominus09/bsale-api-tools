"use client"

import { useEffect,useState } from "react"
import { useRouter } from "next/navigation"

export default function Dashboard(){

  const router = useRouter()

  const [data,setData] = useState<any>(null)
  const [loading,setLoading] = useState(true)

  useEffect(()=>{

    async function load(){

      const company = localStorage.getItem("company_id")

      // si no hay empresa seleccionada
      if(!company){
        router.push("/select-company")
        return
      }

      const res = await fetch(
        `https://api.quillotana.cl/dashboard/${company}`
      )

      const json = await res.json()

      setData(json)
      setLoading(false)

    }

    load()

  },[])

  if(loading) return <p>Cargando...</p>

  return(

    <div>

      <h1 style={{marginBottom:"30px"}}>
        Dashboard
      </h1>

      <div style={{
        display:"flex",
        gap:"20px"
      }}>

        <Card title="LOW" value={data.low} color="#fecaca"/>

        <Card title="HIGH" value={data.high} color="#bfdbfe"/>

        <Card title="ULTRA HIGH" value={data.ultra_high} color="#e9d5ff"/>

      </div>

    </div>

  )

}

function Card({title,value,color}:any){

  return(

    <div style={{
      width:"160px",
      height:"120px",
      background:color,
      borderRadius:"12px",
      padding:"20px",
      color:"#020617"
    }}>

      <h4>{title}</h4>

      <h2>{value}</h2>

    </div>

  )

}
