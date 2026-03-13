"use client"

import { useEffect,useState } from "react"

export default function Dashboard(){

  const [data,setData] = useState<any>(null)

  useEffect(()=>{

    async function load(){

      const res = await fetch("https://api.quillotana.cl/dashboard/1")

      const json = await res.json()

      setData(json)

    }

    load()

  },[])

  if(!data) return <p>Cargando...</p>

  return(

    <div>

      <h1 style={{marginBottom:"30px"}}>
        Dashboard empresa 1
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
