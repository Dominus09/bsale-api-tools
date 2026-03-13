"use client"

import { useEffect, useState } from "react"

export default function Problems(){

  const [rows,setRows] = useState<any[]>([])
  const [loading,setLoading] = useState(true)

  useEffect(()=>{

    async function load(){

      const res = await fetch("https://api.quillotana.cl/margin/problems/1")

      const data = await res.json()

      setRows(data)

      setLoading(false)

    }

    load()

  },[])

  if(loading){
    return <p>Cargando productos...</p>
  }

  return(

    <div>

      <h1 style={{
        marginBottom:"20px",
        fontSize:"24px"
      }}>
        Productos con problemas de margen
      </h1>

      <div style={{
        overflowX:"auto",
        background:"#020617",
        borderRadius:"10px"
      }}>

        <table style={{
          width:"100%",
          borderCollapse:"collapse",
          fontSize:"14px"
        }}>

          <thead>

            <tr style={{
              background:"#1e293b",
              textAlign:"left"
            }}>

              <th style={th}>SKU</th>
              <th style={th}>Producto</th>
              <th style={th}>Variante</th>
              <th style={th}>Lista</th>
              <th style={th}>Costo</th>
              <th style={th}>Precio</th>
              <th style={th}>Margen</th>
              <th style={th}>Estado</th>

            </tr>

          </thead>

          <tbody>

            {rows.map((r,i)=>(

              <tr
                key={i}
                style={{
                  borderBottom:"1px solid #1e293b"
                }}
              >

                <td style={td}>{r.code}</td>

                <td style={td}>{r.product_name}</td>

                <td style={td}>{r.variant_name}</td>

                <td style={td}>{r.price_list_name}</td>

                <td style={td}>
                  ${Number(r.cost_gross).toLocaleString()}
                </td>

                <td style={td}>
                  ${Number(r.price_gross).toLocaleString()}
                </td>

                <td style={td}>
                  {Number(r.margin_percent).toFixed(1)}%
                </td>

                <td style={{
                  ...td,
                  fontWeight:"bold",
                  color:statusColor(r.status)
                }}>
                  {r.status}
                </td>

              </tr>

            ))}

          </tbody>

        </table>

      </div>

    </div>

  )

}

function statusColor(status:string){

  if(status==="LOW") return "#f87171"
  if(status==="HIGH") return "#60a5fa"
  if(status==="ULTRA_HIGH") return "#c084fc"
  if(status==="NO_PRICE") return "#fbbf24"

  return "white"

}

const th={
  padding:"12px",
  fontWeight:"600"
}

const td={
  padding:"12px"
}
