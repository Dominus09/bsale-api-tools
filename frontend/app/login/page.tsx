"use client"

import { useRouter } from "next/navigation"
import { useState } from "react"

export default function LoginPage() {

  const router = useRouter()

  const [user,setUser] = useState("")
  const [pass,setPass] = useState("")
  const [loading,setLoading] = useState(false)

  async function handleLogin(e:any){

    e.preventDefault()

    setLoading(true)

    try{

      const res = await fetch("https://api.quillotana.cl/auth/login",{
        method:"POST",
        headers:{
          "Content-Type":"application/json"
        },
        body:JSON.stringify({
          username:user,
          password:pass
        })
      })

      const data = await res.json()

      if(data.ok){

        document.cookie = "qa_auth=1; path=/"
        localStorage.setItem("qa_user",data.user.username)
        localStorage.setItem("qa_role",data.user.role)

        router.push("/select-company")

      }else{

        alert("Usuario o contraseña incorrecta")

      }

    }catch(err){

      console.error(err)
      alert("Error conectando con la API")

    }

    setLoading(false)

  }

  return(

    <main style={{
      display:"flex",
      height:"100vh",
      justifyContent:"center",
      alignItems:"center",
      background:"#0f172a",
      color:"white",
      fontFamily:"sans-serif"
    }}>

      <form
        onSubmit={handleLogin}
        style={{
          background:"#1e293b",
          padding:"40px",
          borderRadius:"12px",
          width:"320px",
          boxShadow:"0 0 20px rgba(0,0,0,0.3)"
        }}
      >

        <h2 style={{
          marginBottom:"25px",
          textAlign:"center"
        }}>
          La Quillotana Analytics
        </h2>

        <input
          placeholder="Usuario"
          value={user}
          onChange={(e)=>setUser(e.target.value)}
          required
          style={{
            width:"100%",
            marginBottom:"12px",
            padding:"10px",
            borderRadius:"6px",
            border:"none"
          }}
        />

        <input
          type="password"
          placeholder="Contraseña"
          value={pass}
          onChange={(e)=>setPass(e.target.value)}
          required
          style={{
            width:"100%",
            marginBottom:"20px",
            padding:"10px",
            borderRadius:"6px",
            border:"none"
          }}
        />

        <button
          disabled={loading}
          style={{
            width:"100%",
            padding:"12px",
            background:"#dc2626",
            color:"white",
            border:"none",
            borderRadius:"6px",
            cursor:"pointer",
            fontWeight:"bold"
          }}
        >
          {loading ? "Entrando..." : "Entrar"}
        </button>

      </form>

    </main>

  )

}
