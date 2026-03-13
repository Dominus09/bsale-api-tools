"use client"

import { useRouter } from "next/navigation"
import { useState } from "react"

export default function LoginPage() {

  const router = useRouter()

  const [user,setUser] = useState("")
  const [pass,setPass] = useState("")

  function handleLogin(e:any){
    e.preventDefault()

    if(user && pass){
      localStorage.setItem("qa_auth","1")
      router.push("/dashboard")
    }
  }

  return(

    <main style={{
      display:"flex",
      height:"100vh",
      justifyContent:"center",
      alignItems:"center",
      background:"#0f172a",
      color:"white"
    }}>

      <form
        onSubmit={handleLogin}
        style={{
          background:"#1e293b",
          padding:"40px",
          borderRadius:"12px",
          width:"320px"
        }}
      >

        <h2 style={{marginBottom:"20px"}}>
          La Quillotana Analytics
        </h2>

        <input
          placeholder="Usuario"
          value={user}
          onChange={(e)=>setUser(e.target.value)}
          style={{
            width:"100%",
            marginBottom:"10px",
            padding:"10px"
          }}
        />

        <input
          type="password"
          placeholder="Contraseña"
          value={pass}
          onChange={(e)=>setPass(e.target.value)}
          style={{
            width:"100%",
            marginBottom:"20px",
            padding:"10px"
          }}
        />

        <button
          style={{
            width:"100%",
            padding:"10px",
            background:"#dc2626",
            color:"white",
            border:"none",
            borderRadius:"6px"
          }}
        >
          Entrar
        </button>

      </form>

    </main>

  )

}