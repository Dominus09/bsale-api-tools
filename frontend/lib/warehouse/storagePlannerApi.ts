import {getApiBaseUrl} from "@/lib/api-base"
import {getAuthHeaders} from "@/lib/api"
import type {PlanInfo,SavedPlan,Count,Slot} from "./storagePlanner"
async function request<T>(path:string,init?:RequestInit):Promise<T>{
  const res=await fetch(getApiBaseUrl()+"/warehouse/storage-plans"+path,{...init,cache:"no-store",headers:{...getAuthHeaders(),...(init?.headers??{})}})
  if(!res.ok){
    const data=await res.json().catch(()=>null)
    const detail=data?.detail
    throw Error(typeof detail==="string"?detail:detail?.message??"Error del planificador ("+res.status+").")
  }
  return res.json() as Promise<T>
}
export async function listStoragePlans(){return (await request<{items:PlanInfo[]}>("")).items}
export function getStoragePlan(id:string){return request<SavedPlan>("/"+encodeURIComponent(id))}
export function saveStoragePlan(body:{id:string;name:string;expected_row_version:number;counts:Count[];slots:Slot[]}){
  return request<SavedPlan>("",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(body)})
}
