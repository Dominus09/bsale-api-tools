import { NextResponse } from "next/server"
import type { NextRequest } from "next/server"

export function middleware(request: NextRequest){

  const auth = request.cookies.get("qa_auth")

  if(!auth){

    if(request.nextUrl.pathname.startsWith("/dashboard")){
      return NextResponse.redirect(new URL("/login", request.url))
    }

  }

  return NextResponse.next()

}

export const config = {
  matcher: ["/dashboard/:path*"]
}
