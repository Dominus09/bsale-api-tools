"use client"

import { DeliveryLocalToolsHost } from "@/components/delivery-local-tools-host"
import { buildEntregaHref } from "@/lib/delivery-flow"
import { useDeliveryParams } from "@/lib/use-delivery-params"
import { useVisitSession } from "@/lib/use-visit-session"

export default function EntregaLayout({ children }: { children: React.ReactNode }) {
  const { planId, customerKey } = useDeliveryParams()
  const session = useVisitSession(planId, customerKey)
  const c = session.client
  const nav = session.navigation

  return (
    <div className="stack-gap">
      {children}
      {!session.loading && !session.error ? (
        <DeliveryLocalToolsHost
          planId={planId}
          customerKey={customerKey}
          address={c.address}
          city={c.city}
          latitude={c.latitude}
          longitude={c.longitude}
          coordinatesSource={c.coordinates_source}
          locationRegistration={c.location_registration}
          facade={c.facade}
          facadePhoto={c.facade_photo}
          navigationUrl={nav?.available ? nav.google_maps_url || null : null}
          warnAccuracyM={session.p3?.gps_warn_accuracy_m}
          backHref={buildEntregaHref(planId, customerKey, "ficha")}
          onChanged={() => session.reload()}
        />
      ) : null}
    </div>
  )
}
