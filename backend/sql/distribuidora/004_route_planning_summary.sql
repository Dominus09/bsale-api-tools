-- Resumen por camión/día + trigger que recalcula totales desde route_planning.
-- +go es separador para ensure_distribuidora_schema.

CREATE TABLE IF NOT EXISTS distribuidora.route_planning_summary (
    id BIGSERIAL PRIMARY KEY,
    planning_date DATE NOT NULL,
    truck TEXT NOT NULL,
    route_name TEXT,
    driver TEXT,
    assistant_1 TEXT,
    assistant_2 TEXT,
    departure_time TEXT,
    total_clients INTEGER NOT NULL DEFAULT 0,
    total_amount NUMERIC(18, 4) NOT NULL DEFAULT 0,
    general_observation TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_distribuidora_route_planning_summary_date_truck UNIQUE (planning_date, truck)
);
-- +go

CREATE INDEX IF NOT EXISTS idx_distribuidora_route_planning_summary_date
    ON distribuidora.route_planning_summary (planning_date);
-- +go

CREATE OR REPLACE FUNCTION distribuidora.refresh_route_planning_summary_bucket(
    p_planning_date date,
    p_truck text
) RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM distribuidora.route_planning_summary s
    WHERE s.planning_date = p_planning_date
      AND s.truck = p_truck
      AND NOT EXISTS (
          SELECT 1
          FROM distribuidora.route_planning rp
          WHERE rp.planning_date = p_planning_date
            AND rp.truck = p_truck
      );

    INSERT INTO distribuidora.route_planning_summary (
        planning_date,
        truck,
        total_clients,
        total_amount,
        updated_at
    )
    SELECT
        p_planning_date,
        p_truck,
        COUNT(DISTINCT rp.client_id) FILTER (WHERE rp.client_id IS NOT NULL),
        COALESCE(SUM(rp.total_amount), 0),
        NOW()
    FROM distribuidora.route_planning rp
    WHERE rp.planning_date = p_planning_date
      AND rp.truck = p_truck
    GROUP BY p_planning_date, p_truck
    HAVING COUNT(*) > 0
    ON CONFLICT (planning_date, truck) DO UPDATE SET
        total_clients = EXCLUDED.total_clients,
        total_amount = EXCLUDED.total_amount,
        updated_at = NOW();
END;
$$;
-- +go

CREATE OR REPLACE FUNCTION distribuidora.tg_route_planning_touch_summary()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        PERFORM distribuidora.refresh_route_planning_summary_bucket(OLD.planning_date, OLD.truck);
        RETURN OLD;
    ELSIF TG_OP = 'UPDATE' THEN
        IF OLD.planning_date IS DISTINCT FROM NEW.planning_date
           OR OLD.truck IS DISTINCT FROM NEW.truck THEN
            PERFORM distribuidora.refresh_route_planning_summary_bucket(OLD.planning_date, OLD.truck);
        END IF;
        PERFORM distribuidora.refresh_route_planning_summary_bucket(NEW.planning_date, NEW.truck);
        RETURN NEW;
    ELSE
        PERFORM distribuidora.refresh_route_planning_summary_bucket(NEW.planning_date, NEW.truck);
        RETURN NEW;
    END IF;
END;
$$;
-- +go

DROP TRIGGER IF EXISTS trg_route_planning_summary ON distribuidora.route_planning;
-- +go

CREATE TRIGGER trg_route_planning_summary
    AFTER INSERT OR UPDATE OR DELETE
    ON distribuidora.route_planning
    FOR EACH ROW
    EXECUTE PROCEDURE distribuidora.tg_route_planning_touch_summary();
-- +go

DO $$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT DISTINCT planning_date, truck
        FROM distribuidora.route_planning
    LOOP
        PERFORM distribuidora.refresh_route_planning_summary_bucket(r.planning_date, r.truck);
    END LOOP;
END $$;
-- +go
