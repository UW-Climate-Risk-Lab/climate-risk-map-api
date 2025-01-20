SET ROLE pgosm_flex;

DO $$
DECLARE
    r RECORD;
BEGIN
    RAISE NOTICE 'Starting materialized view refresh...';
    
    FOR r IN 
        SELECT schemaname, matviewname 
        FROM pg_matviews 
        WHERE schemaname = 'osm'  -- Adjust schema if needed
        ORDER BY matviewname
    LOOP
        RAISE NOTICE 'Refreshing materialized view %.%', r.schemaname, r.matviewname;
        EXECUTE format('REFRESH MATERIALIZED VIEW %I.%I', r.schemaname, r.matviewname);
    END LOOP;
    
    RAISE NOTICE 'Completed materialized view refresh';
END $$;
