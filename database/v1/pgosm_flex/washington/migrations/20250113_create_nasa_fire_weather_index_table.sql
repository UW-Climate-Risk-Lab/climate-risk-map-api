BEGIN;
SET ROLE climate_user;
CREATE TABLE climate.nasa_nex_fwi (
    id SERIAL PRIMARY KEY,
    osm_id BIGINT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    ssp INT NOT NULL,
    model TEXT NOT NULL,
    ensemble_member TEXT NOT NULL,
    metadata JSONB

);

-- Unique index to constrain possible values for a given feature (osm_id)
CREATE UNIQUE INDEX idx_unique_nasa_nex_fwi_record
    ON climate.nasa_nex_fwi (osm_id, month, year, ssp, model, ensemble_member);

CREATE INDEX idx_nasa_nex_fwi_on_osm_id ON climate.nasa_nex_fwi (osm_id);
CREATE INDEX idx_nasa_nex_fwi_on_month ON climate.nasa_nex_fwi (month);
CREATE INDEX idx_nasa_nex_fwi_on_year ON climate.nasa_nex_fwi (year);
CREATE INDEX idx_nasa_nex_fwi_on_month_year ON climate.nasa_nex_fwi (month, year);
CREATE INDEX idx_nasa_nex_fwi_on_ssp ON climate.nasa_nex_fwi (ssp);

COMMIT;