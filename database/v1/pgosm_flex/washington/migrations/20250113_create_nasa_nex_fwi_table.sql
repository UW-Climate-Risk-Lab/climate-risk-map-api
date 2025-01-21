BEGIN;

SET ROLE climate_user;

CREATE TABLE climate.nasa_nex_fwi (
    id SERIAL PRIMARY KEY,
    osm_id BIGINT NOT NULL,
    month SMALLINT NOT NULL,
    decade SMALLINT NOT NULL,
    ssp SMALLINT NOT NULL, -- ssp -999 is for 'historical' values
    value_mean FLOAT NOT NULL,
    value_median FLOAT NOT NULL,
    value_stddev FLOAT NOT NULL,
    value_min FLOAT NOT NULL,
    value_max FLOAT NOT NULL,
    value_q1 FLOAT NOT NULL,
    value_q3 FLOAT NOT NULL,  
    metadata JSONB

);

-- Unique index to constrain possible values for a given feature (osm_id)
CREATE UNIQUE INDEX idx_unique_nasa_nex_fwi_record
    ON climate.nasa_nex_fwi (osm_id, month, decade, ssp);

CREATE INDEX idx_nasa_nex_fwi_on_osm_id ON climate.nasa_nex_fwi (osm_id);
CREATE INDEX idx_nasa_nex_fwi_on_month ON climate.nasa_nex_fwi (month);
CREATE INDEX idx_nasa_nex_fwi_on_decade ON climate.nasa_nex_fwi (decade);
CREATE INDEX idx_nasa_nex_fwi_on_month_decade ON climate.nasa_nex_fwi (month, decade);
CREATE INDEX idx_nasa_nex_fwi_on_ssp ON climate.nasa_nex_fwi (ssp);

COMMIT;