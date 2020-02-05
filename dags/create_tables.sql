CREATE TABLE staging_temperature_data (
    dt                                  DATE,
    average_temperature                  NUMERIC(10,3),
    average_temperature_uncertainty       NUMERIC(10,3),
    city                        VARCHAR,
    country                     VARCHAR,
    latitude                    VARCHAR,
    longitude                   VARCHAR
);

CREATE TABLE staging_us_city_data (
        city                        VARCHAR,
        state                       VARCHAR,
        median_age                  FLOAT,
        male_population             NUMERIC,
        female_population           NUMERIC,
        total_population            NUMERIC,
        number_of_veterans          NUMERIC,
        foreign_born                NUMERIC,
        average_household_size      DECIMAL(6,3),
        state_code                  VARCHAR,
        race                        VARCHAR,
        count                       NUMERIC
);

CREATE TABLE staging_airport_code_data (
    ident VARCHAR,
    type VARCHAR,
    name VARCHAR,
    elevation_ft NUMERIC,
    continent VARCHAR,
    iso_country VARCHAR,
    iso_region VARCHAR,
    municipality VARCHAR,
    gps_code VARCHAR,
    iata_code VARCHAR,
    local_code VARCHAR,
    coordinates VARCHAR
);

CREATE TABLE fact_temperature (
    id BIGINT IDENTITY(1, 1) NOT NULL,
    city VARCHAR(32) NOT NULL,
    country VARCHAR(32) NOT NULL,
    latitude VARCHAR(10),
    longitude VARCHAR(10),
    average_temperature VARCHAR NOT NULL,
    average_temperature_uncertainty VARCHAR NOT NULL,
    date DATE NOT NULL,
    CONSTRAINT fact_pk PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS time (
    ts DATE NOT NULL,
    day SMALLINT NOT NULL,
    week SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    year SMALLINT NOT NULL,
    weekday SMALLINT NOT NULL,
    CONSTRAINT time_pk PRIMARY KEY (ts)
);

CREATE TABLE IF NOT EXISTS airport (
    code VARCHAR(50) NOT NULL,
    name VARCHAR(500) NOT NULL,
    type VARCHAR(50),
    continent VARCHAR(50) NOT NULL,
    country_code VARCHAR(32) NOT NULL,
    state VARCHAR(32) NOT NULL,
    city VARCHAR(500) NOT NULL,
    CONSTRAINT airport_pk PRIMARY KEY (code)
);

CREATE TABLE IF NOT EXISTS demographic (
    city_name VARCHAR(500) NOT NULL,
    state_code VARCHAR(50) NOT NULL,
    state VARCHAR(50) NOT NULL,
    median_age FLOAT,
    male_population BIGINT,
    female_population BIGINT,
    total_population BIGINT,
    race  VARCHAR(50),
    count  BIGINT,
    CONSTRAINT demographic_pk PRIMARY KEY (city_name, state_code)
);
