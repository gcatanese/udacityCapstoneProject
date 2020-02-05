class SqlQueries:
    fact_table_insert = ("""
        INSERT  INTO fact_temperature (
        city,
        country,
        average_temperature,
        average_temperature_uncertainty,
        latitude,
        longitude,
        date
        )
        SELECT 
                std.city, 
                std.country, 
                std.average_temperature, 
                std.average_temperature_uncertainty, 
                std.latitude, 
                std.longitude, 
                std.dt 
            FROM staging_temperature_data std
    """)

    time_table_insert = ("""
        INSERT  INTO time (
        ts,
        day,
        week,
        month,
        year,
        weekday
        )
        SELECT 
                DISTINCT(std.dt), 
                extract(day from std.dt), 
                extract(week from std.dt), 
                extract(month from std.dt), 
                extract(year from std.dt), 
                extract(dayofweek from std.dt)
        FROM staging_temperature_data std
    """)

    airport_table_insert = ("""
        INSERT  INTO airport (
        code,
        name,
        type,
        continent,
        country_code,
        state,
        city
        )
        SELECT 
                ident, 
                name, 
                type, 
                continent, 
                iso_country,
                iso_region,
                municipality
        FROM staging_airport_code_data
    """)

    demographic_table_insert = ("""
        INSERT  INTO demographic (
        city_name,
        state_code,
        state,
        median_age,
        male_population,
        female_population,
        total_population,
        race,
        count
        )
        SELECT 
                city, 
                state_code,
                state, 
                median_age, 
                male_population, 
                female_population,
                total_population,
                race,
                count
        FROM staging_us_city_data
    """)


