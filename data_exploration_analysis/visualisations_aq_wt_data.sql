-- NOTE: we don't incldue the creation of tables code because they are too generic. 
-- Here is only the code for the visuals


-- for the dashboard for Natarina Use case 
SELECT 
    DATE_FORMAT(Datum, '%H:00') AS Hour,
    AVG(CASE WHEN Parameter = 'T' THEN Wert END) AS Avg_Temperature_C
FROM 
    weather_2024
WHERE 
    Standort = 'Zch_Rosengartenstrasse'
    AND DATE(Datum) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
GROUP BY 
    Hour
ORDER BY 
    Hour;

SELECT 
    DATE_FORMAT(Datum, '%H:00') AS Hour,
    AVG(CASE WHEN Parameter = 'T' THEN Wert END) AS Avg_Temperature_C
FROM 
    weather_2024
WHERE 
    Standort = 'Zch_Stampfenbachstrasse'
    AND DATE(Datum) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
GROUP BY 
    Hour
ORDER BY 
    Hour;

SELECT 
    DATE_FORMAT(Datum, '%H:00') AS Hour,
    AVG(CASE WHEN Parameter = 'T' THEN Wert END) AS Avg_Temperature_C
FROM 
    weather_2024
WHERE 
    Standort = 'Zch_Schimmelstrasse'
    AND DATE(Datum) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
GROUP BY 
    Hour
ORDER BY 
    Hour;


-- for winf data Natarina dashobard again
SELECT 
    Standort AS Location_Name,
    'Rosengarten' AS Location,
    MAX(CASE WHEN Parameter = 'T' THEN Wert END) AS Max_Temperature_C,
    MAX(CASE WHEN Parameter = 'Hr' THEN Wert END) AS Max_Humidity_Percent,
    MAX(CASE WHEN Parameter = 'WVv' THEN Wert END) AS Max_Wind_Speed_m_s
FROM 
    weather_2024
WHERE 
    Standort = 'Zch_Rosengartenstrasse'
    AND DATE(Datum) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
GROUP BY 
    Standort
ORDER BY 
    Standort;

SELECT 
    Standort AS Location_Name,
    'Stampfenbachstr.' AS Location,
    MAX(CASE WHEN Parameter = 'T' THEN Wert END) AS Max_Temperature_C,
    MAX(CASE WHEN Parameter = 'Hr' THEN Wert END) AS Max_Humidity_Percent,
    MAX(CASE WHEN Parameter = 'WVv' THEN Wert END) AS Max_Wind_Speed_m_s
FROM 
    weather_2024
WHERE 
    Standort = 'Zch_Stampfenbachstrasse'
    AND DATE(Datum) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
GROUP BY 
    Standort
ORDER BY 
    Standort;

SELECT 
    Standort AS Location_Name,
    'Schimmelstrasse' AS Location,
    MAX(CASE WHEN Parameter = 'T' THEN Wert END) AS Max_Temperature_C,
    MAX(CASE WHEN Parameter = 'Hr' THEN Wert END) AS Max_Humidity_Percent,
    MAX(CASE WHEN Parameter = 'WVv' THEN Wert END) AS Max_Wind_Speed_m_s
FROM 
    weather_2024
WHERE 
    Standort = 'Zch_Schimmelstrasse'
    AND DATE(Datum) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
GROUP BY 
    Standort
ORDER BY 
    Standort;

-- graph top right time series 

SELECT 
    DATE(Datum) AS day, 
    ROUND(AVG(CASE WHEN Parameter = 'Hr' THEN Wert END), 2) AS avg_humidity,
    ROUND(SUM(CASE WHEN Parameter = 'RainDur' THEN Wert END), 2) AS total_rain_duration_minutes,
    ROUND(AVG(CASE WHEN Parameter = 'WVv' THEN Wert END), 2) AS avg_wind_speed_m_s
FROM 
    weather_2024
WHERE 
    Parameter IN ('Hr', 'RainDur', 'WVv') -- Select relevant parameters
    AND MONTH(Datum) = MONTH(CURDATE())
    AND YEAR(Datum) = YEAR(CURDATE())
GROUP BY 
    DATE(Datum)
ORDER BY 
    day ASC;

    
-- gantt circles
SELECT AVG(ozone) AS daily_avg_ozone
FROM air_quality_2024
WHERE DATE(time) = CURDATE() - INTERVAL 1 DAY;

SELECT AVG(nitrogen_dioxide) AS daily_avg_nitrogen_dioxide
FROM air_quality_2024
WHERE DATE(time) = CURDATE() - INTERVAL 1 DAY;

SELECT AVG(pm2_5) AS avg_pm2_5
FROM air_quality_2024
WHERE DATE(time) = DATE_SUB(CURDATE(), INTERVAL 1 DAY);

SELECT AVG(pm10) AS avg_pm10
FROM air_quality_2024
WHERE DATE(time) = DATE_SUB(CURDATE(), INTERVAL 1 DAY);

-- bottom graphs 
SELECT 
    DATE(time) AS day, 
    ROUND(AVG(pm10), 2) AS avg_pm10,
    ROUND(AVG(pm2_5), 2) AS avg_pm2_5,
    ROUND(AVG(ozone), 2) AS avg_ozone,
    ROUND(AVG(nitrogen_dioxide), 2) AS avg_no2
FROM    
    air_quality_2024
WHERE 
    MONTH(time) = MONTH(CURDATE()) 
    AND YEAR(time) = YEAR(CURDATE())
GROUP BY 
    DATE(time)
ORDER BY 
    day ASC;

SELECT 
    DATE(time) AS day, 
    ROUND(AVG(pm10), 2) AS avg_pm10,
    ROUND(AVG(pm2_5), 2) AS avg_pm2_5,
    ROUND(AVG(ozone), 2) AS avg_ozone,
    ROUND(AVG(nitrogen_dioxide), 2) AS avg_no2
FROM    
    air_quality_2024
WHERE 
    time >= '2024-10-01' AND time < '2024-11-01'
GROUP BY 
    DATE(time)
ORDER BY 
    day ASC;


-- Dashboard for Karin and Ralf 

SELECT 
    CAST(DATE_FORMAT(time, '%Y-%m-01') AS DATETIME) AS month_date, -- Cast to DATETIME for proper time axis in Grafana
    ROUND(AVG(ozone), 2) AS avg_ozone,
    ROUND(AVG(sulphur_dioxide), 2) AS avg_so2,
    ROUND(AVG(carbon_monoxide), 2) AS avg_co,
    ROUND(AVG(nitrogen_dioxide), 2) AS avg_no2
FROM 
    air_quality_2020
WHERE 
    ozone IS NOT NULL
    AND sulphur_dioxide IS NOT NULL
    AND carbon_monoxide IS NOT NULL
    AND nitrogen_dioxide IS NOT NULL
GROUP BY 
    DATE_FORMAT(time, '%Y-%m')

UNION ALL

SELECT 
    CAST(DATE_FORMAT(time, '%Y-%m-01') AS DATETIME) AS month_date,
    ROUND(AVG(ozone), 2) AS avg_ozone,
    ROUND(AVG(sulphur_dioxide), 2) AS avg_so2,
    ROUND(AVG(carbon_monoxide), 2) AS avg_co,
    ROUND(AVG(nitrogen_dioxide), 2) AS avg_no2
FROM 
    air_quality_2021
WHERE 
    ozone IS NOT NULL
    AND sulphur_dioxide IS NOT NULL
    AND carbon_monoxide IS NOT NULL
    AND nitrogen_dioxide IS NOT NULL
GROUP BY 
    DATE_FORMAT(time, '%Y-%m')

UNION ALL

SELECT 
    CAST(DATE_FORMAT(time, '%Y-%m-01') AS DATETIME) AS month_date,
    ROUND(AVG(ozone), 2) AS avg_ozone,
    ROUND(AVG(sulphur_dioxide), 2) AS avg_so2,
    ROUND(AVG(carbon_monoxide), 2) AS avg_co,
    ROUND(AVG(nitrogen_dioxide), 2) AS avg_no2
FROM 
    air_quality_2022
WHERE 
    ozone IS NOT NULL
    AND sulphur_dioxide IS NOT NULL
    AND carbon_monoxide IS NOT NULL
    AND nitrogen_dioxide IS NOT NULL
GROUP BY 
    DATE_FORMAT(time, '%Y-%m')

UNION ALL

SELECT 
    CAST(DATE_FORMAT(time, '%Y-%m-01') AS DATETIME) AS month_date,
    ROUND(AVG(ozone), 2) AS avg_ozone,
    ROUND(AVG(sulphur_dioxide), 2) AS avg_so2,
    ROUND(AVG(carbon_monoxide), 2) AS avg_co,
    ROUND(AVG(nitrogen_dioxide), 2) AS avg_no2
FROM 
    air_quality_2023
WHERE 
    ozone IS NOT NULL
    AND sulphur_dioxide IS NOT NULL
    AND carbon_monoxide IS NOT NULL
    AND nitrogen_dioxide IS NOT NULL
GROUP BY 
    DATE_FORMAT(time, '%Y-%m')

UNION ALL

SELECT 
    CAST(DATE_FORMAT(time, '%Y-%m-01') AS DATETIME) AS month_date,
    ROUND(AVG(ozone), 2) AS avg_ozone,
    ROUND(AVG(sulphur_dioxide), 2) AS avg_so2,
    ROUND(AVG(carbon_monoxide), 2) AS avg_co,
    ROUND(AVG(nitrogen_dioxide), 2) AS avg_no2
FROM 
    air_quality_2024
WHERE 
    ozone IS NOT NULL
    AND sulphur_dioxide IS NOT NULL
    AND carbon_monoxide IS NOT NULL
    AND nitrogen_dioxide IS NOT NULL
GROUP BY 
    DATE_FORMAT(time, '%Y-%m')
ORDER BY 
    month_date ASC; -- Sort by proper date

SELECT 
    DATE(Datum) AS day, 
    ROUND(AVG(CASE WHEN Parameter = 'Hr' THEN Wert END), 2) AS avg_humidity,
    ROUND(AVG(CASE WHEN Parameter = 'WVv' THEN Wert END), 2) AS avg_wind_speed_m_s
FROM (
    SELECT * FROM weather_2020
    UNION ALL
    SELECT * FROM weather_2021
    UNION ALL
    SELECT * FROM weather_2022
    UNION ALL
    SELECT * FROM weather_2023
    UNION ALL
    SELECT * FROM weather_2024
) AS combined_data
WHERE 
    Parameter IN ('Hr', 'WVv')
GROUP BY 
    DATE(Datum)
ORDER BY 
    day ASC;


SELECT 
    DATE(Datum) AS day, 
    ROUND(SUM(CASE WHEN Parameter = 'RainDur' THEN Wert END), 2) AS total_rain_duration_minutes
FROM (
    SELECT * FROM weather_2020
    UNION ALL
    SELECT * FROM weather_2021
    UNION ALL
    SELECT * FROM weather_2022
    UNION ALL
    SELECT * FROM weather_2023
    UNION ALL
    SELECT * FROM weather_2024
) AS combined_data
WHERE 
    Parameter IN ('RainDur')
GROUP BY 
    DATE(Datum)
ORDER BY 
    day ASC;


SELECT 
    DATE(time) AS day, 
    ROUND(AVG(pm10), 2) AS avg_pm10,
    ROUND(AVG(pm2_5), 2) AS avg_pm2_5
FROM 
    air_quality_2024
WHERE 
    MONTH(time) = MONTH(CURDATE()) 
    AND YEAR(time) = YEAR(CURDATE())
GROUP BY 
    DATE(time)
ORDER BY 
    day ASC;


SELECT 
    DATE(time) AS day, 
    ROUND(AVG(nitrogen_dioxide), 2) AS avg_no2,
    ROUND(AVG(ozone), 2) AS avg_ozone
FROM 
    air_quality_2024
WHERE 
    MONTH(time) = MONTH(CURDATE()) 
    AND YEAR(time) = YEAR(CURDATE())
GROUP BY 
    DATE(time)
ORDER BY 
    day ASC;


