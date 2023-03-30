-- Create the dim_date table if it doesn't exist
CREATE TABLE IF NOT EXISTS jet.dim_date (
    date        DATE PRIMARY KEY,
    day         INTEGER NOT NULL,
    month       INTEGER NOT NULL,
    year        INTEGER NOT NULL,
    week        CHAR(10) NOT NULL,
    quarter     char(2) NOT NULL,
    holiday     integer
);

-- Generate a series of dates from 1990-01-01 to 2030-12-31
CREATE TEMP TABLE temp_dates AS
SELECT (generate_series('1990-01-01'::date, '2030-12-31'::date, '1 day'::interval)) AS date;

-- Insert data into the dim_date table
INSERT INTO jet.dim_date (date, day, month, year, week, quarter, holiday)
SELECT 
    date,
    EXTRACT(DAY FROM date),
    EXTRACT(MONTH FROM date),
    EXTRACT(YEAR FROM date),
    TO_CHAR(date, 'Day'),
    'Q' || TO_CHAR(date, 'Q'),
    CASE 
        -- Mark all December 25th and January 1st dates as holidays
        WHEN EXTRACT(MONTH FROM date) = 12 AND EXTRACT(DAY FROM date) = 25 THEN 1
        WHEN EXTRACT(MONTH FROM date) = 1 AND EXTRACT(DAY FROM date) = 1 THEN 1
        ELSE 0
    END AS holiday
FROM temp_dates;

-- Clean up temporary table
DROP TABLE temp_dates;