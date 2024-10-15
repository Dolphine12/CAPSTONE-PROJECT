USE Weather_forecast
CREATE TABLE weather_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    city VARCHAR(100),
    temperature FLOAT,
    humidity INT,
    wind_speed FLOAT,
    weather_description VARCHAR(255),
    time DATETIME
);

ALTER table weather_data 
rename column city to city_name,
rename column time to timestamp;

SELECT * FROM weather_data;


