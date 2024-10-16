import requests
import pandas as pd
import mysql.connector
from datetime import datetime

# Replace with your actual API key and MySQL credentials
API_KEY = "**************"  
BASE_URL = "http://api.openweathermap.org/data/2.5/weather"
CITIES = ["Nairobi", "Mombasa", "Kisumu", "Nakuru", "Eldoret", "Thika", "Malindi"]  # List of Kenyan cities

# MySQL connection setup
db_connection = mysql.connector.connect(
    host="***********",
    user="*******",
    port="******",
    password="******",
    database="********",
)

cursor = db_connection.cursor()
# Ensure the table exists
create_table_query = """
    CREATE TABLE IF NOT EXISTS weather_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        city_name VARCHAR(100),
        temperature FLOAT,
        humidity INT,
        wind_speed FLOAT,
        weather_description VARCHAR(255),
        timestamp DATETIME
    );
"""
cursor.execute(create_table_query)
db_connection.commit()
print("Table 'weather_data' ensured to exist.")


# Function to collect weather data for a single city
def collect_weather_data(city):
    try:
        url = f"{BASE_URL}?q={city},KE&appid={API_KEY}&units=metric"
        response = requests.get(url)
        response.raise_for_status()

        weather_data = response.json()
        print(f"API Response for {city}:", weather_data)

        city_name = weather_data['name']
        country = weather_data['sys']['country']
        temperature = weather_data['main']['temp']
        humidity = weather_data['main']['humidity']
        wind_speed = weather_data['wind']['speed']
        weather_description = weather_data['weather'][0]['description']
        timestamp = datetime.now()

        # Print the data before insertion for debugging
        print(f"Inserting: {city_name}, {temperature}, {humidity}, {wind_speed}, {weather_description}, {timestamp}")

        insert_query = """
            INSERT INTO weather_data (city_name, temperature, humidity, wind_speed, weather_description, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (city_name, temperature, humidity, wind_speed, weather_description, timestamp))
        db_connection.commit()  # Ensure the transaction is committed
        print(f"{cursor.rowcount} rows inserted.")

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error for {city}: {http_err}")
    except mysql.connector.Error as db_err:
        print(f"Database error for {city}: {db_err}")
    except KeyError as key_err:
        print(f"Key error: {key_err} not found in {city}'s API response")
    except Exception as e:
        print(f"An unexpected error occurred for {city}: {e}")

# Loop through each city in the list and collect weather data
for city in CITIES:
    collect_weather_data(city)

# Query the weather data from the MySQL database for Power BI
query = "SELECT * FROM weather_data;"
df = pd.read_sql(query, db_connection)

# Close the database connection
cursor.close()
db_connection.close()

# The 'df' DataFrame will be used in Power BI
dataset = df
