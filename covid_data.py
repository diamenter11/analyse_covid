# import requests
# from pyspark.sql import SparkSession
# import os

# # Create Spark session
# spark = SparkSession.builder \
#     .appName("CovidData") \
#     .config("spark.sql.warehouse.dir", "/temp") \
#     .config("spark.hadoop.security.authentication", "simple") \
#     .config("spark.hadoop.security.authorization", "false") \
#     .getOrCreate()

# # API URLs
# global_url = "https://disease.sh/v3/covid-19/all"
# countries_url = "https://disease.sh/v3/covid-19/countries"

# # Folder to store data
# folder_path = "C:\\Users\\natha\\Desktop\\source"
# if not os.path.exists(folder_path):
#     os.makedirs(folder_path)

# # Function to fetch data from the API
# def fetch_data(url):
#     response = requests.get(url)
#     return response.json()

# # Fetch global data
# global_data = fetch_data(global_url)
# global_df = spark.createDataFrame([global_data])

# # Save global data
# global_df.write.csv(os.path.join(folder_path, "global_data.csv"), header=True)

# # List of countries to fetch data for
# countries = ["Cameroon", "France", "Germany", "USA"]

# for country in countries:
#     # Fetch country-specific data
#     country_data = fetch_data(f"{countries_url}/{country}")
#     country_df = spark.createDataFrame([country_data])
    
#     # Save each country's data
#     country_df.write.csv(os.path.join(folder_path, f"{country}_data.csv"), header=True)

# spark.stop()












# import requests
# from pyspark.sql import SparkSession

# # Initialize Spark session
# spark = SparkSession.builder \
#     .appName("CovidData") \
#     .getOrCreate()

# # Function to fetch data from the Disease.sh API
# def fetch_covid_data():
#     # Fetch global data
#     global_response = requests.get("https://disease.sh/v3/covid-19/all")
#     global_data = global_response.json()
    
#     # Fetch data for specific countries
#     countries = ["Cameroon", "France", "Germany", "USA"]
#     country_data = []
#     for country in countries:
#         response = requests.get(f"https://disease.sh/v3/covid-19/countries/{country}")
#         country_data.append(response.json())
    
#     return global_data, country_data

# # Fetch data
# global_data, country_data = fetch_covid_data()

# # Prepare DataFrames
# global_df = spark.createDataFrame([{
#     "country": "Global",
#     "cases": global_data["cases"],
#     "deaths": global_data["deaths"],
#     "recovered": global_data["recovered"],
# }])

# country_dfs = []
# for data in country_data:
#     country_df = spark.createDataFrame([{
#         "country": data["country"],
#         "cases": data["cases"],
#         "deaths": data["deaths"],
#         "recovered": data["recovered"],
#     }])
#     country_dfs.append(country_df)

# # Show DataFrames in the terminal
# print("Global COVID-19 Data:")
# global_df.show()

# for df in country_dfs:
#     print(f"{df.first()['country']} COVID-19 Data:")
#     df.show()

# # Stop the Spark session
# spark.stop()







