# %%
import requests
import pandas as pd
import json

# %%
import requests

def get_coordinates(city_names, city_regions, country="Greece"):
    """
    Fetches coordinates (latitude and longitude) for a list of city names and includes their regions.
    
    Parameters:
        city_names (list): List of city names (e.g., ["Athens", "Thessaloniki"]).
        city_regions (dict): Dictionary mapping cities to their respective regions.
        country (str): Country name to narrow down the search (default: Greece).
    
    Returns:
        dict: A dictionary where keys are city names and values include coordinates and region.
    """
    base_url = "https://nominatim.openstreetmap.org/search"
    headers = {"User-Agent": "Mozilla/5.0"}
    coordinates = {}
    
    for city in city_names:
        params = {
            "q": f"{city}, {country}",
            "format": "json",
            "limit": 1  # Fetch only the top result
        }
        response = requests.get(base_url, params=params, headers=headers)
        
        if response.status_code == 200 and response.json():
            data = response.json()[0]
            coordinates[city] = {
                "region": city_regions.get(city, "Unknown"),  # Get the region for the city
                "lat": data["lat"],
                "lon": data["lon"]
            }
        else:
            coordinates[city] = {
                "region": city_regions.get(city, "Unknown"),
                "lat": None,
                "lon": None
            }
    
    return coordinates

# List of cities to get coordinates for
cities = [
    "Thessaloniki", "Kozani", "Florina", "Neos Marmaras", "Volos", "Trikala", 
    "Ioannina", "Zagori", "Agrinio", "Athens", "Chalkida", "Patras", 
    "Kalamata", "Corinth", "Tripoli", "Chania", "Heraklion", "Corfu", 
    "Santorini", "Rhodes", "Kos", "Alexandroupoli", "Komotini", "Drama", "Kavala"
]

# Dictionary mapping cities to their regions
city_regions = {
    "Thessaloniki": "Central Macedonia",
    "Kozani": "Western Macedonia",
    "Florina": "Western Macedonia",
    "Neos Marmaras": "Central Macedonia",
    "Volos": "Thessaly",
    "Trikala": "Thessaly",
    "Ioannina": "Epirus",
    "Zagori": "Epirus",
    "Agrinio": "Western Greece",
    "Athens": "Central Greece",
    "Chalkida": "Central Greece",
    "Patras": "Western Greece",
    "Kalamata": "Peloponnese",
    "Corinth": "Peloponnese",
    "Tripoli": "Peloponnese",
    "Chania": "Crete",
    "Heraklion": "Crete",
    "Corfu": "Ionian Islands",
    "Santorini": "Cyclades",
    "Rhodes": "Dodecanese",
    "Kos": "Dodecanese",
    "Alexandroupoli": "North Eastern Greece",
    "Komotini": "North Eastern Greece",
    "Drama": "North Eastern Greece",
    "Kavala":"North Eastern Greece"
}

# Fetch coordinates with regions
city_coordinates = get_coordinates(cities, city_regions)

# Display the results
for city, data in city_coordinates.items():
    if data["lat"] and data["lon"]:
        print(f"{city} ({data['region']}): Latitude {data['lat']}, Longitude {data['lon']}")
    else:
        print(f"{city} ({data['region']}): Coordinates not found.")


# %%
city_coordinates

# %%
# Save the results to a JSON file
output_file = "city_coordinates.json"

with open(output_file, "w") as json_file:
    json.dump(city_coordinates, json_file, indent=4)  # Write to the JSON file with formatting


# %%



