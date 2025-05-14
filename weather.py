# AQI Aggregator Script: Match Exactly Crop Yield States and Complete Matrix

import pandas as pd

# Load the AQI Kaggle dataset
df_aqi = pd.read_csv(r"C:\Users\goura\Downloads\city_day.csv\city_day.csv")

# Convert 'Date' to datetime format
df_aqi['Date'] = pd.to_datetime(df_aqi['Date'])

# Extract year from date
df_aqi['Year'] = df_aqi['Date'].dt.year

# Updated Mapping: Cities matching Crop Yield Dataset States
city_to_state = {
    "Ahmedabad": "Gujarat",
    "Amritsar": "Punjab",
    "Bengaluru": "Karnataka",
    "Chennai": "Tamil Nadu",
    "Mumbai": "Maharashtra",
    "Kolkata": "West Bengal",
    "Lucknow": "Uttar Pradesh",
    "Patna": "Bihar",
    "Thiruvananthapuram": "Kerala",
    "Guwahati": "Assam",
    "Hyderabad": "Telangana",
    "Delhi": "Delhi",
    "Bhopal": "Madhya Pradesh",
    "Bhubaneswar": "Odisha",
    "Jaipur": "Rajasthan",
    "Faridabad": "Haryana",
    "Dehradun": "Uttarakhand",
    "Ranchi": "Jharkhand",
    "Raipur": "Chhattisgarh"
}

# Filter only relevant cities
df_aqi_filtered = df_aqi[df_aqi['City'].isin(city_to_state.keys())]

# Map city to state
df_aqi_filtered['State'] = df_aqi_filtered['City'].map(city_to_state)

# Focus only on 2015–2020
df_aqi_filtered = df_aqi_filtered[(df_aqi_filtered['Year'] >= 2015) & (df_aqi_filtered['Year'] <= 2020)]

# Remove rows where AQI is missing
df_aqi_filtered = df_aqi_filtered.dropna(subset=['AQI'])

# Group by State and Year, and compute average AQI
state_year_aqi = df_aqi_filtered.groupby(['State', 'Year'])['AQI'].mean().reset_index()

# Rename columns
state_year_aqi.rename(columns={'AQI': 'Avg_AQI'}, inplace=True)

# --- Create Complete State-Year Matrix ---
all_states = list(city_to_state.values())
all_years = list(range(2015, 2021))

# Create full matrix of states x years
full_matrix = pd.MultiIndex.from_product([all_states, all_years], names=['State', 'Year']).to_frame(index=False)

# Merge actual AQI values onto the full matrix
state_year_aqi_complete = full_matrix.merge(state_year_aqi, on=['State', 'Year'], how='left')

# Save final output
state_year_aqi_complete.to_csv("State_Year_AQI_Complete.csv", index=False)

print("✅ Successfully aggregated AQI per State-Year with complete matrix, matching your crop yield states!")

# --- OPTIONAL: Join with Crop Yield ---

# Load your crop yield dataset
df_crop = pd.read_csv(r"C:\Users\goura\Downloads\crop_yield.csv\crop_yield.csv")

# Join Crop Data + AQI on State + Year
df_crop_aqi_joined = df_crop.merge(state_year_aqi_complete, left_on=['State', 'Crop_Year'], right_on=['State', 'Year'], how='left')

# Drop redundant 'Year' column from AQI side
df_crop_aqi_joined = df_crop_aqi_joined.drop(columns=['Year'])

# Save final joined dataset
df_crop_aqi_joined.to_csv("Final_Crop_Yield_with_AQI.csv", index=False)

print("✅ Final Crop Yield + AQI dataset created!")