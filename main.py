import json

import pandas as pd

# Load and process bookings.json
with open("bookings.json") as bookings_json: 
  bookings = json.load(bookings_json)
  df = pd.DataFrame(bookings)
  df

# Load and process companies.json
with open("companies.json") as companies_json:
  companies = json.load(companies_json)
  df_companies = pd.DataFrame(companies)
  print(df_companies.columns)
df_companies

# Load and process events.json
with open("events.json") as events_json:
  events = json.load(events_json)
  df_events = pd.DataFrame(events)
  df_events

# Load and process experiences.json
with open("experiences.json") as experiences_json:
  experiences = json.load(experiences_json)
  df_experiences = pd.DataFrame(experiences)
  df_experiences

# Load and process giftcards.json
with open("giftcards.json") as giftcards_json:
  giftcards = json.load(giftcards_json)
  df_giftcards = pd.DataFrame(giftcards)
  df_giftcards

# Load and process locations.json
with open("locations.json") as locations_json:
  locations = json.load(locations_json)
  df_locations = pd.DataFrame(locations)
  df_locations

# Load and process receipts.json
with open("receipts.json") as receipts_json:
  receipts = json.load(receipts_json)
  df_receipts = pd.DataFrame(receipts)
  df_receipts

# Load and process users.json
with open("users.json") as users_json:
  users = json.load(users_json)
df_users = pd.DataFrame(users)
df_users

# Drop duplicates in df based on specific columns
df = df.drop_duplicates(subset=["updated"])
df = df.drop_duplicates(subset=["revision"])
df = df.drop_duplicates(subset=["created"])
df = df.drop_duplicates(subset=["partition"])


# Drop duplicates in bookings based on specific columns
df_bookings = bookings.drop_duplicates(subset=[
    "updated", "revision", "created", 
    "partition", "seq", "key", "document"
])


# Drop duplicates in df_companies based on specific columns
df_companies = companies.drop_duplicates(subset=[
    "updated", "revision", "created", 
    "partition", "seq", "key", "document.website" , 
  "document.features.giftCards.enabled", 
  "document.features.requests.params.emailAddress",
  "document.features.requests.enabled", "document.guides",
  "document.ownerName", "document.companyEmail", "document.languages",
  "document.companyPhone", "document.defaultCurrency", "document.name",
  "document.cvrNr", "document.description.da", "document.description.en",
  "document.description.no" , "document.description.sv", 
  "document.description.de", "document.domains", "document.location.country",
  "document.location.zipCode" , "document.location.address", "document.location.city",
  "document.id", "document.onboardingCompleted", "document.admins", 
  "document.pictures.cover.key", "document.pictures.cover.url", 
  "document.pictures.logo", "document.pictures.logo"
])

# Drop duplicates in df_events 
df_events = events.drop_duplicates()

# Drop duplicates in df_experiences 
df_experiences = experiences.drop_duplicates()

# Drop duplicates in df_giftcards
df_giftcards = giftcards.drop_duplicates()

# Drop duplicates in df_locations
df_locations = locations.drop_duplicates()

# Drop duplicates in df_receipts
df_receipts = receipts.drop_duplicates()

# Drop duplicates in df_users
df_users = users.drop_duplicates()

# Convert non-numeric columns to string to avoid type errors for df_bookigs
df_bookings = df_bookings.applymap(lambda x: str(x)
                               if not pd.api.types.is_numeric_dtype(x) else x)

# Convert columns to appropriate data types
df_bookings = df_bookings.infer_objects()

# Drop rows with misstyped values
df_bookings = df_bookings.dropna()

# Convert non-numeric columns to string to avoid type errors for df_comanies
df_companies = df_companies.applymap(lambda x: str(x)
                                     if not pd.api.types.is_numeric_dtype(x) else x)

# Convert columns to appropriate data types
df_companies = df_companies.infer_objects()

# Drop rows with misstyped values
df_companies = df_companies.dropna()

# Convert non-numeric columns to string to avoid type errors for df_events
df_events = df_events.applymap(lambda x: str(x)
                               if not pd.api.types.is_numeric_dtype(x) else x)

# Convert columns to appropriate data types
df_events = df_events.infer_objects()

# Drop rows with misstyped values
df_events = df_events.dropna()

# Convert non-numeric columns to string to avoid type errors for df_experiences
df_experiences = df_experiences.applymap(lambda x: str(x)
                                         if not pd.api.types.is_numeric_dtype(x) else x)

# Convert columns to appropriate data types
df_experiences = df_experiences.infer_objects()

# Drop rows with misstyped values
df_experiences = df_experiences.dropna()

# Convert non-numeric columns to string to avoid type errors for df_giftcards
df_giftcards = df_giftcards.applymap(lambda x: str(x)
                                     if not pd.api.types.is_numeric_dtype(x) else x)

# Convert columns to appropriate data types
df_giftcards = df_giftcards.infer_objects()

# Drop rows with misstyped values
df_giftcards = df_giftcards.dropna()

# Convert non-numeric columns to string to avoid type errors for df_locations
df_locations = df_locations.applymap(lambda x: str(x)
                                     if not pd.api.types.is_numeric_dtype(x) else x)

# Convert columns to appropriate data types
df_locations = df_locations.infer_objects()

# Drop rows with misstyped values
df_locations = df_locations.dropna()

# Convert non-numeric columns to string to avoid type errors for df_receipts
df_receipts = df_receipts.applymap(lambda x: str(x)
                                   if not pd.api.types.is_numeric_dtype(x) else x)

# Convert columns to appropriate data types
df_receipts = df_receipts.infer_objects()

# Drop rows with misstyped values
df_receipts = df_receipts.dropna()

# Convert non-numeric columns to string to avoid type errors for df_users
df_users = df_users.applymap(lambda x: str(x)
                             if not pd.api.types.is_numeric_dtype(x) else x)

# Convert columns to appropriate data types
df_users = df_users.infer_objects()

# Drop rows with misstyped values
df_users = df_users.dropna()

# Check for null values for df_bookings
df_bookings.isnull().sum()
 # Check for null values for df_companies
df_companies.isnull().sum()
# Check for null values for df_events
df_events.isnull().sum()
# Check for null values for df_experiences
df_experiences.isnull().sum()
# Check for null values for df_giftcards
df_giftcards.isnull().sum()
# Check for null values for df_locations
df_locations.isnull().sum()
# Check for null values for df_receipts
df_receipts.isnull().sum()
# Check for null values for df_users
df_users.isnull().sum()

# Replace NaN values with 0 in the df_bookings dataframe
df_bookings_filled = df_bookings.fillna(0)
# Replace NaN values with 0 in the df_companies dataframe
df_companies_filled = df_companies.fillna(0)
# Replace NaN values with 0 in the df_events dataframe
df_events_filled = df_events.fillna(0)
# Replace NaN values with 0 in the df_experiences dataframe
df_experiences_filled = df_experiences.fillna(0)
# Replace NaN values with 0 in the df_giftcards dataframe
df_giftcards_filled = df_giftcards.fillna(0)
# Replace NaN values with 0 in the df_receipts dataframe
df_receipts_filled = df_receipts.fillna(0)
# Replace NaN values with 0 in the df_users dataframe
df_users_filled = df_users.fillna(0)