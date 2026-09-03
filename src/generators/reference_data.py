"""Synthetic operational data generator -- curated German reference data.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: August 2026

Purpose: fixed, hand-curated lists (German cities with a plausible postcode
prefix, names, street stems, the tariff book, product catalogue). No external
dependency -- deterministic inputs only.
"""

from __future__ import annotations

# (city, 5-digit postcode) -- one representative postcode per city.
CITIES = [
    ("Berlin", "10115"),
    ("Hamburg", "20095"),
    ("Munich", "80331"),
    ("Cologne", "50667"),
    ("Frankfurt am Main", "60311"),
    ("Stuttgart", "70173"),
    ("Duesseldorf", "40210"),
    ("Leipzig", "04109"),
    ("Dortmund", "44135"),
    ("Essen", "45127"),
    ("Bremen", "28195"),
    ("Dresden", "01067"),
    ("Hannover", "30159"),
    ("Nuremberg", "90402"),
    ("Bonn", "53111"),
    ("Muenster", "48143"),
    ("Karlsruhe", "76133"),
    ("Mannheim", "68159"),
    ("Augsburg", "86150"),
    ("Wiesbaden", "65183"),
]

FIRST_NAMES = [
    "Lukas",
    "Leon",
    "Finn",
    "Elias",
    "Paul",
    "Ben",
    "Jonas",
    "Noah",
    "Felix",
    "Maximilian",
    "Emma",
    "Mia",
    "Hannah",
    "Emilia",
    "Sofia",
    "Lena",
    "Marie",
    "Anna",
    "Lea",
    "Clara",
    "Thomas",
    "Michael",
    "Andreas",
    "Stefan",
    "Julia",
    "Katharina",
    "Sabine",
    "Christian",
    "Nicole",
    "Martina",
]

LAST_NAMES = [
    "Mueller",
    "Schmidt",
    "Schneider",
    "Fischer",
    "Weber",
    "Meyer",
    "Wagner",
    "Becker",
    "Schulz",
    "Hoffmann",
    "Schaefer",
    "Koch",
    "Bauer",
    "Richter",
    "Klein",
    "Wolf",
    "Schroeder",
    "Neumann",
    "Schwarz",
    "Zimmermann",
    "Braun",
    "Krueger",
    "Hofmann",
    "Hartmann",
    "Lange",
    "Werner",
    "Krause",
    "Lehmann",
]

STREET_STEMS = [
    "Haupt",
    "Bahnhof",
    "Garten",
    "Schul",
    "Kirch",
    "Berg",
    "Wald",
    "Linden",
    "Birken",
    "Eichen",
    "Ahorn",
    "Rosen",
    "Feld",
    "Wiesen",
    "Mühlen",
    "Ring",
    "Markt",
    "Schloss",
    "Brunnen",
    "Sonnen",
]
STREET_SUFFIXES = ["strasse", "weg", "allee", "platz", "gasse"]

# Energy tariff book -- (code, name, energy_type, unit_price_eur_per_kwh,
# standing_charge_eur_per_month, term_months).
TARIFFS = [
    ("STROM-BASIS", "Strom Basis", "electricity", 0.3190, 9.90, 12),
    ("STROM-FIX24", "Strom Fix 24", "electricity", 0.2980, 11.90, 24),
    ("STROM-OEKO", "Strom Oeko", "electricity", 0.3350, 10.90, 12),
    ("STROM-NACHT", "Strom Nacht", "electricity", 0.2740, 12.90, 12),
    ("GAS-BASIS", "Gas Basis", "gas", 0.1120, 12.90, 12),
    ("GAS-FIX24", "Gas Fix 24", "gas", 0.1050, 14.90, 24),
    ("GAS-OEKO", "Gas Oeko", "gas", 0.1230, 13.90, 12),
    ("GAS-KOMFORT", "Gas Komfort", "gas", 0.1180, 15.90, 18),
]

# Product catalogue -- (sku, name, category, unit_price_eur).
PRODUCTS = [
    ("MET-EL-BASIC", "Smart Electricity Meter Basic", "meter", 79.00),
    ("MET-EL-PLUS", "Smart Electricity Meter Plus", "meter", 129.00),
    ("MET-GAS-BASIC", "Smart Gas Meter Basic", "meter", 89.00),
    ("GW-HOME-1", "Home Energy Gateway", "gateway", 149.00),
    ("PLUG-SMART-1", "Smart Plug", "smart_home", 24.90),
    ("PLUG-SMART-3", "Smart Plug 3-Pack", "smart_home", 64.90),
    ("THERMO-1", "Smart Radiator Thermostat", "smart_home", 54.90),
    ("THERMO-4", "Radiator Thermostat 4-Pack", "smart_home", 199.00),
    ("SENSOR-DOOR", "Door and Window Sensor", "smart_home", 19.90),
    ("SENSOR-MOTION", "Motion Sensor", "smart_home", 29.90),
    ("DISPLAY-1", "In-Home Energy Display", "display", 44.90),
    ("EVSE-11KW", "Wallbox EV Charger 11 kW", "ev_charging", 649.00),
    ("EVSE-22KW", "Wallbox EV Charger 22 kW", "ev_charging", 899.00),
    ("CABLE-T2-5M", "EV Charging Cable Type 2 5 m", "ev_charging", 179.00),
    ("PV-STARTER", "Balcony Solar Starter 600 W", "solar", 549.00),
    ("PV-800", "Balcony Solar 800 W", "solar", 699.00),
    ("BAT-2KWH", "Home Battery 2 kWh Module", "storage", 1290.00),
    ("BAT-5KWH", "Home Battery 5 kWh Module", "storage", 2790.00),
    ("LED-KIT", "Smart LED Starter Kit", "smart_home", 39.90),
    ("REPEATER-1", "Zigbee Range Extender", "smart_home", 22.90),
    ("CT-CLAMP", "Clamp-on Power Sensor", "meter", 34.90),
    ("SUB-CARE-1", "Care Plan 12 Months", "service", 59.00),
    ("SUB-CARE-2", "Care Plan 24 Months", "service", 99.00),
    ("INSTALL-1", "Professional Installation Visit", "service", 129.00),
]
