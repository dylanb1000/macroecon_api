from fredapi import Fred
from sqlalchemy import create_engine
from sqlalchemy.types import String
import pandas as pd
import sys

fred = Fred(api_key=sys.argv[1])

symbols = [
    "GDP",               
    "PAYEMS",            
    "UNRATE",            
    "CPIAUCSL",          
    "PCEPI",             
    "INDPRO",            
    "RSAFS",             
    "HOUST",             
    "EXHOSLUSM495S",     
    "CPATAX"                 
]

descriptions = [
    "Real Gross Domestic Product (GDP): Total inflation-adjusted output of goods and services produced in the U.S.",
    "Nonfarm Payroll Employment: Total number of paid U.S. workers excluding farm, government, and nonprofit employees.",
    "Unemployment Rate: Percentage of labor force unemployed and actively looking for jobs.",
    "Consumer Price Index (CPI-U): Measures average change over time in prices paid by urban consumers for a market basket.",
    "Personal Consumption Expenditures (PCE) Price Index: Measures price changes in goods and services consumed by individuals; Fed’s preferred inflation gauge.",
    "Industrial Production Index: Measures real output of manufacturing, mining, and utilities.",
    "Retail Sales (Total): Total monthly sales by retail stores (excluding food services).",
    "Housing Starts: Number of new residential building construction projects started.",
    "Existing Home Sales: Number of completed transactions for existing single-family homes.",
    "Gross Domestic Product: Corporate Profits After Tax: Profits earned by U.S. corporations after taxes, reflecting business sector health."
]

data = {s: fred.get_series(s) for s in symbols}
data_combined = pd.concat(data, axis=1)
data_description = pd.DataFrame(list(zip(symbols, descriptions)), columns=['symbol', 'description'])
print(data_description)
# MySQL connection info
user = 'root'
password = 'password'
host = 'db'
port = 3306
database = 'MACROECON'

engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')

data_combined.to_sql('macroecon_values', con=engine, if_exists='replace', index=True)
data_description.to_sql('macroecon_description', con=engine, if_exists='replace', index=True)
print("CSV imported into MySQL successfully!")