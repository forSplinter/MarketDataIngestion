import os
import finnhub

FINNUB_API_KEY = "d0lkmchr01qsju09gju0d0lkmchr01qsju09gjug"

finnhub_client = finnhub.Client(api_key=FINNUB_API_KEY)

print(finnhub_client.symbol_lookup("apple"))
