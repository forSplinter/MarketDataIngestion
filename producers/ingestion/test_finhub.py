import os
import finnhub


finnhub_client = finnhub.Client(api_key=FINNUB_API_KEY)

print(finnhub_client.symbol_lookup("apple"))
