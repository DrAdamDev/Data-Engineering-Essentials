import requests
import pandas as pd
from io import BytesIO

pd.set_option('future.no_silent_downcasting', True)

def extract_excel_data(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        # Wrap the byte string in a BytesIO object
        byte_stream = BytesIO(response.content)
        dfs = pd.read_excel(BytesIO(response.content), sheet_name=None)
        
        return dfs
    except (requests.exceptions.RequestException, pd.errors.ParserError) as e:
        print("Error occurred during data extraction:", str(e))
        return None
