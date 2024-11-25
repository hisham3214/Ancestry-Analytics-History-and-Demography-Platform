import requests
import ijson
import json
import io
from decimal import Decimal

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)  # or str(obj) if you prefer
        return super(DecimalEncoder, self).default(obj)

url = "https://restcountries.com/v3.1/all"

try:
    with requests.get(url, stream=True, timeout=60) as response:
        response.raise_for_status()
        # Ensure the raw response is decompressed
        response.raw.decode_content = True
        # Wrap the raw response in a TextIOWrapper for text processing
        with io.TextIOWrapper(response.raw, encoding='utf-8') as text_stream:
            with open("countries_data.json", "w", encoding="utf-8") as outfile:
                outfile.write('[')  # Start of JSON array
                first_item = True
                # Parse each item in the JSON array
                parser = ijson.items(text_stream, 'item')
                for item in parser:
                    if not first_item:
                        outfile.write(',\n')  # Add a comma before the next item
                    else:
                        first_item = False
                    # Write the JSON item to the file using the custom encoder
                    json.dump(item, outfile, ensure_ascii=False, cls=DecimalEncoder)
                outfile.write(']')  # End of JSON array
        print("Data has been saved to 'countries_data.json'")
except requests.exceptions.RequestException as e:
    print(f"Error fetching data: {e}")
except ijson.common.IncompleteJSONError as e:
    print(f"JSON parsing error: {e}")