# How to Run the Project

1. Go to the **Shaker directory**.

2. Run the `get_countries.py` script:
   - This fetches data for 84 countries but shows an error for the rest.
   - It creates a new JSON file called `countries_data.json`.

3. Open `countries_data.json`:
   - Add a closing bracket (`]`) at the end of the file.
   - This step is necessary because the error prevented the script from completing properly.

4. Run the `un_all.py` script:
   - This adds the data from `countries_data.json` into the database.

5. (Optional) Test a specific country:
   - Run the `un_test.py` script:
     ```bash
     python un_test.py
     ```
   - Input the name of the country in the terminal when prompted.
   - The script will add that country's information into the database.
