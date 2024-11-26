# How to Run the Project

1. Go to the **Shaker directory**.

2. Run the `pip install -r requirements.txt` script:
   - This will install all the requirements to run the following codes.

3. (Already done) Run the `python3 get_countries.py` script:
   - Note that the countries_data.json is already found in the repository, and no need to run this command, but you can use it for reference:
        - This fetches data for 84 countries but shows an error for the rest.
        - It creates a new JSON file called `countries_data.json`.

4. (Already Done in countries_data.json, but should be done if the get_countries.py code is run) Open `countries_data.json`:
   - Add a closing bracket (`]`) at the end of the file.
   - This step is necessary because the error prevented the script from completing properly.

5. Run the `python3 un_all.py` script:
   - This adds the data from `countries_data.json` into the database.

6. (Optional) Test a specific country:
   - Run the `python3 un_test.py` script:
     ```bash
     python un_test.py
     ```
   - Input the name of the country in the terminal when prompted.
   - The script will add that country's information into the database.

