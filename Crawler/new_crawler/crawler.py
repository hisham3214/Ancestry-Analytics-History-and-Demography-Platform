import requests
from bs4 import BeautifulSoup
import openai
import numpy as np
import tiktoken  # optional, for token counting
import os
import json
import mysql.connector
import sys
# Add the parent directory to the system path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from main import config  # Import the database configuration from main.py


from gpt_config import key
# Set your OpenAI API Key as an environment variable or directly
openai.api_key = key


# to insert the data into the database:
def insert_llm_data_into_db(llm_data):
    """
    This function takes the JSON dictionary from ask_llm and inserts the data into the database.
    It assumes the llm_data structure matches the schema you've specified.
    """
    try:
        cnx = mysql.connector.connect(**config)
        cursor = cnx.cursor()

        # Insert data sources (if not already present)
        for source in llm_data.get("Data_Sources", []):
            insert_source_query = """
            INSERT INTO Data_Sources (name, website)
            VALUES (%s, %s) AS new
            ON DUPLICATE KEY UPDATE name=new.name, website=new.website
            """
            cursor.execute(insert_source_query, (source['name'], source['website']))

        # We commit here to ensure we have source_id(s) available
        cnx.commit()

        # Get a map from source_name -> source_id for faster lookups
        cursor.execute("SELECT source_id, name FROM Data_Sources")
        source_map = {row[1]: row[0] for row in cursor.fetchall()}

        # Insert countries
        for country in llm_data.get("Countries", []):
            insert_country_query = """
                INSERT INTO Countries (country_name, country_code)
                VALUES (%s, %s)
            """
            cursor.execute(insert_country_query, (country["country_name"], country["country_code"]))
        
        cnx.commit()

        # Get a map from country_name -> country_id
        cursor.execute("SELECT country_id, country_name FROM Countries")
        country_map = {row[1]: row[0] for row in cursor.fetchall()}

        # Insert Birth_Rate data
        for br in llm_data.get("Birth_Rate", []):
            insert_br_query = """
                INSERT INTO Birth_Rate (country_id, source_id, year, birth_rate)
                VALUES (%s, %s, %s, %s)
            """
            cid = country_map.get(br["country_name"])
            sid = source_map.get(br["source_name"])
            if cid and sid:
                cursor.execute(insert_br_query, (cid, sid, br["year"], br["birth_rate"]))

        # Insert Death_Rate data
        for dr in llm_data.get("Death_Rate", []):
            insert_dr_query = """
                INSERT INTO Death_Rate (country_id, source_id, year, death_rate)
                VALUES (%s, %s, %s, %s)
            """
            cid = country_map.get(dr["country_name"])
            sid = source_map.get(dr["source_name"])
            if cid and sid:
                cursor.execute(insert_dr_query, (cid, sid, dr["year"], dr["death_rate"]))

        # Insert Population data
        for pop in llm_data.get("Population", []):
            insert_pop_query = """
                INSERT INTO Population (country_id, source_id, year, population)
                VALUES (%s, %s, %s, %s)
            """
            cid = country_map.get(pop["country_name"])
            sid = source_map.get(pop["source_name"])
            if cid and sid:
                cursor.execute(insert_pop_query, (cid, sid, pop["year"], pop["population"]))

        cnx.commit()

        print("LLM data inserted successfully!")
    except mysql.connector.Error as err:
        print(f"Database error: {err}")
    finally:
        cursor.close()
        cnx.close()


# Example usage:
# Suppose we have fetched some context and a query:
# context = ["Lebanon is a country in the Middle East...", "It has a population of X..."]
# query = "Provide birth rate, death rate, and population data in the specified JSON structure."
# llm_data = ask_llm(context, query)

# insert_llm_data_into_db(llm_data)







####################
# 1. CRAWL THE PAGE
####################
def fetch_page_text(url):
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to retrieve {url}")
    html = response.text
    soup = BeautifulSoup(html, "html.parser")
    # Extract text. This can be improved depending on page structure
    # Here we assume <p> tags contain the main text
    paragraphs = [p.get_text().strip() for p in soup.find_all('p')]
    # Filter out empty paragraphs
    paragraphs = [p for p in paragraphs if p]
    return paragraphs

####################
# 2. EMBEDDING UTILS
####################
def get_embedding(text, model="text-embedding-3-large"):
    # Make sure text is below the model's token limit for embeddings
    response = openai.Embedding.create(
        model="text-embedding-ada-002",
        input=[text]
    )
    embedding = response['data'][0]['embedding']
    return embedding

def cosine_similarity(a, b):
    # a and b are numpy arrays
    return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))

####################
# 3. BUILD THE INDEX
####################
def build_index(paragraphs):
    # Compute embeddings for all paragraphs
    embeddings = []
    for p in paragraphs:
        emb = get_embedding(p)
        embeddings.append(emb)
    return embeddings

####################
# 4. QUERY THE INDEX
####################
def query_index(query, paragraphs, embeddings, top_k=10):
    query_emb = get_embedding(query)
    # Compute similarity with each paragraph embedding
    sims = [cosine_similarity(query_emb, np.array(e)) for e in embeddings]
    
    # Get top-k paragraphs
    top_indices = np.argsort(sims)[-top_k:][::-1]
    top_paragraphs = [paragraphs[i] for i in top_indices]
    return top_paragraphs

####################
# 5. ASK THE LLM
####################
def ask_llm(context, query, model="gpt-4"):
    # Combine all paragraphs into a single context
    context_str = "\n\n".join(context)
    # Instruct the LLM to produce JSON that fits your database schema.
    prompt = (
        f"Given the following text:\n\n{context_str}\n\n"
        f"Please answer the following question:\n{query}\n\n"
        f"Return your answer in the following JSON structure that corresponds to the given database schema:\n\n"
        f"Schema:\n"
        f"{{\n"
        f"  \"Countries\": [\n"
        f"    {{\"country_name\": \"string\", \"country_code\": \"string\"}}\n"
        f"  ],\n"
        f"  \"Data_Sources\": [\n"
        f"    {{\"name\": \"string\", \"website\": \"string\"}}\n"
        f"  ],\n"
        f"  \"Birth_Rate\": [\n"
        f"    {{\"country_name\": \"string\", \"source_name\": \"string\", \"year\": \"integer\", \"birth_rate\": \"float\"}}\n"
        f"  ],\n"
        f"  \"Death_Rate\": [\n"
        f"    {{\"country_name\": \"string\", \"source_name\": \"string\", \"year\": \"integer\", \"death_rate\": \"float\"}}\n"
        f"  ],\n"
        f"  \"Population\": [\n"
        f"    {{\"country_name\": \"string\", \"source_name\": \"string\", \"year\": \"integer\", \"population\": \"integer\"}}\n"
        f"  ]\n"
        f"}}\n\n"
        f"Make sure your response is valid JSON. No additional text outside the JSON structure.\n\n"
        f"Answer:"
    )

    response = openai.ChatCompletion.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )
    
    json_str = response.choices[0].message.content.strip()
    
    # Attempt to parse the LLM response as JSON.
    try:
        data = json.loads(json_str)
    except json.JSONDecodeError:
        data = {
            "error": "Invalid JSON returned by the model.",
            "raw_response": json_str
        }
    
    return data

# Example usage (after fetching and embedding paragraphs, etc.):
# context = ["This is some text about a country...", "Another paragraph..."]
# query = "What is the population of Lebanon in 2020 based on the given schema?"
# result = ask_llm(context, query)
# print(result)  # This should print a dictionary that matches the JSON structure.


####################
# MAIN EXECUTION
####################
if __name__ == "__main__":
    # Example usage
    url = "https://edition.cnn.com/2024/08/20/health/us-birth-rate-fertility-final-data-2023/index.html" #link of the tailored page to scrape
    paragraphs = fetch_page_text(url)
    
    # Build embeddings index (You might want to cache this)
    embeddings = build_index(paragraphs)
    
    # Example user query
    gpt_query = "What are the demographic data of the mentioned country in the text, return it without exmplanation, with the relevant year, if avaiable" #query to be compared and input to gpt's api
    top_context = query_index(gpt_query, paragraphs, embeddings, top_k=10)
        
    answer = ask_llm(top_context, gpt_query)
    insert_llm_data_into_db(answer)
    print("Answer:", answer)
