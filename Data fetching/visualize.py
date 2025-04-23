import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt

# MySQL config
config = {
    'user': 'root',
    'password': 'khalil_13579',
    'host': '127.0.0.1',
    'database': 'fyp',
    'raise_on_warnings': True
}

cnx = mysql.connector.connect(**config)
query = """
    SELECT p.year, SUM(p.population) AS population, ds.name AS source_name
    FROM Population p
    JOIN Data_Sources ds ON p.source_id = ds.source_id
    GROUP BY p.year, ds.name
    ORDER BY p.year ASC
"""

df = pd.read_sql(query, cnx)
cnx.close()

# Plotting
plt.figure(figsize=(14, 7))
for source_name, group in df.groupby("source_name"):
    group_sorted = group.sort_values(by="year")
    plt.plot(group_sorted["year"], group_sorted["population"], marker='o', linestyle='-', label=source_name)

plt.title("Global Population Over Time by Data Source", fontsize=14)
plt.xlabel("Year")
plt.ylabel("Population")
plt.legend(title="Data Source")
plt.grid(True)
plt.tight_layout()
plt.show()
