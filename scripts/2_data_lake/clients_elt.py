#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pymongo
import pandas as pd
from pyhive import hive
from urllib.parse import quote
import urllib.parse


# In[2]:


client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["tpa_13"]
collection = db["clients"]

cursor = collection.find({})


# In[3]:


df = pd.DataFrame(list(cursor))


# In[4]:


client.close()


# In[5]:


# remove all lines with missing values
print(df.shape[0])
df.replace("", pd.NA, inplace=True)
df.dropna(inplace=True)
print(df.shape[0])


# In[6]:


df["age"] = pd.to_numeric(df["age"], errors="coerce")
df["taux"] = pd.to_numeric(df["taux"], errors="coerce")
df["nbEnfantsAcharge"] = pd.to_numeric(df["nbEnfantsAcharge"], errors="coerce")
df = df[(df["age"] >= 0) & (df["taux"] >= 0) & (df["nbEnfantsAcharge"] >= 0)]
print(df.shape[0])


# In[7]:


gender_distinct_values = df["sexe"].unique()
print(gender_distinct_values)


# In[8]:


gender_counts = df["sexe"].value_counts()
print(gender_counts)


# In[9]:


filtered_df = df[~df["sexe"].isin(["N/D", "?"]) & (df["sexe"] != "")]
print(filtered_df)
print(filtered_df.shape[0])


# In[10]:


distinct_values = filtered_df["sexe"].unique()
print(distinct_values)


# In[11]:


mapping_gender = {
    "F": "F",
    "M": "H",
    "Homme": "H",
    "Femme": "F",
    "Masculin": "H",
    "Féminin": "F",
}
filtered_df.loc[:, "sexe"] = filtered_df["sexe"].map(mapping_gender)
print(filtered_df)


# In[12]:


print(filtered_df.shape[0])


# In[13]:


ms_counts = filtered_df["situationFamiliale"].value_counts()
print(ms_counts)


# In[14]:


marital_status_distinct_values = filtered_df["situationFamiliale"].unique()
print(marital_status_distinct_values)


# In[15]:


filtered_df_ = filtered_df[
    ~filtered_df["situationFamiliale"].isin(["N/D", "?"])
    & (filtered_df["situationFamiliale"] != "")
]


# In[16]:


ms_counts = filtered_df_["situationFamiliale"].value_counts()
print(ms_counts)


# In[17]:


filtered_df_copy = filtered_df_.copy()
r_nb_enfants = filtered_df_copy.groupby("situationFamiliale").agg(
    {"nbEnfantsAcharge": "sum", "situationFamiliale": "count"}
)
r_nb_enfants.rename(
    columns={
        "nbEnfantsAcharge": "sum_nbEnfantsAcharge",
        "situationFamiliale": "count_situationFamiliale",
    },
    inplace=True,
)
print(r_nb_enfants)


# In[18]:


seul_df = filtered_df_copy[filtered_df_copy["situationFamiliale"] == "Seule"]
sum_distinct_nb_enfants_seul = seul_df["nbEnfantsAcharge"].value_counts()
print(sum_distinct_nb_enfants_seul)


# In[19]:


mapping_marital_status = {
    "En Couple": "En Couple",
    "Célibataire": "Célibataire",
    "Seule": "Seul(e)",
    "Marié(e)": "En Couple",
    "Seul": "Seul(e)",
    "Divorcée": "Divorcé(e)",
}
filtered_df__ = filtered_df_
filtered_df__.loc[:, "situationFamiliale"] = filtered_df__["situationFamiliale"].map(
    mapping_marital_status
)
print(filtered_df__)


# In[20]:


d = filtered_df__["situationFamiliale"].unique()
print(d)


# In[21]:


_2nd_car_counts = filtered_df__["2eme voiture"].value_counts()
print(_2nd_car_counts)


# In[22]:


filtered_df___ = filtered_df__[
    ~filtered_df__["2eme voiture"].isin(["?"]) & (filtered_df__["2eme voiture"] != "")
]


# In[23]:


_2nd_car_counts = filtered_df___["2eme voiture"].value_counts()
print(_2nd_car_counts)


# In[25]:


# Establish a connection to Hive
conn = hive.Connection(
    host="localhost", port=10000, username="vagrant", database="tpa_13"
)

# Create a cursor object
cursor = conn.cursor()


# In[31]:


drop_table_sql = "DROP TABLE IF EXISTS clients_int"

create_table_sql = """
CREATE TABLE IF NOT EXISTS clients_int (
    age INT,
    sexe STRING,
    taux DOUBLE,
    situationFamiliale STRING,
    nbEnfantsAcharge INT,
    deuxiemeVoiture STRING,
    immatriculation STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1")
"""

# insert_query = f"INSERT INTO clients (age, sexe, taux, situationFamiliale, nbEnfantsAcharge, deuxiemeVoiture, immatriculation) VALUES (?, ?, ?, ?, ?, ?, ?)"


# In[32]:


# Execute the drop table command
cursor.execute(drop_table_sql)

# Execute the create table command
cursor.execute(create_table_sql)


# In[33]:


if "_id" in filtered_df___.columns:
    filtered_df___ = filtered_df___.drop("_id", axis=1)
if "2eme voiture" in filtered_df___.columns:
    filtered_df___.rename(columns={"2eme voiture": "deuxiemeVoiture"}, inplace=True)
print(filtered_df___.dtypes)


# In[34]:


filtered_df___.to_csv(
    "/vagrant/tpa_13/data/processed_clients.csv", index=False, mode="w"
)


# In[36]:


load_data_query = """
LOAD DATA LOCAL INPATH '/vagrant/tpa_13/data/processed_clients.csv' 
INTO TABLE clients_int
"""

# Execute the command
cursor.execute(load_data_query)


# In[37]:


# batch_size = 500

# for i in range(0, len(filtered_df___), batch_size):
#    batch_df = filtered_df___.iloc[i:i+batch_size]
#    values = ','.join(['({})'.format(','.join(["'{}'".format(str(val)) for val in row])) for row in batch_df.values])
#    cursor.execute("""
#        INSERT INTO clients (age, sexe, taux, situationFamiliale, nbEnfantsAcharge, deuxiemeVoiture, immatriculation)
#        VALUES {}
#    """.format(values))
#    print(i)


# In[38]:


# Commit the transaction
conn.commit()

# Close cursor and connection
cursor.close()
conn.close()
print("Data successfully imported in HIVE")
