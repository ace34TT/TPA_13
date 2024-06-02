#!/usr/bin/env python
# coding: utf-8

# In[2]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col , regexp_replace , split ,initcap , upper , avg , trim , lit , coalesce , round , monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, IntegerType , DoubleType


# ## 1 - Creating spark session 

# In[4]:


spark = SparkSession.builder.appName("Catalogue_CO2").getOrCreate()


# ## 2 - Importing data from hdfs 

# In[7]:


schema = StructType([
    StructField("NoLigne", IntegerType(), True),
    StructField("Marque / Modele", StringType(), True),
    StructField("Bonus / Malus", StringType(), True),
    StructField("Rejets CO2 g/km", IntegerType(), True),
    StructField("Cout energie", StringType(), True)
])
co2_df = spark.read.csv("tpa_13/CO2.csv", header=True, schema= schema)


# In[10]:


catalog_df = spark.read.csv("tpa_13/Catalogue.csv", header=True, inferSchema=True)


# ## 3 - Processing CO2

# In[11]:


# fix Bonus / Malus data by removing everything after the euro sign 
co2_df = co2_df.withColumn("Bonus / Malus", regexp_replace("Bonus / Malus", r'€.*', '€'))
# remove all data that have invalide values 
co2_df = co2_df.filter(col("Bonus / Malus") != "-")


# In[12]:


# add create Marque colunm from Marque / Model by taking the first word 
co2_df = co2_df.withColumn("Marque", split(co2_df["Marque / Modele"], " ")[0])
# create Model colunm from Marque / Model col by taking the 
co2_df = co2_df.withColumn("Model", split(co2_df["Marque / Modele"], " ")[1])
# delete the marque / model col as it 
co2_df = co2_df.drop("Marque / Modele")


# In[13]:


# we take all the marques catalogue
marques_catalogue = catalog_df.select("marque").distinct().withColumnRenamed("marque", "Marque")
# we capitalise them in order to be able to compared it with data from co2
marques_catalogue = marques_catalogue.withColumn("Marque", upper(col("Marque")))


# In[15]:


# We filter CO2 to only have data having marques in Catalogues 
co2_df = co2_df.alias("co2").join(marques_catalogue.alias("marques"), "Marque")
co2_df.show()


# In[10]:


# we process Bonus / Malus and Cout energie to be able to perform operations as number
# remove euro , + , white spaces 
processed_co2 = co2_df.withColumn('Bonus / Malus', regexp_replace(col('Bonus / Malus'), '€', '')) \
    .withColumn('Cout energie', regexp_replace(col('Cout energie'), '€', '')) \
    .withColumn('Bonus / Malus', regexp_replace(col('Bonus / Malus'), '\\+', '')) \
    .withColumn('Cout energie', regexp_replace(col('Cout energie'), '\u00A0', '')) \
    .withColumn('Bonus / Malus', regexp_replace(col('Bonus / Malus'), '\u00A0', ''))
# we convert Bonus / Malus , Cout energie to float 
processed_co2 = processed_co2.withColumn('Bonus / Malus', processed_co2['Bonus / Malus'].cast('float')) \
    .withColumn('Cout energie', processed_co2['Cout energie'].cast('float'))
processed_co2.show()


# In[16]:


# we process Bonus / Malus and Cout energie to be able to perform operations as number
# remove euro , + , white spaces 
processed_co2 = co2_df.withColumn('Bonus / Malus', regexp_replace(col('Bonus / Malus'), '€', '')) \
    .withColumn('Cout energie', regexp_replace(col('Cout energie'), '€', '')) \
    .withColumn('Bonus / Malus', regexp_replace(col('Bonus / Malus'), '\\+', '')) \
    .withColumn('Cout energie', regexp_replace(col('Cout energie'), '\u00A0', '')) \
    .withColumn('Bonus / Malus', regexp_replace(col('Bonus / Malus'), '\u00A0', ''))
# we convert Bonus / Malus , Cout energie to float 
processed_co2 = processed_co2.withColumn('Bonus / Malus', processed_co2['Bonus / Malus'].cast('float')) \
    .withColumn('Cout energie', processed_co2['Cout energie'].cast('float'))
processed_co2.show()


# In[17]:


# We take the mean of Bonus / Malus , Regets CO2 , Cout Energie for each brand 
moyennes_par_marque = processed_co2.groupBy('Marque').agg(
    round(avg('Bonus / Malus'), 2).alias('Bonus / Malus'),
    round(avg('Rejets CO2 g/km'), 2).alias('Rejets CO2 g/km'),
    round(avg('Cout Energie'), 2).alias('Cout Energie')
)
moyennes_par_marque.show()


# In[18]:


# we take the mean of Bonus / Malus , Rehet CO2 and Cout Energie of all brand 
moyenne_toutes_marques_df = processed_co2.groupBy().agg(
    round(avg("Bonus / Malus"),2).alias("Moyenne_Bonus_Malus"),
    round(avg("Rejets CO2 g/km"),2).alias("Moyenne_Rejets_CO2"),
    round(avg("Cout Energie"),2).alias("Moyenne_Cout_Energie")
)
moyenne_toutes_marques_df.show()


# In[19]:


# we add column from bonus / malus , rejets co2 and cout energie into catalog
# we leave values null for brandes that are not in co2 
df_resultat = catalog_df.alias("catalogue").join(
    moyennes_par_marque.withColumnRenamed("Marque", "Marque_moyenne").select("Marque_moyenne", "Bonus / Malus", "Rejets CO2 g/km", "Cout Energie"),
    (upper(col("catalogue.marque")) == col("Marque_moyenne")),
    "left_outer"
).drop("Marque_moyenne")
df_resultat.show()


# In[20]:


moyenne_values = moyenne_toutes_marques_df.first()
# we set the values missing brand in co2 to the mean of all brands
updated_catalog_df = df_resultat.withColumn("Bonus / Malus", coalesce(df_resultat["Bonus / Malus"], lit(moyenne_values["Moyenne_Bonus_Malus"]))) \
    .withColumn("Rejets CO2 g/km", coalesce(df_resultat["Rejets CO2 g/km"], lit(moyenne_values["Moyenne_Rejets_CO2"]))) \
    .withColumn("Cout Energie", coalesce(df_resultat["Cout Energie"], lit(moyenne_values["Moyenne_Cout_Energie"])))
updated_catalog_df.show()


# In[21]:


updated_catalog_df.repartition(1).write.csv("tpa_13/transformed_catalog" , mode="overwrite" , header=True)


# In[22]:


spark.stop()


# In[ ]:




