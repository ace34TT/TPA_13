from pyhive import hive

# Establish connection
conn = hive.Connection(
    host="localhost", port=10000, username="vagrant", database="tpa_13"
)

# Create a cursor
cursor = conn.cursor()

# Execute the DROP TABLE statement for IMMATRICULATION_EXT
print("DROP TABLE IF EXISTS IMMATRICULATION_EXT")
cursor.execute("DROP TABLE IF EXISTS IMMATRICULATION_EXT")
# Execute the CREATE TABLE statement for IMMATRICULATION_EXT
print("CREATE TABLE IF EXISTS IMMATRICULATION_EXT")
create_table_immatriculation_query = """
CREATE EXTERNAL TABLE IMMATRICULATION_EXT (
    immatriculation STRING,
    marque STRING,
    nom STRING,
    puissance INT,
    longueur STRING,
    nbPlaces INT,
    nbPortes INT,
    couleur STRING,
    occasion STRING,
    prix INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'tpa_13/immatriculation'
"""

cursor.execute(create_table_immatriculation_query)

# Execute the DROP TABLE statement for MARKETING_EXT
print("DROP TABLE IF EXISTS MARKETING_EXT")
cursor.execute("DROP TABLE IF EXISTS MARKETING_EXT")

# Execute the CREATE TABLE statement for MARKETING_EXT
print("CREATE TABLE MARKETING_EXT")
create_table_marketing_query = """
CREATE EXTERNAL TABLE MARKETING_EXT (
    id INT,
    age INT,
    sexe STRING,
    taux INT,
    situationFamiliale STRING,
    nbEnfantsAcharge INT,
    deuxiemeVoiture BOOLEAN
)
STORED BY 'oracle.kv.hadoop.hive.table.TableStorageHandler'
TBLPROPERTIES (
    "oracle.kv.kvstore" = "kvstore",
    "oracle.kv.hosts" = "localhost:5000",
    "oracle.kv.hadoop.hosts" = "localhost/127.0.0.1",
    "oracle.kv.tableName" = "marketing"
)
"""

cursor.execute(create_table_marketing_query)

# Execute the DROP TABLE statement for CATALOGUE_EXT
print("DROP TABLE IF EXISTS CATALOGUE_EXT")
cursor.execute("DROP TABLE IF EXISTS CATALOGUE_EXT")

# Execute the CREATE TABLE statement for CATALOGUE_EXT
print("CREATE TABLE CATALOGUE_EXT")
create_table_catalogue_query = """
CREATE EXTERNAL TABLE CATALOGUE_EXT (
    marque STRING,
    nom STRING,
    puissance STRING,
    longueur STRING,
    nbPlaces INT,
    nbPortes INT,
    couleur STRING,
    occasion STRING,
    prix INT,
    bonusMalus FLOAT,
    rejetsCO2 FLOAT,
    coutEnergie FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'tpa_13/transformed_catalog/'
tblproperties("skip.header.line.count"="1")
"""

cursor.execute(create_table_catalogue_query)

print("DELETING VIEW IF EXISTS")
cursor.execute("DROP VIEW IF EXISTS v_clients")
print("CREATING VIEW V_CLIENTS")
create_view_clients_imma_query = """
CREATE VIEW IF NOT EXISTS v_clients AS
SELECT 
    immatriculation_ext.immatriculation AS immatriculation_immatriculation,
    immatriculation_ext.marque AS immatriculation_marque,
    immatriculation_ext.nom AS immatriculation_nom,
    immatriculation_ext.puissance AS immatriculation_puissance,
    immatriculation_ext.longueur AS immatriculation_longueur,
    immatriculation_ext.nbplaces AS immatriculation_nbplaces,
    immatriculation_ext.nbportes AS immatriculation_nbportes,
    immatriculation_ext.couleur AS immatriculation_couleur,
    immatriculation_ext.occasion AS immatriculation_occasion,
    immatriculation_ext.prix AS immatriculation_prix,
    clients_int.age AS clients_age,
    clients_int.sexe AS clients_sexe,
    clients_int.taux AS clients_taux,
    clients_int.situationfamiliale AS clients_situationfamiliale,
    clients_int.nbenfantsacharge AS clients_nbenfantsacharge,
    clients_int.deuxiemevoiture AS clients_deuxiemevoiture,
    clients_int.immatriculation AS clients_immatriculation
FROM immatriculation_ext
JOIN clients_int ON immatriculation_ext.immatriculation = clients_int.immatriculation
"""
cursor.execute(create_view_clients_imma_query)

# Commit changes
conn.commit()

# Close cursor and connection
cursor.close()
conn.close()

print("PROCESS DONE !")
