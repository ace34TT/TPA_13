library(ROracle)

hive_jdbc_jar <- "/home/aceky/Downloads/jars/hive-jdbc-3.1.3-standalone.jar"
model <- readRDS("/home/aceky/Study/cours/big-data/INSTALL_MV_BIGDATA_BOX/TPA_13/scripts/3_data_analysis/models/categorie_model.rds")

# model <- readRDS("/vagrant/TPA_13/scripts/3_data_analysis/models/categorie_model.rds")
# hive_jdbc_jar <- "/usr/local/apache-hive-3.1.3-bin/jdbc/hive-jdbc-3.1.3-standalone.jar"

hive_driver <- "org.apache.hive.jdbc.HiveDriver"
hive_url <- "jdbc:hive2://localhost:10000/tpa_13"
hive_drv <- JDBC(hive_driver, hive_jdbc_jar, "`")
hive_conn <- dbConnect(hive_drv, hive_url, "vagrant", "")

query <- "SELECT * FROM marketing_ext"
result <- dbGetQuery(hive_conn, query)
marketing_df <- as.data.frame(result)
colnames(marketing_df) <- sub("^marketing_ext\\.", "", colnames(marketing_df))
marketing_df$id <- NULL
marketing_df$sexe <- factor(marketing_df$sexe)
marketing_df$situationfamiliale <- factor(marketing_df$situationfamiliale)

marketing_df$sexe <- factor(marketing_df$sexe, levels = c("F", "M"))
levels(marketing_df$sexe)[levels(marketing_df$sexe) == "M"] <- "H"

levels_situationfamiliale <- c("En Couple", "Célibataire", "Seul(e)", "Divorcé(e)")
# Update 'situationfamiliale' in marketing_df to include all levels
marketing_df$situationfamiliale <- factor(marketing_df$situationfamiliale,
    levels = levels_situationfamiliale
)
# Convert 'deuxiemevoiture' to a factor
marketing_df$deuxiemevoiture <- as.factor(marketing_df$deuxiemevoiture)

str(marketing_df)
single_prediction <- predict(model, marketing_df, type = "class")
str(marketing_df)
marketing_df$categorie <- single_prediction
marketing_df$deuxiemevoiture <- as.integer(marketing_df$deuxiemevoiture)
marketing_df$categorie <- as.character(marketing_df$categorie)
marketing_df$situationfamiliale <- as.character(marketing_df$situationfamiliale)
marketing_df$deuxiemevoiture <- as.numeric(marketing_df$deuxiemevoiture) - 1

drv <- dbDriver("Oracle")
con <- dbConnect(drv, username = "MBDS", password = "PassMbds", dbname = "//localhost:1521/ORCLPDB1")



create_table <- "
BEGIN
  IF NOT EXISTS (SELECT * FROM user_tables WHERE table_name = 'marketing_result') THEN
    EXECUTE IMMEDIATE 'CREATE Table marketing_result (
    age INTEGER,
    sexe VARCHAR(5),
    taux INTEGER,
    situationfamiliale VARCHAR(50),
    nbEnfantsacharge INTEGER,
    deuxiemevoiture NUMBER(1) NOT NULL CHECK (deuxiemevoiture IN (0, 1)),
    categorie VARCHAR(50)
)';
  END IF;
END;
"
dbSendQuery(con, "BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE YOUR_TABLE_NAME';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;")
dbSendQuery(con, create_table)

insert_row <- function(row) {
    sql_insert <- paste(
        "INSERT INTO marketing_result (AGE, SEXE, TAUX, SITUATIONFAMILIALE, NBENFANTSACHARGE, DEUXIEMEVOITURE, CATEGORIE)",
        "VALUES",
        "(", row$age, ",", "'", row$sexe, "'", ",", row$taux, ",", "'", row$situationfamiliale, "'", ",",
        row$nbenfantsacharge, ",", row$deuxiemevoiture, ",", "'", row$categorie, "'", ")"
    )
    result <- dbSendQuery(con, sql_insert)
    dbCommit(con)
    dbClearResult(result)
}

for (i in 1:nrow(marketing_df)) {
    insert_row(marketing_df[i, ])
}

print("Process done")
