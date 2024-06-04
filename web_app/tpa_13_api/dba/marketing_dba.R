source("dba/db_connection.R")
source("utils/df.utils.R")
library(randomForest)

get_marketings <- function() {

}

model <- readRDS("/home/aceky/Study/cours/big-data/INSTALL_MV_BIGDATA_BOX/tpa_13/scripts/3_data_analysis/models/categorie_model.rds")
client_data <- data.frame(
    age = 24,
    sexe = "F",
    taux = 24,
    situationfamiliale = "En Couple",
    nbenfantsacharge = 0,
    deuxiemevoiture = "true"
)

client_data <- convertDFForPrediction(client_data)
str(client_data)

single_prediction <- predict(model, client_data, type = "class")
single_prediction
