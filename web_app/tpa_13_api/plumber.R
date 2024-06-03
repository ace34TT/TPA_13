#
# This is a Plumber API. In RStudio 1.2 or newer you can run the API by
# clicking the 'Run API' button above.
#
# In RStudio 1.1 or older, see the Plumber documentation for details
# on running the API.
#
# Find out more about building APIs with Plumber here:
#
#    https://www.rplumber.io/
#

library(plumber)
library(ROracle)
library(RJDBC)
library(jsonlite)
library(data.table)

source("utils/utils.R")

#' @filter cors
cors <- function(req, res) {
  res$setHeader("Access-Control-Allow-Origin", "*")

  if (req$REQUEST_METHOD == "OPTIONS") {
    res$setHeader("Access-Control-Allow-Methods", "*")
    res$setHeader("Access-Control-Allow-Headers", req$HTTP_ACCESS_CONTROL_REQUEST_HEADERS)
    res$status <- 200
    return(list())
  } else {
    plumber::forward()
  }
}

model <- readRDS("/home/aceky/Study/cours/big-data/INSTALL_MV_BIGDATA_BOX/TPA_13/scripts/3_data_analysis/models/categorie_model.rds")



#* @apiTitle Plumber Example API


#* Echo back the input
#* @param msg The message to echo
#* @get /echo
function(msg = "") {
  list(msg = paste0("The message is: '", msg, "'"))
}

#* Plot a histogram
#* @serializer png
#* @get /plot
function() {
  rand <- rnorm(100)
  hist(rand)
}

#* Return the sum of two numbers
#* @param a The first number to add
#* @param b The second number to add
#* @post /sum
function(a, b) {
  as.numeric(a) + as.numeric(b)
}

#* @get /get_marketing
function() {
  drv <- dbDriver("Oracle")
  con <- dbConnect(drv, username = "MBDS", password = "PassMbds", dbname = "//localhost:1521/ORCLPDB1")
  data <- dbGetQuery(con, "SELECT * FROM marketing_result ORDER BY id DESC")
  dbDisconnect(con)
  return(data)
}

#* @post /predict_marketing
function(req) {
  client_data <- data.frame(
    age = req$body$age,
    sexe = req$body$sexe,
    taux = req$body$taux,
    situationfamiliale = req$body$situationfam,
    nbenfantsacharge = req$body$nbenfantsacharge,
    deuxiemevoiture = req$body$deuxiemevo
  )
  client_data <- convertDFForPrediction(client_data)
  single_prediction <- predict(model, client_data, type = "class")
  client_data <- convertPredictionToDBSchema(client_data, single_prediction)

  drv <- dbDriver("Oracle")
  con <- dbConnect(drv, username = "MBDS", password = "PassMbds", dbname = "//localhost:1521/ORCLPDB1")
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
  for (i in 1:nrow(client_data)) {
    insert_row(client_data[i, ])
  }
  dbDisconnect(con)
  return(client_data)
}
