library(ROracle)
library(RJDBC)

drv <- dbDriver("Oracle")
con <- dbConnect(drv, username = "MBDS", password = "PassMbds", dbname = "//localhost:1521/ORCLPDB1")

on.exit(dbDisconnect(con), add = TRUE)
