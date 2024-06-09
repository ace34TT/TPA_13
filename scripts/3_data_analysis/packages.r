options(repos = c(CRAN = "https://cloud.r-project.org"))


library(randomForest)


required_packages <- c("randomForest","class" , "e1071","plumber", "RJDBC", "rJava", "ggplot2", "ggfortify", "RColorBrewer", "dplyr", "rpart", "rpart.plot", "ROracle")

for (package in required_packages) {
    if (!requireNamespace(package, quietly = TRUE)) {
        install.packages(package)
    }
}
