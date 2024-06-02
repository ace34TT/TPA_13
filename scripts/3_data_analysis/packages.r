options(repos = c(CRAN = "https://cloud.r-project.org"))

required_packages <- c("RJDBC", "rJava", "ggplot2", "ggfortify", "RColorBrewer", "dplyr", "rpart", "rpart.plot", "ROracle")

for (package in required_packages) {
    if (!requireNamespace(package, quietly = TRUE)) {
        install.packages(package)
    }
}
