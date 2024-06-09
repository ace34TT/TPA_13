library(RJDBC)
library(rJava)
library(ggplot2)
library(ggfortify)
library(RColorBrewer)
library(dplyr)
library(rpart)
library(rpart.plot)
library(randomForest)
# library(e1071)
# library(class)
# library(dplyr)

# hive_jdbc_jar <- "/home/aceky/Downloads/jars/hive-jdbc-3.1.3-standalone.jar"
hive_jdbc_jar <- "/usr/local/apache-hive-3.1.3-bin/jdbc/hive-jdbc-3.1.3-standalone.jar"

# Established connection to hive
hive_driver <- "org.apache.hive.jdbc.HiveDriver"
hive_url <- "jdbc:hive2://localhost:10000/tpa_13"
drv <- JDBC(hive_driver, hive_jdbc_jar, "`")
conn <- dbConnect(drv, hive_url, "vagrant", "")

# querying data from v_clients
query <- "SELECT * FROM v_clients"
result <- dbGetQuery(conn, query)
clients_imma_df <- as.data.frame(result)

# rename properly db headers
colnames(clients_imma_df) <- sub("^v_clients\\.", "", colnames(clients_imma_df))

# remove columns with 0 variances (those are not used to create the cluster)
zero_var_cols <- colnames(clients_imma_df)[apply(clients_imma_df, 2, var) == 0]
clients_imma_df_filtered <- clients_imma_df[, !(names(clients_imma_df) %in% zero_var_cols)]

# we select the columns names used to create the cluster
selected_variables <- c(
    "immatriculation_puissance",
    "immatriculation_nbportes",
    "immatriculation_prix",
    "clients_age",
    "clients_nbenfantsacharge"
)

# set.seed(123)
# create a new dataframe from the selected variables
data_for_clustering <- clients_imma_df_filtered[selected_variables]

# we scale the all numeric columns of the dataframe
clients_imma_df_scaled <- scale(data_for_clustering[, sapply(data_for_clustering, is.numeric)])

# we calculate the the wss of the dataframe (sommes des carrées intra-groupe)
# WSS within square sum , variance au sein du groupe de donnees .
set.seed(123)
wss <- (nrow(clients_imma_df_scaled) - 1) * sum(apply(clients_imma_df_scaled, 2, var))
for (i in 2:15) {
    set.seed(123)
    wss[i] <- sum(kmeans(clients_imma_df_scaled, centers = i)$withinss)
}
# We plot the result to determinate the optimal number of cluster
plot(1:15, wss, type = "b", xlab = "Nombre de clusters", ylab = "Somme des carrés")

# We assign the cluster label to our dataframe
set.seed(123)
kmeans_result <- kmeans(clients_imma_df_scaled, centers = 4)
clients_imma_df$cluster <- kmeans_result$cluster

# We display the cluster repartition
# aggregate(clients_imma_df_scaled, by = list(cluster = kmeans_result$cluster), FUN = mean)
pca_result <- prcomp(clients_imma_df_scaled)
ggplot(clients_imma_df, aes(x = pca_result$x[, 1], y = pca_result$x[, 2], color = factor(kmeans_result$cluster))) +
    geom_point() +
    theme_minimal() +
    labs(x = "PC1", y = "PC2", color = "Cluster")

# We take the mean value of each numeric features in order to determinate the
cluster_specs <- clients_imma_df %>%
    group_by(cluster) %>%
    summarise(
        mean_puissance = mean(immatriculation_puissance, na.rm = TRUE),
        mean_nbportes = mean(immatriculation_nbportes, na.rm = TRUE),
        mean_nbplaces = mean(immatriculation_nbplaces, na.rm = TRUE),
        mean_nbenfants = mean(clients_nbenfantsacharge, na.rm = TRUE),
        mean_prix = mean(immatriculation_prix, na.rm = TRUE),
        dominant_situation_familiale = clients_situationfamiliale[which.max(table(clients_situationfamiliale))],
        dominant_longueur = immatriculation_longueur[which.max(table(immatriculation_longueur))]
    )

cluster_specs

vehicle_categories <- c("Familiale", "Citadine", "Citadine", "Berline de Luxe")

clients_imma_df$vehicle_category <- vehicle_categories[clients_imma_df$cluster]

query <- "SELECT * FROM clients_int"
result <- dbGetQuery(conn, query)
clients_df <- as.data.frame(result)
colnames(clients_df) <- sub("^clients_int\\.", "", colnames(clients_df))

clients_df_ <- merge(clients_df, clients_imma_df[, c("clients_immatriculation", "vehicle_category")], by.x = "immatriculation", by.y = "clients_immatriculation", all.x = TRUE)

client_features <- clients_df_[, !(names(clients_df_) %in% c("immatriculation"))]

# start model
client_features$vehicle_category <- as.factor(client_features$vehicle_category)
client_features$sexe <- as.factor(client_features$sexe)
client_features$situationfamiliale <- as.factor(client_features$situationfamiliale)
client_features$deuxiemevoiture <- as.factor(client_features$deuxiemevoiture)

set.seed(123)
indices <- sample(1:nrow(client_features), 0.7 * nrow(client_features))
train_set <- client_features[indices, ]
test_set <- client_features[-indices, ]

model <- randomForest(vehicle_category ~ age + sexe + taux + situationfamiliale + nbenfantsacharge + deuxiemevoiture, data = train_set)
importance(model)

# end model

predictions <- predict(model, newdata = test_set, type = "class")
confusion_matrix <- table(Predicted = predictions, Actual = test_set$vehicle_category)
accuracy <- sum(diag(confusion_matrix)) / sum(confusion_matrix)
print(accuracy)


# saveRDS(model, file = "/home/aceky/Study/cours/big-data/INSTALL_MV_BIGDATA_BOX/tpa_13/scripts/3_data_analysis/models/categorie_model.rds")
saveRDS(model, file = "/vagrant/tpa_13/3_data_analysis/models/categorie_model.rds")

dbDisconnect(conn)
