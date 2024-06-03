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

hive_jdbc_jar <- "/home/aceky/Downloads/jars/hive-jdbc-3.1.3-standalone.jar"
# hive_jdbc_jar <- "/usr/local/apache-hive-3.1.3-bin/jdbc/hive-jdbc-3.1.3-standalone.jar"
hive_driver <- "org.apache.hive.jdbc.HiveDriver"
hive_url <- "jdbc:hive2://localhost:10000/tpa_13"
drv <- JDBC(hive_driver, hive_jdbc_jar, "`")
conn <- dbConnect(drv, hive_url, "vagrant", "")

query <- "SELECT * FROM v_clients"
result <- dbGetQuery(conn, query)
clients_imma_df <- as.data.frame(result)
colnames(clients_imma_df) <- sub("^v_clients\\.", "", colnames(clients_imma_df))

zero_var_cols <- colnames(clients_imma_df)[apply(clients_imma_df, 2, var) == 0]

clients_imma_df_filtered <- clients_imma_df[, !(names(clients_imma_df) %in% zero_var_cols)]

selected_variables <- c(
    "immatriculation_puissance",
    "immatriculation_nbportes",
    "immatriculation_prix",
    "clients_age",
    "clients_sexe",
    "clients_situationfamiliale",
    "clients_nbenfantsacharge",
    "clients_deuxiemevoiture"
)

data_for_clustering <- clients_imma_df_filtered[selected_variables]

clients_imma_df_scaled <- scale(data_for_clustering[, sapply(data_for_clustering, is.numeric)])

wss <- (nrow(clients_imma_df_scaled) - 1) * sum(apply(clients_imma_df_scaled, 2, var))
for (i in 2:15) wss[i] <- sum(kmeans(clients_imma_df_scaled, centers = i)$withinss)
plot(1:15, wss, type = "b", xlab = "Nombre de clusters", ylab = "Somme des carrés")

set.seed(123)
kmeans_result <- kmeans(clients_imma_df_scaled, centers = 5)
clients_imma_df$cluster <- kmeans_result$cluster

aggregate(clients_imma_df_scaled, by = list(cluster = kmeans_result$cluster), FUN = mean)

pca_result <- prcomp(clients_imma_df_scaled)
ggplot(clients_imma_df, aes(x = pca_result$x[, 1], y = pca_result$x[, 2], color = factor(kmeans_result$cluster))) +
    geom_point() +
    theme_minimal() +
    labs(x = "PC1", y = "PC2", color = "Cluster")

mean_puissance_nbportes <- clients_imma_df %>%
    group_by(cluster) %>%
    summarise(
        mean_puissance = mean(immatriculation_puissance),
        mean_nbportes = mean(immatriculation_nbportes),
        mean_nbplaces = mean(immatriculation_nbplaces),
        mean_prix = mean(immatriculation_prix)
    )

dominant_situation_familiale <- clients_imma_df %>%
    group_by(cluster, clients_situationfamiliale) %>%
    summarise(count = n()) %>%
    top_n(1, count) %>%
    select(cluster, clients_situationfamiliale)

vehicle_categories <- c("Citadine", "Berline", "Citadine", "SUV", "Sportive")

clients_imma_df$vehicle_category <- vehicle_categories[clients_imma_df$cluster]

selected_columns <- clients_imma_df[, c("immatriculation_marque", "immatriculation_nom", "vehicle_category")]

ggplot(clients_imma_df, aes(x = factor(cluster), fill = vehicle_category)) +
    geom_bar() +
    theme_minimal() +
    labs(x = "Cluster", y = "Nombre de clients", fill = "Catégorie de véhicules") +
    theme(axis.text.x = element_text(angle = 45, hjust = 1))

query <- "SELECT * FROM clients_int"
result <- dbGetQuery(conn, query)
clients_df <- as.data.frame(result)
colnames(clients_df) <- sub("^clients_int\\.", "", colnames(clients_df))

clients_df_ <- merge(clients_df, clients_imma_df[, c("clients_immatriculation", "vehicle_category")], by.x = "immatriculation", by.y = "clients_immatriculation", all.x = TRUE)

str(clients_df_)

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
print(model)
importance(model)
str(train_set)
single_tree <- getTree(model, k = 1, labelVar = TRUE)

single_tree_rpart <- rpart(
    vehicle_category ~ .,
    data = train_set,
    method = "class",
    control = rpart.control(cp = 0.001)
)

rpart.plot(single_tree_rpart)
# end model

str(test_set)
predictions <- predict(model, newdata = test_set, type = "class")

confusion_matrix <- table(Predicted = predictions, Actual = test_set$vehicle_category)
accuracy <- sum(diag(confusion_matrix)) / sum(confusion_matrix)
print(accuracy)

saveRDS(model, file = "/home/aceky/Study/cours/big-data/INSTALL_MV_BIGDATA_BOX/TPA_13/scripts/3_data_analysis/models/categorie_model.rds")
# saveRDS(model, file = "/vagrant/TPA_13/3_data_analysis/models/categorie_model.rds")

dbDisconnect(conn)
