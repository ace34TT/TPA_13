client_features$vehicle_category <- as.factor(client_features$vehicle_category)
client_features$sexe <- as.factor(client_features$sexe)
client_features$situationfamiliale <- as.factor(client_features$situationfamiliale)
set.seed(123)
indices <- sample(1:nrow(client_features), 0.7 * nrow(client_features))
train_set <- client_features[indices, ]
test_set <- client_features[-indices, ]

model <- gbm(vehicle_category ~ age + sexe + taux + situationfamiliale + nbenfantsacharge, data = train_set, distribution = "multinomial", n.trees = 100, interaction.depth = 3)

summary(model)

test_probabilities <- predict(model, test_set, n.trees = 100, type = "response")

test_predictions <- ifelse(test_probabilities > 0.5, 1, 0)

accuracy <- mean(test_predictions == test_set$vehicle_category_binary)
print(paste("Accuracy:", accuracy))
