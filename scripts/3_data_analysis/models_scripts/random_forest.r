# 0.83
client_features$vehicle_category <- as.factor(client_features$vehicle_category)
client_features$sexe <- as.factor(client_features$sexe)
client_features$situationfamiliale <- as.factor(client_features$situationfamiliale)

set.seed(123)
indices <- sample(1:nrow(client_features), 0.7 * nrow(client_features))
train_set <- client_features[indices, ]
test_set <- client_features[-indices, ]

model <- randomForest(vehicle_category ~ age + sexe + taux + situationfamiliale + nbenfantsacharge, data = train_set)
print(model)
importance(model)

single_tree <- getTree(model, k = 1, labelVar = TRUE)

single_tree_rpart <- rpart(
    vehicle_category ~ .,
    data = train_set,
    method = "class",
    control = rpart.control(cp = 0.001)
)

rpart.plot(single_tree_rpart)
