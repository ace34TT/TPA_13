# 0.82
set.seed(123)
indices <- sample(1:nrow(client_features), 0.7 * nrow(client_features))
train_set <- client_features[indices, ]
test_set <- client_features[-indices, ]
model <- svm(vehicle_category ~ age + sexe + taux + situationfamiliale + nbenfantsacharge, data = train_set, kernel = "radial")
summary(model)
predictions <- predict(model, test_set)
