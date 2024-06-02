# 0.83
set.seed(123)
indices <- sample(1:nrow(client_features), 0.7 * nrow(client_features))
train_set <- client_features[indices, ]
test_set <- client_features[-indices, ]
str(train_set)
model <- rpart(vehicle_category ~ .,
    data = train_set, method = "class",
    control = rpart.control(cp = 0.005, minsplit = 20, minbucket = 10, maxdepth = 5, xval = 10)
)
