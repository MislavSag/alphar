library(mlr3)


# train/test split
train_set <- sample(task_iris$nrow, 0.8 * task_iris$nrow)
test_set <- setdiff(seq_len(task_iris$nrow), train_set)

# train the model
learner$train(task_iris, row_ids = train_set)

# predict data
prediction <- learner$predict(task_iris, row_ids = test_set)

# calculate performance
prediction$confusion



measure <- msr("classif.acc")
prediction$score(measure)
