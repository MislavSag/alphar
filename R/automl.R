library(AUTOMLPhD)
library(data.table)
library(mlr3automl)
library(mlr3verse)
library(future.apply)
library(paradox)
plan(multicore(workers = 8L))

# import features
features <- get_pead_data(
  save_path = file.path("D:/fundamental_data"), # path where to save all relevant datasets
  feature_from_blob = TRUE,        # download features from blob or calculate new features; if FALSE, it takes a long time
  strategy = "PEAD",               # prediciton after earnings announcements (PEAD) or before (PEA)
  type = "history",                # history data or new data
  filter = "usa",                  # use only usa stocks
  refresh_data_old_days = 300,     # if data older than 100 days scrap new data and save
  delete_missing_revenues = TRUE,  # should missing revenues be deleted from events table
  live_history_days = 7            # how many days in the past; relevant only if type is TRUE
)

# create tasks
holdout_ids <- sample(1:nrow(features), 0.05 * nrow(features))
X_model <- features[-holdout_ids, ]
X_holdout <- features[holdout_ids, ]
task_extreme <- create_task_pead(X_model, bin_type = "extreme")
task_aroundzero <- create_task_pead(X_model, bin_type = "aroundzero")

# caorse filtering
task_extreme <- coarse_filtering(task_extreme, "spearman", 0.01)
task_aroundzero <- coarse_filtering(task_aroundzero, "spearman", 0.01)

# filter features
filtered_features_extreme <- auto_feature_filter(task_extreme, filter.nfeat = 4)
filtered_features_aroundzero <- auto_feature_filter(task_aroundzero, filter.nfeat = 4)

# selected features
oldest_bmr_fi_file <- file.info(list.files("D:/mlfin/mlr3_models", pattern = "fi", full.names = TRUE))
oldest_bmr_fi_file <- rownames(oldest_bmr_fi_file[oldest_bmr_fi_file$ctime == max(oldest_bmr_fi_file$ctime), ])
bmr_fs = auto_feature_selection(task_aroundzero, mlr3autoselector_file = oldest_bmr_fi_file) # mlr3autoselector_file = oldest_bmr_fi_file

# extract important features
important_features <- lapply(bmr_fs$data$learners()$learner, function(x) unlist(x$fselect_result$features))
features_unique <- unique(unlist(important_features[lengths(important_features) == 1]))
important_features <- as.data.table(table(unlist(important_features)))
important_features <- important_features[order(N, decreasing = TRUE), ]
important_features_n <- head(important_features[, V1], 20) # top n features selected through ML
important_features_most_important <- important_features[N == max(N, na.rm = TRUE), V1] # most importnatn feature(s)
features_short <- unique(c(important_features_most_important,
                           intersect(important_features_n, filtered_features_extreme),
                           features_unique))
features_long <- unique(c(important_features_most_important,
                          union(important_features_n, filtered_features_extreme),
                          features_unique))

# define learners
new_params = ParamSet$new(list(
  # ParamInt$new("classif.kknn.k", lower = 1, upper = 5, default = 3, tags = "kknn"),
  # ParamDbl$new("classif.glmnet.alpha", lower = 0, upper = 1, tags = "glmnet"),
  # ParamInt$new("classif.nnet.size", lower = 1, upper = 10, tags = "nnet"),
  # ParamDbl$new("classif.nnet.decay", lower = 0, upper = 0.5, tags = "nnet")
  # ParamDbl$new("classif.C50.CF", lower = 0, upper = 1),
  # ParamInt$new("classif.C50.trials", lower = 1, upper = 40)
  # ParamInt$new("classif.bart.numcut", lower = 1, upper = 500),
  # ParamInt$new("classif.bart.keepevery", lower = 1, upper = 10),
  # ParamInt$new("classif.bart.nskip", lower = 0, upper = 500)
  # ParamInt$new("classif.bart.power", lower = 1, upper = 3),
  # ParamDbl$new("classif.bart.base", lower = 0.5, upper = 0.99),
  # ParamInt$new("classif.bart.k", lower = 1, upper = 3)
  ParamFct$new("classif.gausspr.lernel", levels = c("rbfdot", "polydot", "vaniulladot",
                                                    "tanhdot", "laplacedot", "besseldot",
                                                    "anovadot", "splinedot"))
  ))
my_trafo = function(x, param_set) {
  if ("classif.kknn.k" %in% names(x)) {
    x[["classif.kknn.k"]] = 2^x[["classif.kknn.k"]]
  }
  return(x)
}

l = lrn("classif.AdaBoostM1")
l$param_set


# automl
task_reduced <- task_extreme$clone()
task_reduced <- task_reduced$select(features_short)

bmr_results = AutoML(task_reduced,
                     # learner_list = c("classif.ranger", "classif.xgboost", "classif.liblinear",
                     #                  "classif.kknn", "classif.glmnet", "classif.nnet",
                     #                  "classif.bart"),
                     learner_list = c("classif.gausspr"),
                     additional_params = new_params,
                     # custom_trafo=my_trafo,
                     runtime = Inf)
bmr_results <- AutoML(task_reduced)
train_indices = sample(1:task_reduced$nrow, 4/5*task_reduced$nrow)
bmr_results$train(row_ids = train_indices)

# inspect results
bmr_results$learner$archive$data
bmr_results$tuned_params()
bmr_results$tuned_params()$x_domain

#
predict_indices = setdiff(1:task_reduced$nrow, train_indices)
predictions = bmr_results$predict(row_ids = predict_indices)
predictions$score(msr("classif.acc"))
predictions_dt <- as.data.table(predictions)
prediciotns_almostsure <- predictions_dt[prob.1 > 0.7]
mlr3measures::acc(prediciotns_almostsure$truth, prediciotns_almostsure$response)

# # results by mean
# bmr <- bmr_results$clone(deep = TRUE)
# measures = list(
#   msr("classif.acc", id = "acc_train", predict_sets = 'train'),
#   msr("classif.acc", id = "acc_test", predict_sets = 'test')
#   # msr("classif.auc", id = "auc_train", predict_sets = 'train'),
#   # msr("classif.auc", id = "auc_test", predict_sets = 'test')
# )
#
# bmr$aggregate(measures)
# tab = bmr_results$aggregate(measures)
# ranks = tab[, .(learner_id, rank_train = rank(acc_train), rank_test = rank(acc_test)), by = task_id]
# ranks = ranks[, .(mrank_train = mean(rank_train), mrank_test = mean(rank_test)), by = learner_id]
# ranks[order(mrank_test)]
# autoplot(bmr_results) + theme(axis.text.x = element_text(angle = 45, hjust = 1))
#
# # result for specific model
# choose_model <- 'classif.liblinear.tuned'
# best_submodel <- bmr$score(measures)[learner_id == choose_model, ][acc_test == max(acc_test)]$learner[[1]]
# pred = best_submodel$learner$predict(task_extreme)

# save model
save_path_mlr3models <- "D:/mlfin/mlr3_models"
file_name <- file.path(save_path_mlr3models,
                       paste0(bmr_results$task$id, "-", format.POSIXct(Sys.time(), "%Y%m%d-%H%M%S"), '.rds'))
saveRDS(bmr_results, file = file_name)



# BACKTEST DATA -----------------------------------------------------------

# make predictions
backtest_data <- clf_data[, ..feature_set]
# predictions <- lapply(best_submodel$learner, function(x) {
#   y <- as.data.table(x$learner$predict_newdata(newdata = backtest_data))
#   colnames(y) <- paste(colnames(y), x$id, sep = "_")
#   y
# })
# predictions <- do.call(cbind, predictions)
predictions <- best_submodel$learner$predict_newdata(backtest_data)
predictions <- as.data.table(predictions)
predictions[, grep("row_ids|truth_", colnames(predictions)) := NULL]
cbind(backtest_data)

predictions <- cbind(clf_data[, .(symbol, date)], predictions)
predictions <- unique(predictions)
setorder(predictions, "date")
colnames(predictions) <- gsub(".tuned", "", colnames(predictions))

# create table for backtest
cols <- c("date", "symbol", colnames(predictions)[3:ncol(predictions)])
pead_backtest <- predictions[, ..cols]
# pead_backtest[, mean_prob_1 = mean(), by = date]
pead_backtest <- pead_backtest[, .(symbol = list(symbol), prob.1_classif.ranger = list(prob.1_classif.ranger)), by = date]

# save to local path and dropbox
fwrite(pead_backtest, "D:/mlfin/backtest_data/pead_backtest.csv", col.names = FALSE, dateTimeAs = "write.csv")
drop_upload(file = "D:/mlfin/backtest_data/pead_backtest.csv", path = "pead")



