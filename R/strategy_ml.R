library(data.table)
library(mlfinance)
library(mlr3verse)
library(mlr3forecasting)
library(mlr3viz)
library(mlr3fselect)
library(mlr3extralearners)
library(mlr3misc)
library(ggplot2)
library(future.apply)
library(cpm)
library(DescTools)
library(leanr)

# for perfromance
plan(multiprocess(workers = 8))  # multiprocess(workers = availableCores() - 8)




# SET PARAMETERS ----------------------------------------------------------

# params
filtering = ParamSet$new(
  params = list(
    ParamFct$new(id = 'filtering', levels = c('gpd_cusum')), # 'cusum', 'gpd', vix
    ParamDbl$new(id = 'cusum_threshold', lower = 0.005, upper = 0.01),
    ParamInt$new(id = 'vix_threshold', lower = 20, upper = 30),
    ParamInt$new(id = 'gpd_threshold', lower = 10, upper = 18),
    ParamFct$new(id = 'labeling', levels = c('tb')),
    ParamDbl$new(id = 'targets', lower = 0.03, upper = 0.07)
  )
)
# filtering$add_dep("threshold", 'filtering', CondEqual$new('cusum'))
# filtering$add_dep("vix_threshold", 'filtering', CondEqual$new('vix'))
# filtering$add_dep("gpd_threshold", 'filtering', CondEqual$new('gpd'))

# CUSUM filter
CUSUM_Price <- function(data, h){
  POS <- NEG <- 0
  index <- NULL
  diff_data <- base::diff(data)
  for (i in 1:length(diff_data)){
    POS <- max(0,POS+diff_data[i])
    NEG <- min(0,POS+diff_data[i])
    if(max(POS,-NEG)>=h){
      index <- c(index, i)
      POS <- NEG <- 0
    }
  }
  return(index+1)
}



# IMPORT DATA -------------------------------------------------------------

# import risks data
# risk_path <- "D:/risks"
# gpd_risks_left_tail <- fread(file.path(risk_path, 'gpd_risks_left_tail.csv'), sep = ';')
# gpd_risks_right_tail <- fread(file.path(risk_path, 'gpd_risks_right_tail.csv'))
# cols <- colnames(gpd_risks_right_tail)[grep('q_|e_', colnames(gpd_risks_right_tail))]
# gpd_risks_right_tail[, (cols) := lapply(.SD, as.numeric), .SDcols = cols]
# gpd_risks_left_tail[, (cols) := lapply(.SD, as.numeric), .SDcols = cols]
# colnames(gpd_risks_left_tail) <- paste0("left_", colnames(gpd_risks_left_tail))
# colnames(gpd_risks_right_tail) <- paste0("right_", colnames(gpd_risks_right_tail))

# import market data and merge with risks data
# stocks <- import_lean("D:/market_data/equity/usa/hour/trades_adjusted")
# stocks <- merge(gpd_risks_left_tail, stocks, by.y = c("symbol", "datetime"),
#                 by.x = c('left_symbol', 'left_date'), all.x = TRUE, all.y = FALSE)
# stocks <- merge(stocks, gpd_risks_right_tail, by.y = c("right_symbol", "right_date"),
#                 by.x = c('left_symbol', 'left_date'), all.x = TRUE, all.y = FALSE)
# rm(gpd_risks_left_tail, gpd_risks_right_tail)

# import Var and ES forecasts
risk_paths <- list.files("D:/risks/risk-factors/hour/", full.names = TRUE)
# x <- risk_paths[1]
var_es_data <- lapply(risk_paths, function(x) {
  # y <- list.files(x, full.names = TRUE)[1]
  data_ <- lapply(list.files(x, full.names = TRUE), function(y) {
    # filter Var and ES model
    if (grepl("975_GARCH_plain_100_150|975_EWMA_age_100_150", y)) {
      data_id <- fread(y)
      colnames(data_id)[2:ncol(data_id)] <- paste0(gsub(".*/|\\.csv", "", y), colnames(data_id)[2:ncol(data_id)])
      data_id[, symbol := gsub("_.*", "", gsub(".*/|\\.csv", "", y))]
      data_id <- na.omit(data_id)
    } else {
      data_id <- as.data.table(NULL)
    }
    data_id
  })
  data_[sapply(data_, function(x) nrow(x)) == 0] <- NULL
  Reduce(function(...) merge(..., by = c("symbol", "datetime"), all = FALSE), data_)
})
var_es_data[sapply(var_es_data, function(x) length(x)) == 0] <- NULL
var_es_data <- rbindlist(var_es_data)

# import market data and merge with risks data
stocks <- import_lean("D:/market_data/equity/usa/hour/trades_adjusted")
stocks <- merge(var_es_data, stocks, by = c("symbol", "datetime"), all.x = TRUE, all.y = FALSE)

# import exuber vars
# exuber_data <- lapply(list.files("D:/risks/radf", full.names = TRUE), fread)
# exuber_data <- rbindlist(exuber_data)
# stocks <- merge(stocks, exuber_data, by.x = c("left_symbol", "left_date"),
#                 by.y = c('symbol', 'datetime'), all.x = TRUE, all.y = FALSE)
# rm(exuber_data)

# import changepoints
# chg <- fread(file.path(risk_path, 'changepoints.csv'))
# stocks <- merge(chg, stocks, by.x = c(".id", "index"),
#                 by.y = c('left_symbol', 'left_date'), all.x = TRUE, all.y = FALSE)
# rm(chg)

# clean stocks
setorderv(stocks, c('symbol', 'datetime'))
stocks <- stocks[!is.na(close)]
stocks <- stocks[stocks[, .N, by = .(symbol)][N > 500], on = "symbol"] # stock have at least 1 year of data

# create features
features <- stocks[, .(symbol, datetime, open, high, low, close)]
cols <- colnames(add_features(as.xts.data.table(features[symbol == "AAPL", .(datetime, open, high, low, close)])))
features <- features[, as.data.table(add_features(as.xts.data.table(data.table(datetime, open, high, low, close))))[, index := NULL],
                     by = symbol]
features[, symbol := NULL]

# merge all together
DT <- copy(stocks)
DT <- cbind(DT, features)
rm(features)



# DATA PREP ---------------------------------------------------------------

# impute missing values;outliers;; scaling
features_num <- colnames(DT)[grep('left|right|volume|skewness|std|kurtosis', colnames(DT))] # columns to change
DT[, (features_num) := lapply(.SD, na.locf, na.rm = FALSE), by = symbol, .SDcols = features_num] # impute NA with last observation
DT <- na.omit(DT) # omit missing vlaues
# DT[, (features_num) := lapply(.SD, Winsorize, probs = c(0.03, 0.97)), by = datetime, .SDcols = features_num] # winsorize across dates
DT <- DT[, (features_num) := lapply(.SD, function(x) dplyr::percent_rank(x)), by = datetime, .SDcols = features_num] # rank across dates

# labels
sample <- copy(DT)
sample[, bin := mlfinance::labeling_fixed_time(close, 0.01, horizont = 5 * 8), by = symbol]
table(sample$bin)
sample <- sample[bin %in% c(-1, 1)]
sample[, bin := ifelse(bin == -1, 0, 1)]

# filtering
cusum_filter_index <- sample[, .I[1] + CUSUM_Price(close, 2) - 1, by = symbol]
sample <- sample[cusum_filter_index$V1]

# final clean
sample <- sample[order(datetime)]
sample[, colnames(sample)[duplicated(colnames(sample))]] <- NULL
sample[, c("symbol", "datetime", "open", "high", "low", "close", 'N', 'cl_ath')] <- NULL
sample <- sample[, lapply(.SD, as.numeric)]
sample[, bin := as.factor(bin)]
sample <- sample[, .(A_975_GARCH_plain_100_150var_month, bin)]
str(sample)



# # EXPLARATOR ANALYSIS -----------------------------------------------------
#
# # 1) median across
# x <- stocks[, .(symbol, datetime, left_e_9999_2000_15)]
# x <- x[, sum(left_e_9999_2000_15, na.rm = TRUE), by = datetime]
# x <- x[order(datetime)]
# x <- na.omit(x)
# x[, V1 := Winsorize(V1, p = c(0.001, 0.999))]
# plot(x$datetime, x$V1, type = 'l')
#
#
# # 2) calculate indicators
# left_q_cols <- colnames(stocks)[grep('left_q', colnames(stocks))]
# right_q_cols <- colnames(stocks)[grep('right_q', colnames(stocks))]
# excess_cols <- gsub("right", "excess", colnames(stocks)[grep('right_q', colnames(stocks))])
# excess_cols_dummy <- paste0('dummy_', excess_cols)
# sp500_stocks <- copy(stocks)
# sp500_stocks[, (excess_cols) := sp500_stocks[, ..left_q_cols] - sp500_stocks[, ..right_q_cols]]
# sp500_stocks[, (excess_cols_dummy) := lapply(.SD, function(x) ifelse(x > 0, 1, 0)), .SDcols = excess_cols]
# sp500_stocks[, paste0(excess_cols, '_median') := lapply(.SD, function(x) roll_mean(x, 300, min_obs = 100)),
#              by = .(symbol),
#              .SDcols = excess_cols]
#
# # alterative indicators
# sample <- sp500_stocks[, .(symbol, datetime)]
# sample[, indicator := rowSums(sp500_stocks[, ..excess_cols], na.rm = TRUE)]
# sample <- sample[order(indicator, decreasing = TRUE)]
# sample <- sample[, date := lubridate::date(datetime)]
# test <- sample[, median(indicator, na.rm = TRUE), by = c('datetime')]
# test <- test[order(V1, decreasing = TRUE)]
# test[1:99]
#
# # sum variables across symbols
# x <- test[, .(symbol, datetime, V1)]
# x <- x[, median(V1, na.rm = TRUE), by = datetime]
# x <- x[order(datetime)]
# plot(x$datetime, x$V1, type = 'l')
#
# # backtest
# sample[, side := ifelse(shift(V1) > 1, 0, 1)]
# sample[, returns_strategy := side * returns]
# PerformanceAnalytics::charts.PerformanceSummary(sample[, .(index, returns, returns_strategy)], plot.engine = 'ggplot2')
# charts.PerformanceSummary(sample[index %between% c('2017-01-01', '2018-01-01'), .(index, returns, returns_strategy)],
#                           plot.engine = 'ggplot2')

# TASKS AND SHARED MLR3 OBJECTS -------------------------------------------

# mlr3 task
task = TaskClassif$new(id = 'mlfin', backend = sample, target = 'bin', positive = '1')
# task$set_col_roles('weights', roles = 'weight')

# select features
# remove_cols <- c("symbol", "datetime", "open", "high", "low", "close")
# task$select(setdiff(task$feature_names, remove_cols))

# remove missing values
task$filter(which(complete.cases(task$data())))

# mlr outer resmaling, main measure, terminators, type of search
# same for all models!
resampling = rsmp('forecastHoldout', ratio = 0.7)
measure = msr("classif.auc")
terminator = trm('evals', n_evals = 30)
tuner = tnr("grid_search", resolution = 1)



# FEATURE SELECTION -------------------------------------------------------

# filters
filter_features <- function(filters) {
  filters = flts(filters)
  filtered_features <- rbindlist(lapply(filters, function(x) {
    flt <- x$calculate(task)
    flt_table <- as.data.table(flt)[, `:=`(id = flt$id, package = flt$packages)]
  })
  )
  filtered_features <- filtered_features[, sum(score), by = c('feature')]
  filtered_features <- filtered_features[order(V1, decreasing = TRUE)]
  filtered_features[, rank := 1:.N][]
}
filtered_features_praznik <- filter_features(c("disr", "jmim", "jmi", "mim", "mrmr", "njmim", "cmim"))
# filtered_features_relief <- filter_features(c("relief", "information_gain"))

# selection
learners <- list(lrn("classif.randomForest"), lrn("classif.naive_bayes"),
                 lrn("classif.ranger"), lrn('classif.xgboost'))
fselector = fs("random_search")
autofs <- lapply(learners, function(x) {
    AutoFSelector$new(x, resampling, measure, terminator, fselector)
})

# set benchmark
design = benchmark_grid(
  task = task,
  learner = autofs,
  resampling = rsmp("cv", folds = 5)
)
bmr_fs = benchmark(design, store_models = TRUE)

# extract 10 most important features
bmr_fs$aggregate(c(msrs("classif.acc"), msrs("classif.ce")))
important_features <- lapply(bmr_fs$data$learners()$learner, function(x) unlist(x$fselect_result$features))
important_features <- as.data.table(table(unlist(important_features)))
important_features <- important_features[order(N, decreasing = TRUE), ]
important_features <- important_features[N > (length(autofs) * 5 / 2), ] # 5 is number of folds
setnames(important_features, 'V1', 'feature')
most_important_features <- fintersect(filtered_features_praznik[1:20, 1], important_features[, .(feature)])

# choose only most important features in the task
task$select(most_important_features[[1]])
task$select(filtered_features_praznik$feature[1:20])


# TRAIN ML MODELS WITH MOST IMPORTANT FEATURES ----------------------------

# functoin for constracting autotuners
make_auto_tuner <- function(learner, params) {
  AutoTuner$new(
    learner = learner,
    resampling = resampling,
    measure = measure,
    search_space = params,
    terminator = terminator,
    tuner = tuner,
    store_models = TRUE
  )
}

# random forest
learner = lrn("classif.ranger", predict_type = "prob", predict_sets = c("train", "test"))
params = ParamSet$new(
  params = list(
    ParamInt$new("max.depth", lower = 2, upper = 6),
    ParamInt$new("min.node.size", lower = 3, upper = 6)

  ))
rf = make_auto_tuner(learner, params)

# xgboost
learner = lrn("classif.xgboost", predict_type = "prob", predict_sets = c("train", "test"))
params = ParamSet$new(
  params = list(
    ParamInt$new("max_depth", lower = 2, upper = 6),
    ParamDbl$new("colsample_bytree", lower = 0.1, upper = 0.7)

  ))
xgboost = make_auto_tuner(learner, params)

# neural net
learner = lrn("classif.nnet", predict_type = "prob", predict_sets = c("train", "test"))
params = ParamSet$new(
  params = list(
    ParamInt$new("size", lower = 2, upper = 6),
    ParamDbl$new("decay", lower = 0, upper = 0.001),
    ParamInt$new("maxit", lower = 200, upper = 400)
  ))
nnet = make_auto_tuner(learner, params)

# glmnet
learner = lrn("classif.glmnet", predict_type = "prob", predict_sets = c("train", "test"))
params = ParamSet$new(
  params = list(
    ParamInt$new("alpha", lower = 0, upper = 1)
  ))
glmnet = make_auto_tuner(learner, params)


# adaboostm1
# learner = lrn("classif.AdaBoostM1", predict_type = "prob", predict_sets = c("train", "test"))
# rf_params = ParamSet$new(
#   params = list(
#     ParamInt$new("I", lower = 10, upper = 20)
#
#   ))
# adaboostm1 = AutoTuner$new(
#   learner = learner,
#   resampling = resampling,
#   measure = measure,
#   search_space = rf_params,
#   terminator = terminator,
#   tuner = tuner,
#   store_models = TRUE
# )
# learner$param_set

# learners used for benchmarking
featureless = lrn("classif.featureless", predict_type = "prob", predict_sets = c("train", "test"))

# set benchmark
design = benchmark_grid(
  task = task,
  learner = list(rf, xgboost, nnet, glmnet, featureless),
  resampling = rsmp("cv", folds = 5)
)
bmr = mlr3::benchmark(design = design, store_models = TRUE)



# INSPECT RESULTS AND SAVE ------------------------------------------------

# results by mean
bmr_results <- bmr$clone(deep = TRUE)
measures = list(
  msr("classif.acc", id = "acc_train", predict_sets = 'train'),
  msr("classif.acc", id = "acc_test", predict_sets = 'test'),
  msr("classif.auc", id = "auc_train", predict_sets = 'train'),
  msr("classif.auc", id = "auc_test", predict_sets = 'test')

)
bmr_results$aggregate(measures)
tab = bmr_results$aggregate(measures)
ranks = tab[, .(learner_id, rank_train = rank(-auc_train), rank_test = rank(-auc_test)), by = task_id]
ranks = ranks[, .(mrank_train = mean(rank_train), mrank_test = mean(rank_test)), by = learner_id]
ranks[order(mrank_test)]
autoplot(bmr_results) + theme(axis.text.x = element_text(angle = 45, hjust = 1))
autoplot(bmr_results$clone(deep = TRUE)$filter(task_id = "mlfin"), type = "roc")

# result for specific model
choose_model <- 'classif.ranger.tuned'
measures = list(
  msr("classif.acc", id = "acc_train", predict_sets = 'train'),
  msr("classif.acc", id = "acc_test", predict_sets = 'test'),
  msr("classif.auc", id = "auc_train", predict_sets = 'train'),
  msr("classif.auc", id = "auc_test", predict_sets = 'test')
)
best_submodel <- bmr$score(measures)[learner_id == 'classif.ranger.tuned', ][acc_test == max(acc_test)]$learner[[1]]

# save
file_name <- paste0('mlmodels/', gsub('\\.', '_', choose_model), '_', best_submodel$hash, '.rds')
saveRDS(best_submodel, file = file_name)



# generate params
# params_prepare <- generate_design_random(filtering, 1)
# cat(params_prepare$data$filtering)

# filtering
# filtering_events <- lapply(risks_dt, function(x) {
#   close <- x[, .(Datetime, close)]
#   if (params_prepare$data$filtering == 'cusum') {
#       filtering_events <- cusum_filter(close, params_prepare$data$cusum_threshold)
#     } else if (params_prepare$data$filtering == 'vix') {
#       filtering_events <- x[VIX_close > params_prepare$data$vix_threshold][, .(Datetime)]
#     } else if (params_prepare$data$filtering == 'gpd') {
#       filtering_events <- x[gpd_es_2500_10_9990 * 1000 > params_prepare$data$gpd_threshold][, .(Datetime)]
#     } else if (params_prepare$data$filtering == 'gpd_cusum') {
#       filtering_events <- x[gpd_es_2500_10_9990 * 1000 > params_prepare$data$gpd_threshold][, .(Datetime)]
#       filtering_events_cusum <- cusum_filter(close, params_prepare$data$cusum_threshold)
#       filtering_events <- fintersect(filtering_events, filtering_events_cusum)
#     }
#   filtering_events
# })

# labeling
# options(future.globals.maxSize=850*1024^2) # this is needed because by default max allowed size is 500 MiB
# risks <- future_lapply(seq_along(risks_dt), function(x) {
#   close <- risks_dt[[x]][, .(Datetime, close)]
#   # labeling
#   if (params_prepare$data$labeling == 'tb') {
#     vertical_barieers <- add_vertical_barrier(filtering_events[[x]], close$Datetime, 180)
#     events <- get_events(price = close,
#                          t_events = filtering_events[[x]],
#                          pt_sl = c(1, 1),
#                          target = data.table(index = close$Datetime, 0.05), # params$data$targets
#                          min_ret = 0.0005,
#                          vertical_barrier_times = vertical_barieers
#     )
#     labels <- get_bins(events, close)
#     labels <- drop_labels(labels, 0.05)
#   }
#
#   # main data
#   X = risks_dt[[x]][labels, on = c(Datetime = 'Datetime')]
#   X[, bin := as.factor(as.character(X$bin))]
#   X
# })
# names(risks) <- names(risks_dt)

