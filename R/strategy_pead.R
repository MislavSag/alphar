library(data.table)
library(fmpcloudr)
library(eventstudies)
library(xts)
library(tidyr)
library(ggplot2)
library(lubridate)
library(httr)
library(mlr3verse)
library(mlr3forecasting)
library(mlr3viz)
library(mlr3fselect)
library(mlr3extralearners)
library(mlr3misc)
library(mlr3hyperband)
library(DescTools)
library(TTR)
library(naniar)
library(future.apply)
library(Rcatch22)



# set fmpcloudr api token
API_KEY = "15cd5d0adf4bc6805a724b4417bbaafc"
fmpc_set_token(API_KEY)

# performance
plan(multicore(workers = 8))



# DATA IMPORT AND WRANGLING -----------------------------------------------

# usa stocks
url <- paste0("https://financialmodelingprep.com/api/v3/available-traded/list?apikey=", API_KEY)
stocks <- rbindlist(httr::content(GET(url)))
usa_symbols <- stocks[exchange %in% c("AMEX", "New York Stock Exchange Arca",
                                      "New York Stock Exchange", "NasdaqGS",
                                      "Nasdaq", "NASDAQ", "AMEX", "NYSEArca",
                                      "NASDAQ Global Market", "Nasdaq Global Market",
                                      "NYSE American", "Nasdaq Capital Market",
                                      "Nasdaq Global Select")]
url <- paste0("https://financialmodelingprep.com/api/v3/historical/sp500_constituent?apikey=", API_KEY)
sp500 <- rbindlist(httr::content(GET(url)))
sp500_symbols <- unique(c(sp500$removedTicker, sp500$symbol))

# get earnings estimates and actual earnings
earnings <- fread("D:/fundamental_data/earnings_announcement/ea-2021-06-09.csv")
earnings[, date := as.Date(date)]
earnings <- earnings[date < Sys.Date()]     # remove announcements for today
earnings <- na.omit(earnings, cols = c("eps", "epsEstimated")) # remove rows with NA for earnings
earnings[revenue == 0 | revenueEstimated == 0] # number of missing revenues data

# get earnings calls transcripts (conference calls)
transcript_files <- list.files("D:/fundamental_data/transcripts", full.names = TRUE)
files_sizes <- base::file.info(transcript_files, extra_cols = FALSE)["size"]
transcript_files <- transcript_files[files_sizes > 0]     # keep only files with data
transcripts <- lapply(transcript_files, fread)
transcripts <- transcripts[lengths(transcripts) == 5]     # keep only elements with 5 columns
wrong_class <- unlist(lapply(transcripts, function(x) {class(x$quarter) == "character"}))
transcripts <- transcripts[!wrong_class]                  # remove elements with quarter element as characetr type
right_names <- unlist(lapply(transcripts, function(x) {
  all(names(x) == c("symbol", "quarter", "year", "date", "content"))
  }))
transcripts <- transcripts[right_names]                   # keep elements with right element names
transcripts <- rbindlist(transcripts)
data.table::setnames(transcripts, "date", "datetime_transcript")
table(format.POSIXct(transcripts$date, "%H:%M:%S")) # frequenciese of transcript times; most are > 17:00
transcripts[, date := as.Date(datetime_transcript)]
transcripts[, date_transcript := date] # we will need date which will be discard in merge
transcripts <- transcripts[!duplicated(transcripts[, .(symbol, date)])] # remove one duplicate

# merge earnings and transcripts
DT <- transcripts[earnings, on = c(symbol = "symbol", date = "date"), roll = "nearest"]
DT[, .(symbol, date, date_transcript, datetime_transcript)]  # view merge. Transcript date >= date
DT[date_transcript < date, `:=`(content = NA, date_transcript = NA)] # transcript date can't be before earnings announcement date (?)
DT[, date_diff := date_transcript - date] # diff between earning report date and transcript call date
DT[date_diff > 10, `:=`(content = NA, date_transcript = NA)] # remove if transcript is delayed by more than 10 days
# DT[!is.na(content) & duplicated(content)] # show duplicates
DT[!is.na(content) & duplicated(content), `:=`(content = NA, date_transcript = NA)] # duplicated transcripts to NA
DT[, `:=`(date_diff = NULL, year = NULL)]
str(DT)
rm(list = c("earnings", "transcripts"))

# get daily market data for all stocks
prices <- fread("D:/fundamental_data/daily_data/daily_prices.csv")
prices[, returns := adjClose / data.table::shift(adjClose) - 1, by = symbol]
sum(is.na(prices$adjClose))
prices_n <- prices[, .N, by = symbol]
prices_n <- prices_n[which(prices_n$N > 30)]  # remove prices with only 30 observations
prices <- prices[symbol %in% prices_n$symbol]



# CREATE LABELS -----------------------------------------------------------

# Cimport AR data
car_labels <- future_lapply(list.files("D:/fundamental_data/car/labels", full.names = TRUE), fread,
                            colClasses = c(symbol = "character", abnormal_returns_test_last = "numeric"))
car_labels <- rbindlist(car_labels)
car_labels$abnormal_returns_test_last <- as.numeric(car_labels$abnormal_returns_test_last)

# merge labels and DT
DT <- DT[symbol %in% car_labels$symbol]
DT <- merge(DT, car_labels, by.x = c("symbol", "date"), by.y = c("symbol", "datetime"), all.x = TRUE, all.y = FALSE)
str(DT)



# GENERATE FEATURES -------------------------------------------------------

# features from prices
indicators <- copy(prices)
setorderv(indicators, c("symbol", "date"))

# import time series features
catch_22_features <- list.files("D:/fundamental_data/catch22", full.names = TRUE)
catch_22_features <- lapply(catch_22_features, fread)
catch_22_features <- catch_22_features[sapply(catch_22_features, nrow)>0]
catch_22_features <- rbindlist(catch_22_features)
indicators <- catch_22_features[indicators, on = c(symbol = "symbol", date = "date")]

# indicators
close_ath_percente <- function(close) {
  cl_ath <- cummax(close)
  cl_ath_dev <- (cl_ath - close) / cl_ath
  return(cl_ath_dev)
}
indicators[, `:=`(
  returns_week = frollsum(returns, 5, na.rm = TRUE),
  returns_month = frollsum(returns, 22, na.rm = TRUE),
  volume_week = frollmean(volume, 5, na.rm = TRUE),
  volume_month = frollmean(volume, 22, na.rm = TRUE),
  close_ath = close_ath_percente(adjClose),
  rsi_month = RSI(adjClose, 22),
  # macd = MACD(adjClose, 22)[, "macd"],
  # macdsignal = MACD(adjClose, 22)[, "signal"],
  std_week = roll::roll_sd(returns, 5),
  std_month = roll::roll_sd(returns, 22),
  skew_month = as.vector(RollingWindow::RollingSkew(returns, window = 22, na_method = 'ignore')),
  kurt_month = as.vector(RollingWindow::RollingKurt(returns, window = 22, na_method = 'ignore'))
), by = symbol]

# merge indicators and announcement data
DT[, date_events := date]
A <- indicators[DT, on = c(symbol = "symbol", date = "date"), roll = -Inf]
str(A)
A[, .(symbol, date, date_events, date_transcript, datetime_transcript)]

# actual vs estimated
A[, `:=`(
  eps_diff = (eps - epsEstimated) / adjClose,
  rev_diff = (revenue - revenueEstimated) / adjClose
)]
setorderv(A, c("symbol", "date"))
str(A)

# fundamental data
reports <- fread("D:/fundamental_data/pl.csv")
reports[, `:=`(date = as.Date(date),
               fillingDate = as.Date(fillingDate),
               acceptedDateTime = as.POSIXct(acceptedDate, format = "%Y-%m-%d %H:%M:%S"),
               acceptedDate = as.Date(acceptedDate, format = "%Y-%m-%d %H:%M:%S"))]
fin_growth <- fread("D:/fundamental_data/fin_growth.csv")
fin_growth[, date := as.Date(date)]
fin_ratios <- fread("D:/fundamental_data/key_metrics.csv")
fin_ratios[, date := as.Date(date)]

# merge all fundamental data
fundamentals <- merge(reports, fin_growth, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE)
fundamentals <- merge(fundamentals, fin_ratios, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE)
fundamentals <- fundamentals[date > as.Date("1998-01-01")]
fundamentals[, acceptedDateFundamentals := acceptedDate]
data.table::setnames(fundamentals, "date", "fundamental_date")
str(fundamentals)

# merge fundamental data and A
str(A)
head(A[symbol == "AAPL", .(symbol, date, adjClose, datetime)])
tail(A[symbol == "AAPL", .(symbol, date, adjClose, datetime)])
head(fundamentals[symbol == "AAPL", .(symbol, fundamental_date, fillingDate, acceptedDate, acceptedDateFundamentals)])
tail(fundamentals[symbol == "AAPL", .(symbol, fundamental_date, fillingDate, acceptedDate, acceptedDateFundamentals)])

# A <- fundamentals[, .(symbol, acceptedDate, ebitgrowth)][A[, .(symbol, date, volume_month)], on = c(symbol = "symbol", "acceptedDate" = "date")]
A <- fundamentals[A, on = c(symbol = "symbol", "acceptedDate" = "date")] # exact match!
A <- A[!is.na(ebitgrowth)]  # THIS DECREASES DATA BY HUGE AMOUNT
# test[symbol == "AAPL", .(symbol, acceptedDate, acceptedDateFundamentals, datetime)]
# test[symbol == "AAPL" & !is.na(content), .(symbol, acceptedDate, acceptedDateFundamentals, datetime)]
# head(A, 1)

# earning calls sentiments
A[symbol == "AAPL" & date_events %between% c("2020-01-01", "2021-12-31")]



# CLASSIFICATION ----------------------------------------------------------

# filter by symbols
clf_data <- A[symbol %in% usa_symbols$symbol]

# select features vector
str(clf_data)
features <- setdiff(
  c(colnames(A)[which(colnames(A) == "revenueGrowth"):which(colnames(A) == "sgaexpensesGrowth")],
    colnames(A)[which(colnames(A) == "CO_f1ecac"):which(colnames(A) == "SP_Summaries_welch_rect_centroid")],
    colnames(A)[which(colnames(A) == "returns"):ncol(A)]),
  c("outcomes", "abnormal_returns_test_last", "quarter", "year",
    "content", "datetime", "date_transcript", "datetime_transcript",
    "time", "date_diff", "revenue", # "revenueEstimated", # "rev_diff",
    "date_events"))
features_labels <- c(features, "abnormal_returns_test_last")

# filter only success
clf_data <- clf_data[outcomes == "success"]
clf_data <- clf_data[, ..features_labels]

# NA nalysis
naniar::gg_miss_var(clf_data)

# remove Inf values
clf_data <- clf_data[epsEstimated != 0]
clf_data <- clf_data[revenueEstimated != 0]
clf_data <- clf_data[rev_diff != 0]
clf_data <- na.omit(clf_data)
char_cols <- colnames(clf_data)[which(unlist(lapply(clf_data, is.character), use.names = FALSE))]
clf_data[, (char_cols):= lapply(.SD, as.numeric), .SDcols = char_cols]
clf_data <- clf_data[!is.infinite(rowSums(clf_data))]

# create bins (labels)
# 1) Create -1, 0 and 1, where every group is nth tercile.
#    For example, -1 is [min(abnormal_returns_test_last), 33 percentile]
# 2) Create {0, 1} bins. If absolute return is > 0, than 1, vice versa
# 3) Create {0, 1} bins. 0 is lowest 20 quanntile. 1 is highest 20 quantile
clf_data[, bin := cut(abnormal_returns_test_last,
                      quantile(abnormal_returns_test_last, probs = c(0, .33, 0.66, 1)),
                      labels = c(-1L, 0L, 1L),
                      include.lowest = TRUE)]
table(clf_data$bin)
clf_data[, bin01 := as.factor(ifelse(abnormal_returns_test_last > 0, 1L, 0L))]
table(as.integer(clf_data$bin01))
clf_data[, binextreme := cut(abnormal_returns_test_last,
                      quantile(abnormal_returns_test_last, probs = c(0, 0.2, 0.8, 1)),
                      labels = c(-1, 0, 1),
                      include.lowest = TRUE)]
table(clf_data$binextreme)
clf_data[, `:=`(
  abnormal_returns_test_last = NULL
  )]

# remove outliers
skimr::skim(clf_data)
clf_data[, (features) := lapply(.SD, Winsorize, probs = c(0.05, 0.95)), .SDcols = features] # winsorize across dates
skimr::skim(clf_data)

# define task
task = TaskClassif$new(id = 'mlfin_earnings', backend = clf_data[, .SD, .SDcols = !c("bin01", "binextreme")], target = 'bin')
task_01 = TaskClassif$new(id = 'mlfin_earnings', backend = clf_data[, .SD, .SDcols = !c("bin", "binextreme")], target = 'bin01')
task_extreme <- clf_data[binextreme != 0, .SD, .SDcols = !c("bin", "bin01")]
task_extreme$binextreme <- droplevels(task_extreme$binextreme)
task_extreme = TaskClassif$new(id = 'mlfin_earnings', backend = task_extreme, target = 'binextreme')
print(task$nrow)
print(task_01$nrow)
print(task_extreme$nrow)

# mlr outer resmaling, main measure, terminators, type of search
# same for all models!
resampling = rsmp('holdout', ratio = 0.7)
measure = msr("classif.acc")
terminator = trm('evals', n_evals = 30)
tuner = tnr("grid_search", resolution = 1)


# FEATURES IMPORTANCE -----------------------------------------------------

# filters
filter_features <- function(task, filters) {
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
filtered_features_praznik <- filter_features(task, c("disr", "jmim", "jmi", "mim", "mrmr", "njmim", "cmim"))
filtered_features_praznik_01 <- filter_features(task_01, c("disr", "jmim", "jmi", "mim", "mrmr", "njmim", "cmim"))
filtered_features_praznik_extreme <- filter_features(task_extreme, c("disr", "jmim", "jmi", "mim", "mrmr", "njmim", "cmim"))
# filtered_features_relief <- filter_features(c("relief", "information_gain"))

# selection
learners <- list(
  lrn("classif.ranger", predict_type = "prob"),
  # lrn("classif.svm", type = "C-classification", kernel = "radial", predict_type = "prob"),
  lrn('classif.xgboost', predict_type = "prob")
  # lrn("classif.naive_bayes", predict_type = "prob"),
  # lrn("classif.catboost", predict_type = "prob"),
  # lrn("classif.gbm", predict_type = "prob"),
  # lrn("classif.lightgbm", predict_type = "prob"),
  # lrn("classif.rfsrc", predict_type = "prob")
  )
fselector = fs("random_search")
autofs <- lapply(learners, function(x) {
  AutoFSelector$new(x, resampling, measure, terminator, fselector)
})

# set benchmark
design = benchmark_grid(
  task = list(task_extreme),
  learner = autofs,
  resampling = rsmp("cv", folds = 5)
)
bmr_fs = benchmark(design, store_models = TRUE)

# save feature importance results
file_path <- "C:/Users/Mislav/Documents/GitHub/alphar/mlmodels"
file_name <- paste0("feature-importance-earnings-", format.POSIXct(Sys.time(), "%Y-%m-%d%H-%M-%S"), ".rds")
saveRDS(bmr_fs, file = file.path(file_path, file_name))
bmr_fs <- readRDS("C:/Users/Mislav/Documents/GitHub/alphar/mlmodels/feature-importance-earnings-2021-07-0608-56-15.rds")

# extract 10 most important features
bmr_fs$aggregate(c(msrs("classif.acc"), msrs("classif.ce")))
important_features <- lapply(bmr_fs$data$learners()$learner, function(x) unlist(x$fselect_result$features))
important_features <- as.data.table(table(unlist(important_features)))
important_features <- important_features[order(N, decreasing = TRUE), ]
important_features <- important_features[N > (length(autofs) * 5 / 2), ] # 5 is number of folds
setnames(important_features, 'V1', 'feature')
most_important_features <- fintersect(filtered_features_praznik[1:20, 1], important_features[, .(feature)])

# choose only most important features in the task
# task$select(most_important_features[[1]])
task$select(important_features[1:20][[1]])



# MODEL -------------------------------------------------------------------

# functoin for constracting autotuners
make_auto_tuner <- function(learner, params) {
  AutoTuner$new(
    learner = learner,
    resampling = rsmp('holdout', ratio = 0.8),
    measure = msr("classif.acc"),
    search_space = params,
    terminator = trm('evals', n_evals = 40),
    tuner = tnr("random_search"),
    store_models = TRUE
  )
}

# adaboost DOESNT WORK
# learner = lrn("classif.AdaBoostM1", predict_type = "prob", predict_sets = c("train", "test"))
# params = ParamSet$new(
#   params = list(
#     ParamInt$new("P", lower = 90, upper = 100),
#     ParamInt$new("S", lower = 1, upper = 10)
#
#   ))
# adaboost = make_auto_tuner(learner, params)

# catboost ERROR DURING TRENING
# learner = lrn("classif.atboost", predict_type = "prob", predict_sets = c("train", "test"))
# params = ParamSet$new(
#   params = list(
#     ParamInt$new("ntree", lower = 2, upper = 20),
#     ParamDbl$new("k", lower = 0.1, upper = 3),
#     ParamDbl$new("power", lower = 0.1, upper = 3)
#   ))
# catboost = make_auto_tuner(learner, params)


# clf c50
learner = lrn("classif.C50", predict_type = "prob", predict_sets = c("train", "test"))
params = ParamSet$new(
  params = list(
    ParamDbl$new("CF", lower = 0, upper = 1),
    ParamInt$new("trials", lower = 1, upper = 50)

  ))
clfc50 = make_auto_tuner(learner, params)

# random forest
learner = lrn("classif.ranger", predict_type = "prob", predict_sets = c("train", "test"))
params = ParamSet$new(
  params = list(
    ParamInt$new("max.depth", lower = 2, upper = 7),
    ParamInt$new("min.node.size", lower = 3, upper = 6)

  ))
rf = make_auto_tuner(learner, params)

# xgboost
learner = lrn("classif.xgboost", predict_type = "prob", predict_sets = c("train", "test"))
params = ParamSet$new(
  params = list(
    ParamInt$new("max_depth", lower = 2, upper = 7),
    ParamDbl$new("colsample_bytree", lower = 0.1, upper = 0.8)

  ))
xgboost = make_auto_tuner(learner, params)

# neural net
# learner = lrn("classif.nnet", predict_type = "prob", predict_sets = c("train", "test"))
# params = ParamSet$new(
#   params = list(
#     ParamInt$new("size", lower = 2, upper = 6),
#     ParamDbl$new("decay", lower = 0, upper = 0.01),
#     ParamInt$new("maxit", lower = 200, upper = 400)
#   ))
# nnet = make_auto_tuner(learner, params)

# glmnet
learner = lrn("classif.glmnet", predict_type = "prob", predict_sets = c("train", "test"))
params = ParamSet$new(
  params = list(
    ParamInt$new("alpha", lower = 0, upper = 1)
  ))
glmnet = make_auto_tuner(learner, params)

# learners used for benchmarking
featureless = lrn("classif.featureless", predict_type = "prob", predict_sets = c("train", "test"))

# set benchmark
design = benchmark_grid(
  task = list(task_extreme),
  learner = list(clfc50, rf, xgboost, glmnet, featureless),
  resampling = rsmp("cv", folds = 5)
)
bmr = mlr3::benchmark(design = design, store_models = TRUE)


# INSPECT RESULTS AND SAVE ------------------------------------------------

# results by mean
bmr_results <- bmr$clone(deep = TRUE)
measures = list(
  msr("classif.acc", id = "acc_train", predict_sets = 'train'),
  msr("classif.acc", id = "acc_test", predict_sets = 'test')
  # msr("classif.auc", id = "auc_train", predict_sets = 'train'),
  # msr("classif.auc", id = "auc_test", predict_sets = 'test')

)
bmr_results$aggregate(measures)
tab = bmr_results$aggregate(measures)
ranks = tab[, .(learner_id, rank_train = rank(acc_train), rank_test = rank(acc_test)), by = task_id]
ranks = ranks[, .(mrank_train = mean(rank_train), mrank_test = mean(rank_test)), by = learner_id]
ranks[order(mrank_test)]
autoplot(bmr_results) + theme(axis.text.x = element_text(angle = 45, hjust = 1))

# result for specific model
choose_model <- 'classif.ranger.tuned'
best_submodel <- bmr$score(measures)[learner_id == choose_model, ][acc_test == max(acc_test)]$learner[[1]]
# pred = best_submodel$learner$train(task)$predict(task)
pred = best_submodel$learner$predict(task_extreme)

# analyse best model
pred$confusion
best_submodel$learner
prob_conf <- cbind.data.frame(pred$data$row_ids, pred$data$truth, pred$data$response, pred$data$prob)
x <- prob_conf[prob_conf$`1` > 0.60, ]
sum(x$`pred$data$truth` == 1) / nrow(x)

# model psecific graphs
# plot(best_submodel$learner$model)

# save best model
file_name <- paste0('mlmodels/', gsub('\\.', '_', choose_model), '_', best_submodel$hash, '.rds')
saveRDS(best_submodel, file = file_name)

# save benchmark
file_name <- paste0('mlmodels/bmr_results', '-', format.POSIXct(Sys.time(), "%Y%m%d-%H%M%S"), '.rds')
saveRDS(bmr_results, file = file_name)



# TODO --------------------------------------------------------------------


###### REMOVE FOR NOW, DATA SEEMS WRONG
# # earnings estimates
# qearnings <- fmpc_analyst_outlook(tickers, 'estimateQtr', limit = 1000)
# qearnings <- as.data.table(qearnings)
# qearnings[, date := as.Date(date)]
# qearnings[, quarter := data.table::quarter(date)]
# qearnings[, year := data.table::year(date)]
# qearnings[, date := NULL]
# DT <- merge(DT, qearnings, by = c("symbol", "quarter", "year"), all.x = TRUE, all.y = FALSE)
###### REMOVE FOR NOW, DATA SEEMS WRONG


# CHECK LATER
fmpc_analyst_outlook(tickers, 'grade', limit = 1000)
s <- fmpc_analyst_outlook(tickers, 'surprise', limit = 1000)
fmpc_analyst_outlook(ticker, 'recommend')
fmpc_analyst_outlook(ticker, 'press')
x <- fmpc_earning_call_transcript(ticker, quarter = 1, year = 2019)


