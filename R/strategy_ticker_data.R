library(data.table)
library(QuantTools)
library(pins)
library(AzureStor)
library(highfrequency)
library(HighFreq)
library(QuantTools)

# TODO market profile indicator;

# metodologija:
#  on to zove NADRO (prema redoslijedu, odnosno važnosti):
# 1. Narrative (MP + Long term VWAPs),
# 2. Acceptance (gdje se cijena nalazi u odnosu na vrijednost)
# 3. Developing Value Area (kako se cijena kreće u odnosu na Developing Value Area koju dobiva iz VWAPa i st. dev. na trading chartu)
# 4. Rhythm (shvaćanje veličine rotacija / swingova)
# 5. Order Flow (čitanje naloga kroz Cumulative Delta ili FootPrint)

# set up
ENDPOINT = storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), key=Sys.getenv("BLOB-KEY"))

# parameters
symbols <- c("AAPL", "TSLA")

# download data from azure
lapply(symbols, function(x) {
  board <- board_azure(
    container = storage_container(ENDPOINT, "equity-usa-tick-finam"),
    path = tolower(x),
    n_processes = 10,
    versioned = FALSE,
    cache = "D:/findata/tick_finam"
  )
  lapply(pin_list(board), pin_download, board = board, version = NULL)
})

# import data
files_import <- list.files("D:/findata/tick_finam", full.names = TRUE)
files_import <- files_import[grep(paste0(tolower(symbols), collapse = "|"), files_import)]
files_import <- list.files(files_import, recursive = TRUE, pattern = "csv", full.names = TRUE)
tick_data <- lapply(files_import, function(file_) { # future_lapply
  fread(file = file_, col.names = c('SYMBOL', 'DT', "PRICE", "SIZE"))
})
tick_data <- rbindlist(tick_data)
tick_data$DT <- as.POSIXct(as.numeric(tick_data$DT),
                           origin=as.POSIXct("1970-01-01", tz="EST"),
                           tz="EST")

# clean raw tick data
tick_data <- tradesCleanup(tDataRaw = tick_data)
print(tick_data$report)
tick_data <- tick_data$tData
setorderv(tick_data, "DT")

# time bars
time_bars <- lapply(symbols, function(s) aggregateTrades(tick_data[SYMBOL == s], alignBy = "minutes", alignPeriod = 1))
time_bars <- rbindlist(time_bars)


# FILTER BARS -------------------------------------------------------------

# index for generating features
# labels_ <- labels[, .(SYMBOL, DT)]
# labels_$index <- TRUE
# filter_index <- labels_[time_bars[, .(SYMBOL, DT)], on = c("SYMBOL", "DT")]
# index_at <- which(filter_index$index == TRUE)
# index_at_lag <- c(index_at - 1)

# generate features
time_bars[, VWAP_DAY_DIV := ((VWPRICE - TTR::VWAP(VWPRICE, SIZE, 60 * 8)) / VWPRICE)]
time_bars[, VWAP_WEEK_DIV := ((VWPRICE - TTR::VWAP(VWPRICE, SIZE, 60 * 8 * 5)) / VWPRICE)]
time_bars[, VWAP_MONTH_DIV := ((VWPRICE - TTR::VWAP(VWPRICE, SIZE, 60 * 8 * 22)) / VWPRICE)]
time_bars[, VWAP_HALF_YEAR_DIV := ((VWPRICE - TTR::VWAP(VWPRICE, SIZE, 60 * 8 * 22 * 6)) / VWPRICE)]
time_bars[, VWAP_YEAR_DIV := ((VWPRICE - TTR::VWAP(VWPRICE, SIZE, 60 * 8 * 22 * 12)) / VWPRICE)]
ticks_ <- copy(tick_data[, .(DT, PRICE, SIZE)])
setnames(ticks_, c("time", "price", "volume"))
rvp <- roll_volume_profile(ticks_, 60, step = 60, alpha = 0.9, cut = 3)
rvp$profile[[1]]
rvp$profile[[10]]

features_set <- features_hf(time_bars,
                            window_sizes = c(5, 22, 22 * 3, 22 * 6, 22 * 12),
                            quantile_divergence_window = c(22, 22*3, 22*12))
head(features_set[, 1:10])
cols_to_lag <- colnames(features_set)[7:ncol(features_set)]
features_set[, (cols_to_lag) := lapply(.SD, shift, n = 1L, type = "lag"), .SDcols = cols_to_lag]
head(features_set[, 1:10])

data_ <- time_bars[, .(SYMBOL, DT, VWPRICE)]
setnames(data_, c("symbol", "date", "close"))
arima_forecasts <- rolling_univariate_forecasts(data_, index_at_lag, c(200), 8L, forecast_type = "autoarima")
nnetar_forecasts <- rolling_univariate_forecasts(data_, index_at_lag, c(200), 8L, forecast_type = "nnetar")
exuber_features <- rolling_exuber(data_, index_at_lag, c(100, 200, 600), 14L, 4L)
backcusum_features <- rolling_backcusum(data_, index_at_lag, c(200), 4L)
gpd_features <- rolling_gpd(data_, index_at_lag, c(200), 8L, 0.04)
gas_features <- rolling_gas(data_, index_at_lag, c(100), 8L,
                            gas_dist = "sstd", gas_scaling = "Identity", prediction_horizont = 10)
catch22_features <- rolling_theft(data_, index_at_lag, c(22, 22 * 3, 22 * 12), 8L, "catch22")
feasts_features <- rolling_theft(data_, index_at_lag, c(22, 22 * 3, 22 * 12), 8L, "feasts")

# merge features
features <- Reduce(function(x, y) merge(x, y, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE),
                   list(arima_forecasts, nnetar_forecasts, exuber_features, gas_features,
                        backcusum_features, gpd_features, catch22_features,
                        feasts_features))
setnames(features, c("symbol", "date"), c("SYMBOL", "DT"))
features <- merge(features, features_set_lag, by = c("SYMBOL", "DT"), all.x = TRUE, all.y = FALSE)
features <- merge(labels, features, by = c("SYMBOL", "DT"), all.x = TRUE, all.y = FALSE)

# filter only succesfull outcomes derived from CAR calculation and remove cols with many NA
keep_cols <- names(which(colMeans(!is.na(features)) > 0.8))
clf_data <- features[, .SD, .SDcols = keep_cols]

# select relevant columns
feature_cols <- setdiff(colnames(clf_data), c("PRICE", "NUMTRADES", "SIZE", "VWPRICE", "ret", "trgt",
                                              "bin", "DT_features", "SYMBOL", "DT"))
clf_data <- clf_data[, .SD, .SDcols = c(feature_cols, "bin")]

# balance label
table(clf_data$bin)
clf_data <- clf_data[bin != 0]

#  winsorization (remove ooutliers)
clf_data <- na.omit(clf_data)
clf_data[, (feature_cols) := lapply(.SD, Winsorize, probs = c(0.03, 0.97), na.rm = TRUE), .SDcols = feature_cols] # winsorize across dates

# define task
clf_data[, bin := as.factor(bin)]
task = TaskClassif$new(id = "hft", backend = clf_data, target = 'bin')

# caorse filtering
task <- coarse_filtering(task, "spearman", 0.01)

# filter features
filtered_features <- auto_feature_filter(task, filter.nfeat = 4)

# selected features
# oldest_bmr_fi_file <- get_all_blob_files("mlr3models")
# oldest_bmr_fi_file$datetime <- as.POSIXct(gsub("-", " ", gsub(".*[a-z]+-|.rds", "", oldest_bmr_fi_file$name)), format = "%Y%m%d %H%M%S")
# oldest_bmr_fi_file_last <- oldest_bmr_fi_file[oldest_bmr_fi_file$datetime == max(oldest_bmr_fi_file$datetime), ]
bmr_fs = auto_feature_selection(task, save_to_blob = TRUE)
bmr_fs$score(msr("classif.acc"))

# extract important features
bmr_fs_dt <- as.data.table(bmr_fs)
important_features <- lapply(bmr_fs_dt$learner, function(x) unlist(x$fselect_result$features))
important_features <- as.data.table(table(unlist(important_features)))
important_features <- important_features[order(N, decreasing = TRUE), ]
important_features_n <- head(important_features[, V1], 20) # top n features selected through ML
important_features_most_important <- important_features[N == max(N, na.rm = TRUE), V1] # most importnatn feature(s)
features_short <- unique(c(important_features_most_important, intersect(important_features_n, filtered_features)))
features_long <- unique(c(important_features_most_important, union(important_features_n, filtered_features)))

# automl
task_short <- task$clone()
task_short <- task_short$select(features_short)
task_long <- task$clone()
task_long <- task_long$select(features_long)

# inspect short features
library(rpart.plot)
learner = lrn("classif.rpart", maxdepth = 3, predict_type = "prob")
learner$train(task_short)
predictins = learner$predict(task)
predictins$score(msr("classif.acc"))
learner$importance()
rpart_model <- learner$model
rpart.plot(rpart_model)

# AUTOML
bmr_results = AutoML(task_short)
train_indices = sample(1:task_long$nrow, 4/5*task_long$nrow)
bmr_results$train(row_ids = train_indices)

# train performance
bmr_results$tuned_params()
bmr_results$learner$archive$data
# print())bmr_results$tuned_params()$x_domain[[1]]$)

# prediction on test set
predict_indices = setdiff(1:task_short$nrow, train_indices)
predictions = bmr_results$predict(row_ids = predict_indices)
predictions$score(msr("classif.acc"))
predictions_dt <- as.data.table(predictions)
prediciotns_almostsure <- predictions_dt[prob.1 > 0.6]
mlr3measures::acc(prediciotns_almostsure$truth, prediciotns_almostsure$response)
predictions$confusion

# holdout predictions
# task_holdout <- create_task_pead(X_holdout, bin_type = "extreme")
# task_holdout$select(task_extreme_long$feature_names)
# holdout_predicitions <- bmr_results$predict(task_holdout)
# holdout_predicitions$score(msr("classif.acc"))
# prediciotns_almostsure_holdout <- as.data.table(holdout_predicitions)
# prediciotns_almostsure_holdout <- prediciotns_almostsure_holdout[prob.1 > 0.6]
# mlr3measures::acc(prediciotns_almostsure_holdout$truth, prediciotns_almostsure_holdout$response)

# save model
file_name <- file.path("D:/mlfin/mlr3_models",
                       paste0('automl-', task$id, "-", format.POSIXct(Sys.time(), "%Y%m%d-%H%M%S"), '.rds'))
saveRDS(bmr_results, file = file_name)
file.copy(file_name, "C:/Users/Mislav/Documents/GitHub/alphar/R/plumber_deploy/ml_model_hft.rds")

