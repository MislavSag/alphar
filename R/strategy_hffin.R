library(QuantTools)
library(highfrequency)
library(zip)
library(mlfinance)
library(ggplot2)
library(DescTools)
library(mlr3verse)
library(AUTOMLPhD)
library(mlr3automl)
library(future.apply)



# setup
save_path <- "D:/tick_data"
plan(multicore(workers = 8L))

# parameteres
bar_filters <- "backcusum" # backcusum or cusum
feature_selection = "new"  # new or old

# import data
symbol <- c("aapl", "tsla")
tick_data <- future_lapply(symbol, function(s) {
  files_import <- list.files(file.path(save_path, s), full.names = TRUE)
  tick_data <- lapply(files_import, function(x) fread(cmd = paste0('unzip -p ', x), col.names = c('DT', "PRICE", "SIZE")))
  tick_data <- rbindlist(tick_data)
  tick_data[, SYMBOL := s]
  tick_data
})
tick_data <- rbindlist(tick_data)

# clean raw tick data
tick_data <- tradesCleanup(tDataRaw = tick_data)
print(tick_data$report)
tick_data <- tick_data$tData

# time bars
time_bars <- lapply(tolower(symbol), function(s) aggregateTrades(tick_data[SYMBOL == s], alignBy = "minutes", alignPeriod = 1))
time_bars <- rbindlist(time_bars)
# volume_bars <- businessTimeAggregation(tData, measure = "volume", obs = 200)
# intensity_bars <- businessTimeAggregation(tData, measure = "intensity")
# vol_bars <- businessTimeAggregation(tData, measure = "vol") # ASKED QUESTION ON highfreqency ISSUES
# ohlcv <- makeOHLCV(tick_data, alignBy = "minutes", alignPeriod = 1)
# price <- time_bars[, .(DT, PRICE)]

# filter bars
if (bar_filters == "cusum") {

  # cusum bar filters
  daily_vol <- daily_volatility(price, 50)
  daily_vol_mean <- mean(daily_vol$Value, na.rm = TRUE)
  cusum_events <-  cusum_filter(price, daily_vol_mean * 3)
  vartical_barriers <- add_vertical_barrier(cusum_events, time_bars$DT, num_days = 5)

} else if (bar_filters == "backcusum") {
  # backcusum filters
  data_ <- time_bars[, .(SYMBOL, DT, VWPRICE)]
  setnames(data_, c("symbol", "date", "close"))
  backcusum_events_all <- rolling_backcusum(data_, windows = c(60 * 4), workers = 8L)
  backcusum_column <- "backcusum_rejections_1_240" # 0.1 0.05 0.01 0.001
  filtered_events <- backcusum_events_all[get(backcusum_column) == 1, .SD, .SDcols = c("symbol", "date")]
  vertical_barriers <- lapply(unique(filtered_events$symbol), function(x) {
    sample_ <- filtered_events[symbol == x]
    y <- add_vertical_barrier(sample_[, .(date)], time_bars[SYMBOL == x, DT])
    y$symbol <- x
    y
  })
  vertical_barriers <- rbindlist(vertical_barriers)
}

# plot CUSUM events
# filtered_events_index <- filtered_events[symbol == "aapl", .SD, .SDcols = "date"]
# setnames(filtered_events_index, "date", "DT")
# filtered_events_index[, index := TRUE]
# data_plot <- filtered_events_index[time_bars[, .(DT, PRICE)], on = "DT"]
# ggplot(data_plot, aes(x = DT, y = PRICE)) +
#   geom_line() +
#   geom_point(data = data_plot[index == TRUE], aes(x = DT, y = PRICE), color = "red")
# ggplot(data_plot[1:10000], aes(x = DT, y = PRICE)) +
#   geom_line() +
#   geom_point(data = data_plot[1:10000][index == TRUE], aes(x = DT, y = PRICE), color = "red")

# labeling
min_return <- 0.0001
pt_sl <- c(1, 1)
events <- lapply(symbol, function(s) {
  sample_ <- time_bars[SYMBOL == s, .(DT, PRICE)]
  filtered_events_ <- filtered_events[symbol == s, .SD, .SDcols = "date"]
  daily_vol <- cbind(sample_[, .(Datetime = DT)], Value = 0.003)
  vertical_barriers_ <- vertical_barriers[symbol == s, .(Datetime, t1)]
  events <- get_events(price = sample_,                           # close data.table with index and value
                       t_events = filtered_events_,               # bars to look at for trades
                       pt_sl = pt_sl,                             # multiple target argument to get width of the barriers
                       target = daily_vol,                        # values that are used to determine the width of the barrier
                       min_ret = min_return,                      # minimum return between events
                       vertical_barrier_times=vertical_barriers_,  # vartical barriers timestamps
                       side_prediction=NA)                        # prediction from primary model (in this case no primary model)
  return(events)
})
labels <- lapply(1:length(events), function(i) {
  y <- get_bins(events[[i]], time_bars[SYMBOL == symbol[i], .(DT, PRICE)])
  y <- drop_labels(y, 0.05)
  y$SYMBOL <- symbol[i]
  y
})
labels <- rbindlist(labels)
setnames(labels, c("Datetime"), c("DT"))
labels <- labels[, .SD, .SDcols = c("SYMBOL", "DT", "ret", "trgt", "bin")]
table(labels$bin)
setorderv(labels, c("SYMBOL", "DT"))

# plot triple barrier
events_ <- events[[1]]
sample_index <- sample(1:nrow(events_), 4)
gs <- list()
for (i in seq_along(sample_index)) {
  h_lines <- time_bars[SYMBOL == "aapl" & DT %between% c(events_[sample_index[i]]$t0, events_[sample_index[i]]$t1)]
  h_lines <- h_lines[, pt := head(PRICE, 1) + (head(PRICE, 1) * events_[sample_index[i]]$trgt)]
  h_lines <- h_lines[, sl := head(PRICE, 1) - (head(PRICE, 1) * (events_[sample_index[i]]$trgt))]
  gs[[i]] <- ggplot(h_lines, aes(x = DT, y = PRICE)) +
    geom_line() +
    geom_point(data = h_lines[DT == events_[sample_index[i]]$t0], aes(x = DT, y = PRICE), color = "red", size = 3) +
    geom_line(data = h_lines, aes(x = DT, y = sl), color = "red") +
    geom_line(data = h_lines, aes(x = DT, y = pt), color = "green")
}
patchwork::wrap_plots(gs)

# index for generating features
labels_ <- labels[, .(SYMBOL, DT)]
labels_$index <- TRUE
filter_index <- labels_[time_bars[, .(SYMBOL, DT)], on = c("SYMBOL", "DT")]
index_at <- which(filter_index$index == TRUE)
index_at_lag <- c(index_at - 1)

# generate features
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
features <- merge(features, features_set, by = c("SYMBOL", "DT"), all.x = TRUE, all.y = FALSE)
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
if (feature_selection == "new") {
  bmr_fs = auto_feature_selection(task, save_to_blob = TRUE)
}
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

# AUTOML
bmr_results = AutoML(task_long)
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
symbol_holdout <- c("aal")
tick_data_holdout <- future_lapply(symbol, function(s) {
  files_import <- list.files(file.path(save_path, s), full.names = TRUE)
  tick_data <- lapply(files_import, function(x) fread(cmd = paste0('unzip -p ', x), col.names = c('DT', "PRICE", "SIZE")))
  tick_data <- rbindlist(tick_data)
  tick_data[, SYMBOL := s]
  tick_data
})
tick_data_holdout <- rbindlist(tick_data_holdout)
tick_data_holdout <- tradesCleanup(tDataRaw = tick_data_holdout)
tick_data_holdout <- tick_data_holdout$tData
time_bars <- lapply(tolower(symbol), function(s) aggregateTrades(tick_data_holdout[SYMBOL == s], alignBy = "minutes", alignPeriod = 1))
time_bars <- rbindlist(time_bars)
data_holdout <- time_bars[, .(SYMBOL, DT, VWPRICE)]
setnames(data_holdout, c("symbol", "date", "close"))
backcusum_events_holdout <- rolling_backcusum(data_holdout, windows = c(60 * 4), workers = 8L)
filtered_events_holdout <- backcusum_events_holdout[get(backcusum_column) == 1, .SD, .SDcols = c("symbol", "date")]
vertical_barriers_holdout <- lapply(unique(filtered_events_holdout$symbol), function(x) {
  sample_ <- filtered_events_holdout[symbol == x]
  y <- add_vertical_barrier(sample_[, .(date)], time_bars[SYMBOL == x, DT])
  y$symbol <- x
  y
})
vertical_barriers_holdout <- rbindlist(vertical_barriers_holdout)


task_holdout <- create_task_pead(X_holdout, bin_type = "extreme")
# task_holdout$select(task_extreme_long$feature_names)
# holdout_predicitions <- bmr_results$predict(task_holdout)
# holdout_predicitions$score(msr("classif.acc"))
# prediciotns_almostsure_holdout <- as.data.table(holdout_predicitions)
# prediciotns_almostsure_holdout <- prediciotns_almostsure_holdout[prob.1 > 0.6]
# mlr3measures::acc(prediciotns_almostsure_holdout$truth, prediciotns_almostsure_holdout$response)



# ASK QUESTION ON STACK ---------------------------------------------------

# library(QuantTools)
# library(highfrequency)
#
# # import data and cleaning
# ticks <- get_finam_data("AAPL", as.Date("2021-09-01"), as.Date("2021-09-02"), "tick")
# setnames(ticks, c("DT", "PRICE", "SIZE"))
# ticks$SYMBOL <- "AAPL"
# ticks_cleaned <- tradesCleanup(tDataRaw = ticks)
# ticks_cleaned <- ticks_cleaned$tData
#
# # aggreagtion
# vol_bars <- businessTimeAggregation(ticks_cleaned, measure = "vol")
