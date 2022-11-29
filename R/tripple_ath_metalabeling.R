library(data.table)
library(dtts)
library(nanotime)
library(AzureStor)
library(ggplot2)
library(PerformanceAnalytics)
library(finfeatures)
library(highfrequency)
library(mlr3verse)
library(reticulate)
# python packages
reticulate::use_python("C:/ProgramData/Anaconda3/envs/mlfinlabenv/python.exe", required = TRUE)
mlfinlab = reticulate::import("mlfinlab", convert = FALSE)
pd = reticulate::import("pandas", convert = FALSE)
builtins = import_builtins(convert = FALSE)
main = import_main(convert = FALSE)


# set up
ENDPOINT = storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), key=Sys.getenv("BLOB-KEY"))
CONT = storage_container(ENDPOINT, "equity-usa-hour-fmpcloud-adjusted")

# parameters
start_holdout_date <- as.Date("2021-06-01")


# UTILS -------------------------------------------------------------------

check_dates <- function(x) {
  min_ <- min(format.POSIXct(as.POSIXct(rownames(x), tz = "EST"), format = "%H:%M:%S"), na.rm = TRUE)
  max_ <- max(format.POSIXct(as.POSIXct(rownames(x), tz = "EST"), format = "%H:%M:%S"), na.rm = TRUE)
  print(paste("Min ", min_, "Max", max_))
}


# IMPORT DATA -------------------------------------------------------------
# import universe
sp100 <- fread("D:/universum/sp-100-index-03-11-2022.csv")

# import data
azure_blobs <- list_blobs(CONT)
market_data_list <- lapply(azure_blobs$name, function(x) {
  print(x)
  y <- tryCatch(storage_read_csv2(CONT, x), error = function(e) NA)
  if (is.null(y) | all(is.na(y))) return(NULL)
  y <- cbind(symbol = x, y)
  return(y)
})
market_data <- rbindlist(market_data_list)
market_data[, symbol := toupper(gsub("\\.csv", "", symbol))]
keep_symbols <- market_data[, .N, by = symbol][N > 600, symbol]
market_data <- market_data[symbol %in% keep_symbols]
market_data[, N := 0]
setorderv(market_data, c('symbol', 'datetime'))

# upsample
# market_data_sample <- copy(market_data)
# market_data_sample[, datetime := as.nanotime(datetime)]
# market_data_sample <- setkey(market_data_sample, symbol, datetime)
# grid.align(market_data_sample, as.nanoduration("00:04:00"), func=colMeans)
# market_data[, hours_4 := ifelse(format.POSIXct(datetime, format = "%H%M%S") %between% c("00:00:00", "12:00:00"), 1, 2)]
# market_data <- market_data[, `:=`(
#   open = head(open, 1),
#   high = max(high, na.rm = TRUE),
#   low = min(low, na.rm = TRUE),
#   close = tail(close, 1),
#   volume = sum(volume, nna.rm = TRUE)
# ), by = c("symbol", "")]

# clean data
market_data[, returns := close / shift(close) - 1, by = "symbol"]
market_data <- na.omit(market_data)
market_data$datetime <- as.POSIXct(as.numeric(market_data$datetime),
                                   origin=as.POSIXct("1970-01-01", tz="EST"),
                                   tz="EST")
market_data <- market_data[format.POSIXct(datetime, format = "%H:%M:%S") %between% c("09:30:00", "16:00:00")]
market_data <- unique(market_data, by = c("symbol", "datetime"))
spy <- market_data[symbol == "SPY", .(datetime, close, returns)]

# sample
sp100 <- fread("D:/universum/sp-100-index-03-11-2022.csv")
tickers <- sp100$Symbol
# tickers = c("COST", "LMT", "PGR", "SPG", "AAPL", "V", "MCO")
DT <- market_data[symbol %in% c(tickers)]

# monthly ATH
DT[, monthly_ath := shift(frollapply(high, 22 * 2, max), 1), by = "symbol"]
DT[, signal := ifelse(close > monthly_ath, 1, 0)]
DT[, signal_cumsum := roll::roll_sum(signal, 8 * 5), by = "symbol"]

# visualize
# sample_ <- DT[symbol == "AAPL"]
# ggplot(sample_, aes(x = datetime)) +
#   geom_line(aes(y = close)) +
#   geom_point(aes(y = close), data = sample_[signal == 1], color = "red", size = 0.1)
# ggplot(sample_[datetime %between% c("2020-01-01", "2022-01-01")], aes(x = datetime)) +
#   geom_line(aes(y = close)) +
#   geom_point(aes(y = close), data = sample_[signal == 1 & datetime %between% c("2020-01-01", "2022-01-01")],
#              color = "red", size = 0.9)
# ggplot(sample_[datetime %between% c("2020-01-01", "2022-01-01")], aes(x = datetime)) +
#   geom_line(aes(y = close)) +
#   geom_point(aes(y = close), data = sample_[signal == 1 & datetime %between% c("2020-01-01", "2022-01-01")],
#              color = "red", size = 0.9) +
#   geom_line(aes(y = signal_cumsum))

# backtst function
backtest <- function(returns, indicator, threshold, return_cumulative = TRUE) {
  sides <- vector("integer", length(indicator))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(indicator[i-1]) || is.na(indicator[i-2])) {
      sides[i] <- NA
    } else if ((indicator[i-1] > indicator[i-2]) & (indicator[i-1] > indicator[i-5])) {
      sides[i] <- 0
    } else {
      sides[i] <- 1
    }
  }
  sides <- ifelse(is.na(sides), 1, sides)
  returns_strategy <- returns * sides
  if (return_cumulative) {
    return(PerformanceAnalytics::Return.cumulative(returns_strategy))
  } else {
    return(returns_strategy)
  }
}

# backtest
strategy_returns <- backtest(sample_$returns, sample_$signal_cumsum, 1, FALSE)
charts.PerformanceSummary(xts(cbind(sample_$returns, strategy_returns), order.by = sample_$datetime))
# Performance(xts(strategy_returns, order.by = backtest_data$datetime))
# Performance(xts(backtest_data$returns, order.by = backtest_data$datetime))





# LABELING ----------------------------------------------------------------
# metalableing
symbols_ <- unique(DT$symbol)
tb_lists <- list()
for (i in seq_along(symbols_)) {

  # cuurent symbol
  s <- symbols_[[i]]

  # debugging
  print(s)

  # sample data only for one symbol
  sample_ <- DT[symbol == s]

  # close seires for python packages
  close_series <- reticulate::r_to_py(sample_[, .(datetime, close)])
  close_series <- close_series$set_index(close_series$columns[0])
  close_series$index = pd$to_datetime(close_series$index)
  close_series = close_series$squeeze()
  close_series = close_series$tz_localize(tz='UTC')
  close_series = close_series$tz_convert(tz='EST')
  check_dates(py_to_r(close_series))

  # filter events
  events_ <- DT[signal == 1, .SD, .SDcols = c("symbol", "datetime")]
  events_ = events_[symbol == s, .(datetime)]
  events_ = reticulate::r_to_py(events_)
  events_ = events_$set_index(events_$columns[0])
  events_ = pd$Series(index = events_$index)
  events_ = events_$tz_localize(tz='UTC')
  events_ = events_$tz_convert(tz='EST')
  events_ = events_$index
  # events_ = pd$Series(events_$index$values, index=events_$index)

  # # Compute volatility
  daily_vol = mlfinlab$util$get_daily_vol(close = close_series)

  # # Apply Symmetric CUSUM Filter and get timestamps for events
  # cusum_events = mlfinlab$filters$cusum_filter(close_series, r_to_py(daily_vol)) # OVO U PRETHODNI KORAK
  # filtered_events_py = reticulate::r_to_py(filtered_events_)                          # OVO U PRETHODNI KORAK

  # Compute vertical barrier
  vertical_barriers = mlfinlab$labeling$add_vertical_barrier(
    t_events=events_,
    close=close_series,
    num_days=30)

  # predictions for meta labeling
  preds <- pd$concat(list(pd$Series(close_series$index), side = pd$Series(rep(1, py_to_r(close_series$size)))), axis = 1)

  # tripple barier events
  triple_barrier_events = mlfinlab$labeling$get_events(
    close=close_series,
    t_events=pd$Series(events_), # events
    pt_sl=r_to_py(c(1L, 1L)),
    target=daily_vol$multiply(5L),
    min_ret=r_to_py(FALSE),
    num_threads=r_to_py(1L),
    vertical_barrier_times=r_to_py(FALSE)# vertical_barriers
    # side_prediction = preds
    )

  # labels
  labels = mlfinlab$labeling$get_bins(triple_barrier_events, close_series)
  labels = mlfinlab$labeling$drop_labels(labels, 0.05)
  labels_summary <- py_to_r(labels)
  table(labels_summary$bin)

  # tripple barrier info
  triple_barrier_info = pd$concat(c(triple_barrier_events$t1, labels), axis=1)
  triple_barrier_info$dropna(inplace=TRUE)
  # triple_barrier_info$t1$tz_convert('US/Eastern')
  # triple_barrier_info$index$tz_convert('US/Eastern')
  triple_barrier_info <- py_to_r(triple_barrier_info)
  attr(triple_barrier_info$t1, "tzone") <- "UTC"
  attr(triple_barrier_info$t1, "tzone") <- "EST"
  triple_barrier_info$datetime <- as.POSIXct(rownames(triple_barrier_info), tz = "EST")
  rownames(triple_barrier_info) <- NULL
  triple_barrier_info <- as.data.table(triple_barrier_info)
  head(triple_barrier_info, 100)

  # DEBUG
  # min(format.POSIXct(triple_barrier_info$datetime, format = "%H:%M:%S"))
  # max(format.POSIXct(triple_barrier_info$datetime, format = "%H:%M:%S"))

  # merge market data and labels
  result <- merge(triple_barrier_info, sample_, by = "datetime", all.x = TRUE, all.y = FALSE)

  tb_lists[[i]] <- result
}
names(tb_lists) <- symbols_
labels_dt <- rbindlist(tb_lists, idcol = "symbol")
summary(labels_dt$ret)
table(labels_dt$bin)
sum(labels_dt$bin == 1) / nrow(labels_dt)
# labels$symbol <- NULL # TODO FIX THIS ABOVE
setnames(labels_dt, "datetime", "date")  # TODO FIX THIS ABOVE

# plot triple barrier
sample_index <- sample(1:nrow(labels), 4)
gs <- list()
for (i in seq_along(sample_index)) {
  meta_ <- labels_dt[sample_index[i]]
  h_lines <- DT[symbol == meta_$symbol & datetime %between% c(meta_[, date], meta_[, t1])]
  h_lines <- h_lines[, pt := head(close, 1) + (head(close, 1) * meta_$trgt)]
  h_lines <- h_lines[, sl := head(close, 1) - (head(close, 1) * meta_$trgt)]
  gs[[i]] <- ggplot(h_lines, aes(x = datetime, y = close)) +
    geom_line() +
    geom_point(data = h_lines[datetime == meta_$date], aes(x = datetime, y = close), color = "red", size = 3) +
    geom_point(data = h_lines[datetime == meta_$t1], aes(x = datetime, y = close), color = "red", size = 3) +
    geom_line(data = h_lines, aes(x = datetime, y = sl), color = "red") +
    geom_line(data = h_lines, aes(x = datetime, y = pt), color = "green")
}
patchwork::wrap_plots(gs)



# FIXES HORIZONT LABELING -------------------------------------------------

# calculate future returns
DT[, ret_8 := shift(close, -7L, "shift") / shift(close, -1L, "shift") - 1, by = "symbol"]
DT[, ret_40 := shift(close, -39L, "shift") / shift(close, -1L, "shift") - 1, by = "symbol"]
DT[, ret_176 := shift(close, -175L, "shift") / shift(close, -1L, "shift") - 1, by = "symbol"]



# FEATURES ----------------------------------------------------------------

# index for generating features
at_ <- which(DT$signal == 1)
length(at_)
lag_ = 0

# make OHLCV instance
OhlcvInstance = Ohlcv$new(DT[, c(1:7, 13:15)], date_col = "datetime")

# Features from OHLLCV
print("Calculate Ohlcv features.")
OhlcvFeaturesInit = OhlcvFeatures$new(windows = c(15, 30, 60, 60 * 4, 60 * 8, 60 * 4 * 5),
                                      quantile_divergence_window =  c(22, 22*3, 22*6, 22*12))
OhlcvFeaturesSet = OhlcvFeaturesInit$get_ohlcv_features(OhlcvInstance)
cols_keep <- colnames(OhlcvFeaturesSet)[grep("changes_", colnames(OhlcvFeaturesSet))]
cols_keep <- setdiff(colnames(OhlcvFeaturesSet), cols_keep)
OhlcvFeaturesSet = OhlcvFeaturesSet[, ..cols_keep] # REMOVE THIS IN FINFEATURES PACKAGE
OhlcvFeaturesSet_sample <- OhlcvFeaturesSet[at_ - lag_]
# DEBUGGING
# head(OhlcvFeaturesSet_sample[symbol == "AAPL", 1:5], 10) # FOR TEST, DELTE LATER+
# head(DT[symbol == "AAPL" & signal == 1, 1:5], 1000) # FOR TEST, DELTE LATER+
# tail(OhlcvFeaturesSet_sample[symbol == "AAPL", 1:5], 10) # FOR TEST, DELTE LATER+
# tail(ohlcv_meta[symbol == "AAPL", 1:5], 1000) # FOR TEST, DELTE LATER+

# BidAsk features
RollingBidAskInstance <- RollingBidAsk$new(c(30, 60), 8L, at = at_, lag = lag_, na_pad = TRUE, simplify = FALSE)
RollingBidAskFeatures = RollingBidAskInstance$get_rolling_features(OhlcvInstance)
########### DEBUGGING ###########
# head(RollingBidAskFeatures[symbol == "AAPL", 1:5], 10) # FOR TEST, DELTE LATER
# head(ohlcv_meta[symbol == "AAPL", 1:5], 10) # FOR TEST, DELTE LATER+
# tail(RollingBidAskFeatures[symbol == "AAPL", 1:5], 10) # FOR TEST, DELTE LATER
# tail(ohlcv_meta[symbol == "AAPL", 1:5], 10) # FOR TEST, DELTE LATER+
########### DEBUGGING ###########
RollingBackcusumInit = RollingBackcusum$new(c(60 * 8), 8L, at = at_,lag = lag_, na_pad = TRUE, simplify = FALSE)
RollingBackCusumFeatures = RollingBackcusumInit$get_rolling_features(OhlcvInstance)
RollingExuberInit = RollingExuber$new(c(200, 600), 8L, at = at_, lag = lag_, na_pad = TRUE, simplify = FALSE, exuber_lag = 1L)
RollingExuberFeatures = RollingExuberInit$get_rolling_features(OhlcvInstance)
gc()
RollingForecatsInstance = RollingForecats$new(c(200), 8L, lag = lag_, at = at_, na_pad = TRUE, simplify = FALSE,
                                              forecast_type = "autoarima", h = 22)
RollingForecatsAutoarimaFeatures = RollingForecatsInstance$get_rolling_features(OhlcvInstance)
gc()
##################### COMP INTENSIVE #####################
# RollingForecatsInstance = RollingForecats$new(c(200), 10L, lag = lag_, at = at_, na_pad = TRUE, simplify = FALSE,
#                                               forecast_type = "nnetar", h = 22)
# RollingForecatNnetarFeatures = RollingForecatsInstance$get_rolling_features(OhlcvInstance)
# gc()
# RollingGasInit = RollingGas$new(c(60 * 8), 10L, at = at_, lag = lag_, na_pad = TRUE, simplify = FALSE,
#                                 gas_dist = "sstd", gas_scaling = "Identity", prediction_horizont = 10)
# RollingGasFeatures = RollingGasInit$get_rolling_features(OhlcvInstance)
# gc()
# RollingGpdInit = RollingGpd$new(c(60 * 8), 10L, at = at_, lag = lag_, na_pad = TRUE, simplify = FALSE, threshold = 0.05)
# RollingGpdFeatures = RollingGpdInit$get_rolling_features(OhlcvInstance)
# gc()
##################### COMP INTENSIVE #####################
# CHECK !
# c(60, 60 * 4, 60 * 8, 60 * 8 * 5) THIS WINDOW VECTOR DOESNT WORK
RollingTheftInit = RollingTheft$new(c(60 * 4, 60 * 8), workers = 8L, at = at_, lag = lag_, na_pad = TRUE, simplify = FALSE,
                                    features_set = "catch22")
RollingTheftCatch22Features = RollingTheftInit$get_rolling_features(OhlcvInstance)
gc()
RollingTheftInit = RollingTheft$new(c(60 * 6, 60 * 8 * 5), 8L, at = at_, lag = lag_, na_pad = TRUE, simplify = FALSE,
                                    features_set = "feasts")
RollingTheftFeastsFatures = RollingTheftInit$get_rolling_features(OhlcvInstance)
gc()
RollingTheftInit = RollingTheft$new(windows = c(60 * 4), workers = 1L, at = at_, lag = lag_, na_pad = TRUE, simplify = FALSE,
                                    features_set = "tsfel")
RollingTheftTsfelFeatures = suppressMessages(RollingTheftInit$get_rolling_features(OhlcvInstance))
RollingTsfeaturesInit = RollingTsfeatures$new(windows = 60 * 4, workers = 8L, at = at_, lag = lag_, na_pad = TRUE, simplify = FALSE)
RollingTsfeaturesFeatures = RollingTsfeaturesInit$get_rolling_features(OhlcvInstance)
# doesn't work
# RollingUfriskInit = RollingUfrisk$new(windows = 200, workers = 1L, at = c(303:310, 500:510), lag = lag_, na_pad = TRUE, simplify = FALSE)
# RollingUfriskFeatures = RollingUfriskInit$get_rolling_features(OhlcvInstance)

# merge all features
features <- Reduce(function(x, y) merge(x, y, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE),
                   list(OhlcvFeaturesSet_sample, RollingBidAskFeatures, RollingBackCusumFeatures,
                        RollingExuberFeatures, RollingForecatsAutoarimaFeatures,
                        RollingTheftCatch22Features,
                        RollingTheftTsfelFeatures))

features <- Reduce(function(x, y) merge(x, y, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE),
                   list(OhlcvFeaturesSet_sample, RollingBidAskFeatures, RollingBackCusumFeatures,
                        RollingExuberFeatures, RollingForecatsAutoarimaFeatures,
                        RollingTheftFeastsFatures, RollingTheftTsfelFeatures,
                        RollingTsfeaturesFeatures))

# save features
# setorderv(features, c("symbol", "date"))
# print("Saving features with fundamentals to blob.")
# time_ <- format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
# fwrite(features, paste0("D:/mlfin/mlr3_models/ATH-features-", time_, ".csv"))

# merge features and ohcv with bins
list.files("D:/mlfin/mlr3_models")
features <- fread("D:/mlfin/mlr3_models/ATH-features-20220317120311.csv")
features <- as.data.table(features)
attr(features$date, "tzone") <- "EST"


# join meta labels to features
features[, date_join_features := date]
labels_dt[, date_join_ohlcv_meta := date]
cols_features_keep <- c("symbol", "date", "date_join_features", "ret_8", "ret_40", "ret_176",
                        colnames(features)[which(colnames(features) == "close_ath"):ncol(features)])
clf_data <- features[, ..cols_features_keep][labels_dt, on = c("symbol", "date"), roll = Inf]
clf_data[, .(symbol, date, date_join_features, date_join_ohlcv_meta)]
all(clf_data$date_join_ohlcv_meta == clf_data$date_join_features) # dates the same!

# # join fixed horizon labels to features
# clf_data <- merge(clf_data, ohlcv_filters[, .(symbol, datetime, ret_30, ret_60, ret_240, ret_480)],
#                   by.x = c("symbol", "date"), by.y = c("symbol", "datetime"),
#                   all.x = TRUE, all.y = FALSE)

# remove columns with many NA
keep_cols <- names(which(colMeans(!is.na(clf_data)) > 0.85))
print(paste0("Removing columns with many NA values: ", setdiff(colnames(clf_data), keep_cols)))
clf_data <- clf_data[, .SD, .SDcols = keep_cols]

# remove Inf and Nan values if they exists
is.infinite.data.frame <- function(x) do.call(cbind, lapply(x, is.infinite))
keep_cols <- names(which(colMeans(!is.infinite(as.data.frame(clf_data))) > 0.99))
print(paste0("Removing columns with Inf values: ", setdiff(colnames(clf_data), keep_cols)))
clf_data <- clf_data[, .SD, .SDcols = keep_cols]

# remove rows with Inf values
clf_data <- clf_data[is.finite(rowSums(clf_data[, .SD, .SDcols = is.numeric], na.rm = TRUE))]
nrow(features)
nrow(clf_data)

# define feature columns
feature_cols <- setdiff(colnames(clf_data)[which(colnames(clf_data) %in% cols_features_keep)],
                        c("symbol", "date", "date_join_features", "ret_8", "ret_40", "ret_176"))
# clf_data <- clf_data[, .SD, .SDcols = c(feature_cols, "bin")]

# remove NA values
clf_data <- na.omit(clf_data, cols = feature_cols)

# remove constant columns
features_ <- clf_data[, ..feature_cols]
remove_cols <- colnames(features_)[apply(features_, 2, var, na.rm=TRUE) == 0]
print(paste0("Removing feature with 0 standard deviation: ", remove_cols))
feature_cols <- setdiff(feature_cols, remove_cols)

# remove highly correlated features
features_ <- clf_data[, ..feature_cols]
cor_matrix <- cor(features_)
cor_matrix_rm <- cor_matrix                  # Modify correlation matrix
cor_matrix_rm[upper.tri(cor_matrix_rm)] <- 0
diag(cor_matrix_rm) <- 0
remove_cols <- colnames(features_)[apply(cor_matrix_rm, 2, function(x) any(x > 0.98))]
print(paste0("Removing highly correlated featue (> 0.98): ", remove_cols))
feature_cols <- setdiff(feature_cols, remove_cols)

# convert logical to integer
chr_to_num_cols <- setdiff(colnames(clf_data[, .SD, .SDcols = is.character]), c("symbol", "time", "right_time"))
clf_data <- clf_data[, (chr_to_num_cols) := lapply(.SD, as.numeric), .SDcols = chr_to_num_cols]


# TODO ADD THIS INSIDE MLR3 GRAPH
clf_data[, y := data.table::year(as.Date(date))]
cols <- c(feature_cols, "y")
library(DescTools)
clf_data[, (cols) := lapply(.SD, function(x) {Winsorize(as.numeric(x), probs = c(0.01, 0.99), na.rm = TRUE)}), by = "y", .SDcols = cols]
clf_data[, y := NULL]

# remove constant columns
features_ <- clf_data[, ..feature_cols]
remove_cols <- colnames(features_)[apply(features_, 2, var, na.rm=TRUE) == 0]
table(features_$TSFEL_0_Histogram_7_240)
print(paste0("Removing feature with 0 standard deviation: ", remove_cols))
feature_cols <- setdiff(feature_cols, remove_cols)

# remove missing values
colnames(clf_data)[which(as.vector(colSums(is.na(clf_data)) > 20))]
clf_data[, i.symbol := NULL]
clf_data <- na.omit(clf_data)


# FEATURE SLECTION --------------------------------------------------------
# define feature matrix
cols_keep <- c(feature_cols, "ret_176")
X <- clf_data[, ..cols_keep]
X <- na.omit(X)
X <- as.matrix(X)
dim(X)

# f1st
library(gausscov)
f1st_fi <- f1st(X[, ncol(X)], X[, -ncol(X)], kmn = 20, sub = TRUE)
f1st_fi[[1]]
colnames(X)[200]
cov_index_f1st <- colnames(X[, -ncol(X)])[f1st_fi[[1]][, 1]]

# f3st_1
f3st_1 <- f3st(X[, ncol(X)], X[, -ncol(X)], m = 1, kexmx = 150)
cov_index_f3st_1 <- unique(as.integer(f3st_1[[1]][1, ]))[-1]
cov_index_f3st_1 <- cov_index_f3st_1[cov_index_f3st_1 != 0]
cov_index_f3st_1 <- colnames(X[, -ncol(X)])[cov_index_f3st_1]

# interesection of all important vars
most_important_vars <- intersect(cov_index_f1st, cov_index_f3st_1)
important_vars <- c(cov_index_f1st, cov_index_f3st_1)



# DEFINE TASK -------------------------------------------------------------
# separte train/tet and holdout
holdout_ids <- which(as.Date(clf_data$date) > start_holdout_date)
X_model <- clf_data[-holdout_ids, ]
X_holdout <- clf_data[holdout_ids, ] # TODO SAVE THIS FOR QUANTCONNECT BACKTESTING

# select only labels and features
labels <- c("bin", "ret_40")
X_model <- X_model[, .SD, .SDcols = c("symbol", "date", feature_cols, labels)]
X_holdout <- X_holdout[, .SD, .SDcols = c("symbol", "date", feature_cols, labels)]

# task for classification
X_model[, bin := as.factor(bin)]
X_holdout[, bin := as.factor(bin)]
task_classif = as_task_classif(X_model[, .SD, .SDcols = !c("symbol","date", labels[2])], id = "hft", target = 'bin')
task_classif_holdout = as_task_classif(X_holdout[, .SD, .SDcols = !c("symbol","date", labels[2])], id = "hft", target = 'bin')

# task for regression
task_reg <- as_task_regr(X_model[, .SD, .SDcols = !c("symbol","date", labels[1])], id = "hft_reg", target = 'ret_40')
task_reg_holdout <- as_task_regr(X_holdout[, .SD, .SDcols = !c("symbol","date", labels[1])], id = "hft_reg", target = 'ret_40')


# FEATURE SELECTION (TEST) ------------------------------------------------
# select features
vars_ <- most_important_vars
task_classif$select(vars_)
task_classif_holdout$select(vars_)
task_reg$select(vars_)
task_reg_holdout$select(vars_)

# rpart tree classificatoin
library(rpart.plot)
learner = lrn("classif.rpart", maxdepth = 3, predict_type = "prob", cp = 0.001)
task_ <- task_classif$clone()
learner$train(task_)
predictins = learner$predict(task_)
predictins$score(c(msr("classif.acc")))
learner$importance()
rpart_model <- learner$model
rpart.plot(rpart_model)



# ranger
library(DALEX)
library(DALEXtra)
learner = lrn("classif.ranger")
learner$predict_sets = c("train", "test")
learner$predict_type = "prob"
learner$param_set
learner$param_set$values$max.depth = 4
learner$param_set$values$num.trees = 3000
learner$param_set$values$num.threads = 2L
learner$train(task_classif)
preds = learner$predict(task_classif)
preds$confusion
preds$score(msrs(c("classif.fbeta", "classif.acc")))

# DALEX
ranger_exp = explain_mlr3(learner, task_classif$data(), as.numeric(as.character(task_classif$data()$bin)),
                          label    = "Ranger RF",
                          colorize = FALSE)
ranger_vi = model_parts(ranger_exp)
plot(ranger_vi, max_vars = 12, show_boxplots = FALSE)

# Partial Dependence Plots
selected_variables = ranger_vi[order(ranger_vi$dropout_loss, decreasing = TRUE), "variable"][1:4]
ranger_pd = model_profile(ranger_exp, variables = selected_variables)$agr_profiles
plot(ranger_pd)

# holdout prediction
preds_holdout <- learner$predict(task_classif_holdout)
preds_holdout$confusion
preds_holdout$score(msrs(c("classif.acc", "classif.recall", "classif.precision")))
prediciotns_extreme_holdout <- as.data.table(preds_holdout)
prediciotns_extreme_holdout <- prediciotns_extreme_holdout[prob.1 > 0.65]
nrow(prediciotns_extreme_holdout)
mlr3measures::acc(prediciotns_extreme_holdout$truth, prediciotns_extreme_holdout$response)




# INDIVIDUAL TUNERS -----------------------------------------------------
# ranger
learner_ranger = lrn("classif.bart", predict_type = "prob", id = "bart")
learner_ranger$param_set
search_space = ps(
  ntree = p_int(200, 1000)
  # k = p_dbl(2, 6)
  # k = p_int(2, 6),
)
at_ranger = auto_tuner(
  method = "random_search",
  learner = learner_ranger,
  resampling = rsmp("cv", folds = 3),
  measure = msr("classif.acc"),
  search_space = search_space,
  term_evals = 5
)
at_ranger$train(task_classif)

# inspect results
archive <- as.data.table(at_ranger$archive)
archive$classif.acc
length(at_ranger$state)
ggplot(archive[, mean(classif.acc), by = "x_domain_ntree"], aes(x = x_domain_ntree, y = V1)) + geom_line()
ggplot(archive[, mean(classif.acc), by = "prep_branch.selection"], aes(x = prep_branch.selection, y = V1)) + geom_bar(stat = "identity")
preds = at_ranger$predict(task_classif)
preds$confusion
preds$score(msr("classif.acc"))

# holdout
preds_holdout <- at_ranger$predict(task_classif_holdout)
preds_holdout$confusion
autoplot(preds_holdout, type = "roc")
preds_holdout$score(msrs(c("classif.acc"))) # , "classif.recall", "classif.precision"
prediciotns_extreme_holdout <- as.data.table(preds_holdout)
prediciotns_extreme_holdout <- prediciotns_extreme_holdout[prob.1 > 0.65]
nrow(prediciotns_extreme_holdout)
mlr3measures::acc(prediciotns_extreme_holdout$truth, prediciotns_extreme_holdout$response)
sum(task_classif$data()$bin == 1) / task_classif$nrow




# CLASSIFICATION AUTOML ---------------------------------------------------
# learners
learners_l = list(
  ranger = lrn("classif.ranger", predict_type = "prob", id = "ranger"),
  log_reg = lrn("classif.log_reg", predict_type = "prob", id = "log_reg"),
  kknn = lrn("classif.kknn", predict_type = "prob", id = "kknn"),
  # extratrees = lrn("classif.extratrees", predict_type = "prob", id = "extratrees"),
  cv_glmnet = lrn("classif.cv_glmnet", predict_type = "prob", id = "cv_glmnet"),
  xgboost = lrn("classif.xgboost", predict_type = "prob", id = "xgboost")
)
# create graph from list of learners
choices = c("ranger", "log_reg", "kknn", "cv_glmnet", "xgboost") # , "log_reg"
learners = po("branch", choices, id = "branch_learners") %>>%
  gunion(learners_l) %>>%
  po("unbranch", choices, id = "unbranch_learners")

# create complete grapg
graph = po("removeconstants", ratio = 0.05) %>>%
  # modelmatrix
  # po("branch", options = c("nop_filter", "modelmatrix"), id = "interaction_branch") %>>%
  # gunion(list(po("nop", id = "nop_filter"), po("modelmatrix", formula = ~ . ^ 2))) %>>%
  # po("unbranch", id = "interaction_unbranch") %>>%
  # scaling
  po("branch", options = c("nop_prep", "yeojohnson", "pca", "ica"), id = "prep_branch") %>>%
  gunion(list(po("nop", id = "nop_prep"), po("yeojohnson"), po("pca", scale. = TRUE), po("ica"))) %>>%
  po("unbranch", id = "prep_unbranch") %>>%
  learners #  %>>%
  # po("classifavg", innum = length(learners_l))
plot(graph)
graph_learner = as_learner(graph)
as.data.table(graph_learner$param_set)[1:100, .(id, class, lower, upper)]
as.data.table(graph_learner$param_set)[100:190, .(id, class, lower, upper)]
search_space = ps(
  # preprocesing
  # interaction_branch.selection = p_fct(levels = c("nop_filter", "modelmatrix")),
  prep_branch.selection = p_fct(levels = c("nop_prep", "yeojohnson", "pca", "ica")),
  pca.rank. = p_int(2, 6, depends = prep_branch.selection == "pca"),
  ica.n.comp = p_int(2, 6, depends = prep_branch.selection == "ica"),
  yeojohnson.standardize = p_lgl(depends = prep_branch.selection == "yeojohnson"),
  # models
  ranger.ranger.mtry.ratio = p_dbl(0.2, 1),
  ranger.ranger.max.depth = p_int(2, 6),
  kknn.kknn.k = p_int(5, 20),
  # extratrees.extratrees.ntree = p_int(200, 1000),
  # extratrees.extratrees.mtry = p_int(5, task_extreme$ncol),
  # extratrees.extratrees.nodesize = p_int(2, 10),
  # extratrees.extratrees.numRandomCuts = p_int(2, 5)
  xgboost.xgboost.nrounds = p_int(100, 5000),
  xgboost.xgboost.eta = p_dbl(1e-4, 1),
  xgboost.xgboost.max_depth = p_int(1, 8),
  xgboost.xgboost.colsample_bytree = p_dbl(0.1, 1),
  xgboost.xgboost.colsample_bylevel = p_dbl(0.1, 1),
  xgboost.xgboost.lambda = p_dbl(0.1, 1),
  xgboost.xgboost.gamma = p_dbl(1e-4, 1000),
  xgboost.xgboost.alpha = p_dbl(1e-4, 1000),
  xgboost.xgboost.subsample = p_dbl(0.1, 1)
)
# plan("multisession", workers = 4L)
at_classif = auto_tuner(
  method = "random_search",
  learner = graph_learner,
  resampling = rsmp("cv", folds = 3),
  measure = msr("classif.acc"),
  search_space = search_space,
  term_evals = 20
)
at_classif$train(task_classif)

# inspect results
archive <- as.data.table(at_classif$archive)
archive$classif.acc
length(at_ranger$state)
ggplot(archive[, mean(classif.acc), by = "ranger.ranger.max.depth"], aes(x = ranger.ranger.max.depth, y = V1)) + geom_line()
ggplot(archive[, mean(classif.acc), by = "prep_branch.selection"], aes(x = prep_branch.selection, y = V1)) + geom_bar(stat = "identity")
preds = at_classif$predict(task_classif)
preds$confusion
preds$score(msr("classif.acc"))

# holdout
preds_holdout <- at_classif$predict(task_classif_holdout)
preds_holdout$confusion
autoplot(preds_holdout, type = "roc")
preds_holdout$score(msrs(c("classif.acc"))) # , "classif.recall", "classif.precision"
prediciotns_extreme_holdout <- as.data.table(preds_holdout)
prediciotns_extreme_holdout <- prediciotns_extreme_holdout[prob.1 > 0.6]
nrow(prediciotns_extreme_holdout)
mlr3measures::acc(prediciotns_extreme_holdout$truth, prediciotns_extreme_holdout$response)
sum(task_classif$data()$bin == 1) / task_classif$nrow

# predictions for qc
cols_qc <- c("symbol", "date")
predictoins_qc <- cbind(X_holdout_[, ..cols_qc], as.data.table(preds_holdout))
predictoins_qc[, grep("row_ids|truth", colnames(predictoins_qc)) := NULL]
predictoins_qc <- unique(predictoins_qc)
setorder(predictoins_qc, "date")

# save to dropbox for live trading (create table for backtest)
cols <- c("date", "symbol", colnames(predictoins_qc)[3:ncol(predictoins_qc)])
pead_qc <- predictoins_qc[, ..cols]
pead_qc <- pead_qc[, .(symbol = paste0(unlist(symbol), collapse = ", "),
                       prob1 = paste0(unlist(prob.1), collapse = ",")), by = date]
bl_endp_key <- storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), key=Sys.getenv("BLOB-KEY"))
cont <- storage_container(bl_endp_key, "qc-backtest")
storage_write_csv2(pead_qc, cont, file = "pead_qc_backtest_graph.csv", col_names = FALSE)



# # SYSTEMIC RISK -----------------------------------------------------------
# #
# indicators <- copy(market_data)
# indicators[, monthly_ath := shift(frollapply(high, 22 * 2, max), 1), by = "symbol"]
# indicators[, signal := ifelse(close > monthly_ath, 1, 0)]
#
# # aggregate
# indicators <- indicators[, .(signal = sum(signal, na.rm = TRUE)), by = "datetime"]
# setorderv(indicators, "datetime")
#
# # merge indicators and spy
# backtest_data <- spy[indicators, on = 'datetime']
# backtest_data <- na.omit(backtest_data)
# setorder(backtest_data, "datetime")
#
# # visualize
# ggplot(backtest_data, aes(x = datetime, y = signal)) + geom_line()
#
# # backtst function
# backtest <- function(returns, indicator, threshold, return_cumulative = TRUE) {
#   sides <- vector("integer", length(indicator))
#   for (i in seq_along(sides)) {
#     if (i %in% c(1) || is.na(indicator[i-1])) {
#       sides[i] <- NA
#     } else if (indicator[i-1] < threshold) {
#       sides[i] <- 0
#     } else {
#       sides[i] <- 1
#     }
#   }
#   sides <- ifelse(is.na(sides), 1, sides)
#   returns_strategy <- returns * sides
#   if (return_cumulative) {
#     return(PerformanceAnalytics::Return.cumulative(returns_strategy))
#   } else {
#     return(returns_strategy)
#   }
# }
#
# # optimizations loop
# thresholds <- c(seq(1, 200, 1))
# colnames(backtest_data)
# variables <- "signal" # colnames(indicators)[grep("below", colnames(indicators))]
# params <- expand.grid(thresholds, variables, stringsAsFactors = FALSE)
# returns_strategies <- list()
# x <- vapply(1:nrow(params), function(i) backtest(backtest_data$returns,
#                                                  # backtest_data[, get(params[i, 2])],
#                                                  TTR::SMA(backtest_data[, get(params[i, 2])], 8 * 15),
#                                                  params[i, 1]),
#             numeric(1))
# optim_results <- cbind(params, cum_return = x)
#
# # inspect
# tail(optim_results[order(optim_results$cum_return), ], 50)
# # ggplot(optim_results[grep("01|03", optim_results$Var2),], aes(x = cum_return)) + geom_histogram() +
# #   geom_vline(xintercept = 4.1, color = "red") + facet_grid(. ~ Var2)
# # ggplot(optim_results[grep("03", optim_results$Var2),], aes(x = cum_return)) + geom_histogram() +
# #   geom_vline(xintercept = 4.1, color = "red") + facet_grid(. ~ Var2)
# # summary_results <- as.data.table(optim_results)
# # summary_results[, median(cum_return), by = Var2]
#
# # backtest individual
# strategy_returns <- backtest(backtest_data$returns, backtest_data$signal, 1, return_cumulative = FALSE)
# charts.PerformanceSummary(xts(cbind(backtest_data$returns, strategy_returns), order.by = backtest_data$datetime))
# Performance(xts(strategy_returns, order.by = backtest_data$datetime))
# Performance(xts(backtest_data$returns, order.by = backtest_data$datetime))
#
