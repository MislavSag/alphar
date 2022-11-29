library(data.table)
library(timechange)
library(roll)
library(tiledb)
library(lubridate)
library(rtsplot)
library(TTR)
library(patchwork)
library(ggplot2)
library(AzureStor)
library(PerformanceAnalytics)
library(QuantTools)
library(LogicReg)



# UTILS -------------------------------------------------------------------
# date segments
GFC <- c("2007-01-01", "2010-01-01")
COVID <- c("2020-01-01", "2021-06-01")
AFTER_COVID <- c("2021-06-01", "2022-01-01")
NEW <- c("2022-01-01", as.character(Sys.Date()))


# IMPORT DATA -------------------------------------------------------------
# import market data with OHLCV features
arr <- tiledb_array("D:/equity-usa-hour-fmpcloud-adjusted", as.data.frame = TRUE)
system.time(hour_data <- arr[])
tiledb_array_close(arr)
hour_data_dt <- as.data.table(hour_data)
hour_data_dt[, time := as.POSIXct(time, tz = "UTC")]

# keep only trading hours
hour_data_dt <- hour_data_dt[as.integer(time_clock_at_tz(time,
                                                         tz = "America/New_York",
                                                         units = "hours")) %in% 10:16]

# clean data
hour_data_dt[, returns := close / shift(close) - 1, by = "symbol"]
hour_data_dt <- unique(hour_data_dt, by = c("symbol", "time"))
hour_data_dt <- na.omit(hour_data_dt)

# spy data
spy <- hour_data_dt[symbol == "SPY", .(time, close, returns)]
spy <- unique(spy, by =  "time")

# visualize
rtsplot(as.xts.data.table(spy[, .(time, close)]))
rtsplot(as.xts.data.table(hour_data_dt[symbol == "AAPL", .(time, close)]))

# parameter
windows_ = c(7, 7 * 5, 7 * 5 * 22, 7 * 5 * 22 * 6)

# close ATH
hour_data_dt[, close_ath := (cummax(high) - close) / cummax(high), by = symbol]
new_cols <- paste0("ath_", windows_)
hour_data_dt[, (new_cols) := lapply(c(1, windows_), function(w) (shift(frollapply(high, w, max), 1L) / close) - 1), by = symbol]

# whole number discrepancy
hour_data_dt$pretty_1 <- sapply(hour_data_dt$close, function(x) pretty(x)[1])
hour_data_dt$pretty_2 <- sapply(hour_data_dt$close, function(x) pretty(x)[2])
hour_data_dt[, close_round_div_down := (close - pretty_1) / pretty_1]
hour_data_dt[, close_round_div_up := (close - pretty_2) / pretty_2]
hour_data_dt[, `:=`(pretty_1 = NULL, pretty_2 = NULL)]

# trading rules
hour_data_dt[, close_above_sma200 := (close - sma(close, n = 200)) / sma(close, n = 200), by = symbol]
hour_data_dt[, close_above_sma100 := (close - sma(close, n = 100)) / sma(close, n = 100), by = symbol]
hour_data_dt[, close_above_sma50 := (close - sma(close, n = 50)) / sma(close, n = 50), by = symbol]
hour_data_dt[, close_above_sma22 := (close - sma(close, n = 22)) / sma(close, n = 22), by = symbol]

# rolling quantile substraction
quantile_divergence_window = c(7 * 5 * 22, 7 * 5 * 22 * 6)
generate_quantile_divergence <- function(ohlcv, p = 0.99, window_sizes = quantile_divergence_window) {
  q_cols <- paste0("q", p * 100, "_close_", window_sizes)
  ohlcv[, (q_cols) := lapply(window_sizes, function(w) roll::roll_quantile(close, width = w, p = p)), by = symbol]
  new_cols <- paste0("q", p * 100, "_close_divergence_", window_sizes)
  ohlcv[, (new_cols) := lapply(q_cols, function(x) (close - get(x)) / close), by = symbol]
  ohlcv[, (q_cols):=NULL]
  return(ohlcv)
}
hour_data_dt <- generate_quantile_divergence(hour_data_dt, p = 0.01)
hour_data_dt <- generate_quantile_divergence(hour_data_dt, p = 0.25)
hour_data_dt <- generate_quantile_divergence(hour_data_dt, p = 0.5)
hour_data_dt <- generate_quantile_divergence(hour_data_dt, p = 0.75)
hour_data_dt <- generate_quantile_divergence(hour_data_dt, p = 0.25)
hour_data_dt <- generate_quantile_divergence(hour_data_dt, p = 0.99)

# create dummy variables
cols <- colnames(hour_data_dt)[9:length(hour_data_dt)]
new_cols_dummy <- paste0(cols, "_dummy")
hour_data_dt[, (new_cols_dummy) := lapply(.SD, function(x) ifelse(x >= 0, 1, 0)), .SDcols = cols]

# create labels
hour_data_dt[, ret_label_1 := shift(close, -1L, type = "shift") / close - 1, by = symbol]
hour_data_dt[, ret_label_7 := shift(close, -7L, type = "shift") / close - 1, by = symbol]
hour_data_dt[, ret_label_week := shift(close, -7 * 5, type = "shift") / close - 1, by = symbol]
hour_data_dt[, ret_label_month := shift(close, -7 * 5 * 22, type = "shift") / close - 1, by = symbol]

# filter bars
weekly_volume <- hour_data_dt[, .(weekly_volume = sum(volume, na.rm = TRUE)),
                              by = .(symbol,
                                     year = data.table::year(time),
                                     week = data.table::week(time))]
weekly_volume[, weekly_volume := shift(weekly_volume)]

# add weekly volume column
hour_data_dt[, `:=`(week = data.table::week(time), year = data.table::year(time))]
hour_data_dt <- merge(hour_data_dt, weekly_volume,
                      by = c("symbol", "week", "year"),
                      all.x = TRUE, all.y = FALSE)

# add filter variables and filter
DT <- copy(hour_data_dt)

# creat volume predictors
DT[, volume_012 := as.integer((volume / weekly_volume)  > 0.12)]
DT[, volume_010 := as.integer((volume / weekly_volume)  > 0.1)]
DT[, volume_007 := as.integer((volume / weekly_volume)  > 0.07)]
DT[, volume_005 := as.integer((volume / weekly_volume)  > 0.05)]
DT[, volume_003 := as.integer((volume / weekly_volume)  > 0.03)]
DT[, volume_001 := as.integer((volume / weekly_volume)  > 0.01)]

# filter data
DT <- DT[volume_007 == TRUE]
dim(hour_data_dt)
dim(DT)

# train and test set
cols_keep <- c("symbol", "time", new_cols_dummy,
               colnames(DT)[grep("volume_", colnames(DT))],
               "ret_label_7")
DT <- DT[, ..cols_keep]
DT <- na.omit(DT)
X_train <- as.data.frame(DT[as.Date(time) %between% c("2015-01-01", "2021-01-01")])
X_test <- as.data.frame(DT[as.Date(time) %between% c("2021-01-01", "2022-10-01")])

# fit model
myanneal <- logreg.anneal.control(start = -1, end = -4, iter = 10000, update = 100)
fit1 <- logreg(resp = X_train[, ncol(X_train)],
               bin = X_train[, 3:(ncol(X_train)-1)],
               type = 2,
               select = 2,
               ntrees = c(1, 2),
               nleaves = c(1, 7),
               anneal.control = myanneal)
plot(fit1)
print(fit1)

# predictions
z <- predict(fit1)
# plot(z, X_train[, ncol(X_train)]-z, xlab="fitted values", ylab="residuals")
table(z)
z$

# inspect results
X_test_predictions <- predict(fit1, newbin = X_test[, 3:(ncol(X_test)-1)])
X_test_post <- cbind(X_test[, c("symbol", "time", "ret_label_7")], X_test_predictions)
X_test_post <- as.data.table(X_test_post)
table(X_test_post$X_test_predictions)
X_test_post[X_test_predictions >= 0][, .N, by = ret_label_7 >= 0]
X_test_post[X_test_predictions >= 0][, .N, by = ret_label_7 >= 0][, N / sum(N)]
X_test_post[X_test_predictions >= 0.002][, .N, by = ret_label_7 >= 0]
X_test_post[X_test_predictions >= 0.002][, .N, by = ret_label_7 >= 0][, N / sum(N)]
X_test_post[X_test_predictions < 0][, .N, by = ret_label_7 < 0]
X_test_post[X_test_predictions < 0][, .N, by = ret_label_7 < 0]


data(logreg.savefit1,logreg.savefit2,logreg.savefit3,logreg.savefit4,
     logreg.savefit5,logreg.savefit6,logreg.savefit7,logreg.testdat)
myanneal <- logreg.anneal.control(start = -1, end = -4, iter = 500, update = 100)
# in practie we would use 25000 iterations or far more - the use of 500 is only
# to have the examples run fast
## Not run: myanneal <- logreg.anneal.control(start = -1, end = -4, iter = 25000, update = 500)
fit1 <- logreg(resp = logreg.testdat[,1], bin=logreg.testdat[, 2:21], type = 2,
               select = 1, ntrees = 2, anneal.control = myanneal)
# the best score should be in the 0.95-1.10 range
plot(fit1)

# you'll probably see X1-X4 as well as a few noise predictors
# use logreg.savefit1 for the results with 25000 iterations
plot(logreg.savefit1)
print(logreg.savefit1)

