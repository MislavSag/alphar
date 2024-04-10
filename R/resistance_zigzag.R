library(data.table)
library(arrow)
library(lubridate)

# library(ts2net)
# library(finfeatures)
# library(ggplot2)
# library(stringr)
# library(dataPreparation)
# library(runner)
# library(TTR)
# library(PerformanceAnalytics)


# PARAMETERS -------------------------------------------------------------
# Parameters we use in analysis below
symbols = c("aapl", "abbv", "amzn", "cost", "spy", "dow", "duk", "acn")
zz_pct_change = 5 # 2, 3 sd
e = 0.01 # mean return

# import Databento minute data
prices = read_parquet("F:/databento/minute.parquet")

# change timezone
attr(prices$ts_event, "tz")
prices[, ts_event := with_tz(ts_event, tz = "America/New_York")]
attr(prices$ts_event, "tz")

# calcualte ohlc returns
cols = c("open", "high", "low", "close")
prices[, (paste0(cols, "_returns")) := lapply(.SD, function(x) (x / shift(x)) - 1),
       .SDcols = cols, by = instrument_id ]
prices = na.omit(prices)

# TODO add to mlfinance package
# remove outliers
remove_sd_outlier_recurse = function(df, ...) {

  # remove outliers
  x_ = dataPreparation::remove_sd_outlier(data_set = df, ...)
  while (nrow(x_) < nrow(df)) {
    df <- x_
    x_ = dataPreparation::remove_sd_outlier(data_set = df, ...)
  }
  return(df)
}

# remove outliers
prices = prices[, remove_sd_outlier_recurse(
  df = .SD, cols = c("high_returns", "high", "low_returns", "low"),
  n_sigmas = 4), by = symbol]

# plot
data_ <- as.xts.data.table(market_data[symbol == "AAPL", .(date, high)])
rtsplot(data_, type = "l")
data_ <- as.xts.data.table(market_data[symbol == "AAPL", .(date, low)])
rtsplot(data_, type = "l")

# calcualte zig zag
dt = prices[, .(symbol, date, open, high, low, close, volume)]
setorder(DT, symbol, date)
DT <- na.omit(DT)
DT[, zz := ZigZag(as.matrix(.SD[, .(high, low)]),
                  change = zz_pct_change),
   by = symbol]
DT[, infl := (zz - shift(zz)) > 0]
DT[, infl_max := (infl - shift(infl)) < 0]
DT[, infl_min := (infl - shift(infl)) > 0]
DT[infl_max == TRUE, upper := high * (1 + e)]
DT[infl_max == TRUE, lower := high * (1 - e)]

# runner
DT_inflmax <- DT[infl_max == TRUE]
DT_inflmax[, mean_resistance := list(runner(
  .SD,
  f = function(x) {
    ind <- vapply(1:length(x$high), function(j) tail(x$high, 1) %between% c(x$lower[j], x$upper[j]), FUN.VALUE = logical(1))
    x[ind, mean(high, na.rm = TRUE)]
  },
  k = 10,
  na_pad = TRUE)
), by = symbol]
DT_inflmax[, n_resistance := list(runner(
  .SD,
  f = function(x) {
    ind <- vapply(1:length(x$high), function(j) tail(x$high, 1) %between% c(x$lower[j], x$upper[j]), FUN.VALUE = logical(1))
    sum(ind)
  },
  k = 10,
  na_pad = TRUE)
), by = symbol]
# DT_inflmax[, resistance_group := list(runner(
#   .SD,
#   f = function(x) {
#     paste0(sort(which(vapply(1:length(x$high), function(j) tail(x$high, 1) %between% c(x$lower[j], x$upper[j]), FUN.VALUE = logical(1)))), collapse = "-")
#     # which(vapply(1:length(x$high), function(j) tail(x$high, 1) %between% c(x$lower[j], x$upper[j]), FUN.VALUE = logical(1)))
#   },
#   k = 20,
#   na_pad = TRUE)
# ), by = symbol]
str(DT_inflmax)
head(DT_inflmax, 50)
# DT_inflmax[, mean_resistance := data.table::frollmean(high, 20, na.rm = TRUE), by = .(symbol, resistance_group)]
# DT_inflmax[, n_resistance := unlist(sapply(strsplit(resistance_group, "-"), length))]

# merge with init DT
strategy_data <- merge(DT, DT_inflmax[, .(symbol, date, mean_resistance, n_resistance)],
                       by = c("symbol", "date"), all.x = TRUE, all.y = FALSE)
strategy_data[, mean_resistance := nafill(mean_resistance, type = "locf")]
strategy_data[, n_resistance := nafill(n_resistance, type = "locf")]

# plot
sample_plot_data <- strategy_data[0:150000,]
ggplot(sample_plot_data, aes(x = date)) +
  geom_line(aes(y = high)) +
  geom_point(data = sample_plot_data[infl_max == TRUE], aes(y = high), color = "green", size = 3) +
  geom_point(data = sample_plot_data[infl_min == TRUE], aes(y = low), color = "red", size = 3) +
  geom_line(aes(y = mean_resistance), color = "green")
sample_plot_data <- strategy_data[symbol == "COST" & date %between% c("2020-08-01", "2021-12-30")]
ggplot(sample_plot_data, aes(x = date)) +
  geom_line(aes(y = high)) +
  geom_point(data = sample_plot_data[infl_max == TRUE], aes(y = high), color = "green", size = 3) +
  geom_point(data = sample_plot_data[infl_min == TRUE], aes(y = low), color = "red", size = 3) +
  geom_line(aes(y = mean_resistance), color = "green")


# backtest
mean_res <- strategy_data[, mean_resistance]
n_res <- strategy_data[, n_resistance]
high <- strategy_data[, high]
signals <- vector("numeric", length(mean_res))
for (i in seq_along(mean_res)) {
  if (i == 1 || is.na(mean_res[i - 1])) {
    signals[i] <- NA
  } else if (n_res[i - 1] >= 3 & high[i - 1] > mean_res[i - 1]) {
    signals[i] <- 1
  } else {
    signals[i] <- 0
  }
}
strategy_data[, signals := signals]
strategy_data[signals == 1]

# sample_data <- strategy_data[10000:13000]
# ggplot(sample_data, aes(x = date)) +
#   geom_line(aes(y = high)) +
#   geom_line(data = sample_data[signals == 1], aes(y = high), color = "red")
#
# # performance function
# Performance <- function(x) {
#   cumRetx = Return.cumulative(x)
#   annRetx = Return.annualized(x, scale=252 * 8)
#   sharpex = SharpeRatio.annualized(x, scale=252 * 8)
#   winpctx = length(x[x > 0])/length(x[x != 0])
#   annSDx = sd.annualized(x, scale=252 * 8)
#
#   DDs <- findDrawdowns(x)
#   maxDDx = min(DDs$return)
#   maxLx = max(DDs$length)
#
#   Perf = c(cumRetx, annRetx, sharpex, winpctx, annSDx, maxDDx, maxLx)
#   names(Perf) = c("Cumulative Return", "Annual Return","Annualized Sharpe Ratio",
#                   "Win %", "Annualized Volatility", "Maximum Drawdown", "Max Length Drawdown")
#   return(Perf)
# }
#
#
# backtest results
# backtest_data <- copy(strategy_data)
# backtest_data[, benchmark := close / shift(close) - 1]
# backtest_data[signals == 1, strategy := benchmark]
# backtest_data[signals == 0, strategy := 0]
# charts.PerformanceSummary(as.xts.data.table(backtest_data[, .(date, benchmark, strategy)]))

# Return.cumulative(na.omit(as.xts.data.table(backtest_data[, .(datetime, benchmark, strategy)])))
# SharpeRatio.annualized(na.omit(as.xts.data.table(backtest_data[, .(datetime, benchmark, strategy)])), scale=252 * 7)
# x = na.omit(as.xts.data.table(backtest_data[, .(datetime, benchmark, strategy)]))
# length(x$strategy[x$strategy > 0])/length(x$strategy[x$strategy != 0])
# length(x$benchmark[x$benchmark > 0])/length(x$benchmark[x$benchmark != 0])
# DDs <- findDrawdowns(x$benchmark)
# maxDDx = min(DDs$return)
# maxLx = max(DDs$length)

# save to blob for QC backtest
qc_data <- strategy_data[, .(symbol, date, signals)]
qc_data[, signals := na.fill(signals, fill = 0)]
qc_data[, signals := signals - shift(signals)]
qc_data <- qc_data[signals == 1]
qc_data <- na.omit(qc_data)
qc_data <- as.data.table(tidyr::pivot_wider(qc_data, date, names_from = symbol, values_from = signals))
setorder(qc_data, date)
qc_data <- as.data.frame(qc_data)
qc_data[, 2:ncol(qc_data)] <- lapply(2:ncol(qc_data), function(x) {
  qc_data[, x] <- ifelse(qc_data[, x] == 1, colnames(qc_data)[x], NA)
})
qc_data$symbols <- apply(qc_data[, 2:ncol(qc_data)], MARGIN = 1, paste0, collapse = ";")
qc_data$symbols <- gsub("NA;|;NA", "", qc_data$symbols)
qc_data <- qc_data[, c(1, ncol(qc_data))]
head(qc_data)
tail(qc_data)
bl_endp_key <- storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), key=Sys.getenv("BLOB-KEY"))
cont <- storage_container(bl_endp_key, "qc-backtest")
storage_write_csv(qc_data, cont, "resistance.csv")
