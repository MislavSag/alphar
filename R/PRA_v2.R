library(data.table)
library(httr)
library(QuantTools)
library(ggplot2)
library(TTR)
library(PerformanceAnalytics)
library(patchwork)
library(runner)
library(tiledb)
library(lubridate)
library(rvest)
library(AzureStor)



# PARAMETERS
windows = c(8 * 5, 8 * 22, 8 * 22 * 3, 8 * 22 * 6,  8 * 22 * 12, 8 * 22 * 12 * 2)

# import data
arr <- tiledb_array("D:/equity-usa-hour-fmpcloud-adjusted")
system.time(hour_data_raw <- arr[])
tiledb_array_close(arr)
hour_data_raw <- as.data.table(hour_data_raw)
attr(hour_data_raw$time, "tz") <- Sys.getenv("TZ")
hour_data_raw[, time := with_tz(time, "America/New_York")]

# keep trading hours
hour_data <- hour_data_raw[as.ITime(time) %between% c(as.ITime("10:00:00"), as.ITime("16:00:00"))]
hour_data <- unique(hour_data, cols = c("symbol", "time"))

# keep only symbols with at least N obsrvations
keep_symbols <- hour_data[, .N, by = symbol][N > max(windows), symbol]
hour_data <- hour_data[symbol %in% keep_symbols]
hour_data[, N := 0]
setorderv(hour_data, c('symbol', 'time'))

# calcualte returns
hour_data[, returns := close / shift(close) - 1, by = "symbol"]
hour_data <- na.omit(hour_data)

# extract SPY
spy <- hour_data[symbol == "SPY", .(time, close, returns)]
symbols <- unique(hour_data$symbol)
close_data <- hour_data[, .(symbol, time, close)]

# import indicators
# indicators_old <- fread("D:/risks/pra/pra_indicators.csv")
# attr(indicators_old$datetime, "tzone") <- "EST"
# setorderv(indicators_old, "datetime")
# last_datetime <- max(indicators_old$datetime)
# close_data <- close_data[datetime %between% c(as.Date(last_datetime) - (windows[length(windows)] / 8),
#                                               Sys.Date())]

# calculate main variable
# CALCUALTE OR
cols <- paste0("pr_", windows)
close_data[, (cols) := lapply(windows, function(w) roll_percent_rank(close, w)), by = "symbol"]

# fwrite(close_data, "D:/risks/pra/pra_raw.csv", dateTimeAs = "write.csv")
# IMPORT
# close_data <- fread("D:/risks/pra/pra_raw.csv")
# close_data$datetime <- as.POSIXct(as.numeric(close_data$datetime),
#                                   origin=as.POSIXct("1970-01-01", tz="EST"),
#                                   tz="EST")

# sp100 stocks
sp100 <- GET("https://en.wikipedia.org/wiki/S%26P_100") |>
  httr::content(x = _) |>
  html_elements(x = _, "table") |>
  (`[[`)(3) |>
  html_table(x = _, fill = TRUE)
symbols_100 = sp100$Symbol



# DUMMY -------------------------------------------------------------------

# dummy
cols_above_999 <- paste0("pr_above_dummy_", windows)
close_data[, (cols_above_999) := lapply(.SD, function(x) ifelse(x > 0.999, 1, 0)), .SDcols = cols]
cols_below_001 <- paste0("pr_below_dummy_", windows)
close_data[, (cols_below_001) := lapply(.SD, function(x) ifelse(x < 0.001, 1, 0)), .SDcols = cols]
cols_net_1 <- paste0("pr_below_dummy_net_", windows)
close_data[, (cols_net_1) := close_data[, ..cols_above_999] - close_data[, ..cols_below_001]]

cols_above_99 <- paste0("pr_above_dummy_99_", windows)
close_data[, (cols_above_99) := lapply(.SD, function(x) ifelse(x > 0.99, 1, 0)), .SDcols = cols]
cols_below_01 <- paste0("pr_below_dummy_01_", windows)
close_data[, (cols_below_01) := lapply(.SD, function(x) ifelse(x < 0.01, 1, 0)), .SDcols = cols]
cols_net_2 <- paste0("pr_below_dummy_net_0199", windows)
close_data[, (cols_net_2) := close_data[, ..cols_above_99] - close_data[, ..cols_below_01]]

cols_above_97 <- paste0("pr_above_dummy_97_", windows)
close_data[, (cols_above_97) := lapply(.SD, function(x) ifelse(x > 0.97, 1, 0)), .SDcols = cols]
cols_below_03 <- paste0("pr_below_dummy_03_", windows)
close_data[, (cols_below_03) := lapply(.SD, function(x) ifelse(x < 0.03, 1, 0)), .SDcols = cols]
cols_net_3 <- paste0("pr_below_dummy_net_0397", windows)
close_data[, (cols_net_3) := close_data[, ..cols_above_97] - close_data[, ..cols_below_03]]

cols_above_95 <- paste0("pr_above_dummy_95_", windows)
close_data[, (cols_above_95) := lapply(.SD, function(x) ifelse(x > 0.95, 1, 0)), .SDcols = cols]
cols_below_05 <- paste0("pr_below_dummy_05_", windows)
close_data[, (cols_below_05) := lapply(.SD, function(x) ifelse(x < 0.05, 1, 0)), .SDcols = cols]
cols_net_4 <- paste0("pr_below_dummy_net_0595", windows)
close_data[, (cols_net_4) := close_data[, ..cols_above_95] - close_data[, ..cols_below_05]]


# get risk measures
indicators <- close_data[symbol != "SPY", lapply(.SD, sum, na.rm = TRUE),
                         .SDcols = c(colnames(close_data)[grep("pr_\\d+", colnames(close_data))],
                                     cols_above_999, cols_above_99, cols_below_001, cols_below_01,
                                     cols_above_97, cols_below_03, cols_above_95, cols_below_05,
                                     cols_net_1, cols_net_2, cols_net_3, cols_net_4),
                         by = .(time)]
indicators <- unique(indicators, by = c("time"))
file_name <- paste0("D:/risks/pra/pra_indicators",
                    format(Sys.time(), format = "%Y%m%d%H%M%S"),
                    ".csv")
fwrite(indicators, file_name)

# load indicators
# indicators <- fread("D:/risks/pra/pra_indicators.csv")
# attr(indicators$datetime, "tzone") <- "EST"
setorder(indicators, "datetime")

# SP 100
close_data_100 <- close_data[symbol %in% c(symbols_100, "SPY")]
indicators_100 <- close_data_100[symbol != "SPY", lapply(.SD, sum, na.rm = TRUE),
                                 .SDcols = c(colnames(close_data)[grep("pr_\\d+", colnames(close_data))],
                                             cols_above_999, cols_above_99, cols_below_001, cols_below_01,
                                             cols_above_97, cols_below_03, cols_above_95, cols_below_05,
                                             cols_net_1, cols_net_2, cols_net_3, cols_net_4),
                                 by = .(time)]
indicators_100 <- unique(indicators_100, by = c("time"))

# merge indicators and spy
backtest_data <- spy[indicators, on = 'time']
backtest_data <- na.omit(backtest_data)
setorder(backtest_data, "time")
backtest_data_100 <- spy[indicators_100, on = 'time']
backtest_data_100 <- na.omit(backtest_data_100)
setorder(backtest_data_100, "time")



# PR STATS ----------------------------------------------------------------
# pr stats
# indicators_sum <- close_data[, lapply(.SD, sum, na.rm = TRUE), .SDcols = colnames(close_data)[-(1:3)], by = .(datetime)]
# colnames(indicators_sum)[-1] <- paste0(colnames(indicators_sum)[-1], "_sum")
# indicators_median <- close_data[, lapply(.SD, median, na.rm = TRUE), .SDcols = colnames(close_data)[-(1:3)], by = .(datetime)]
# colnames(indicators_median)[-1] <- paste0(colnames(indicators_median)[-1], "_median")
# indicators_skew <- close_data[, lapply(.SD, skewness, na.rm = TRUE), .SDcols = colnames(close_data)[-(1:3)], by = .(datetime)]
# colnames(indicators_skew)[-1] <- paste0(colnames(indicators_skew)[-1], "_skew")
#
# indicators <- Reduce(function(x, y) merge(x, y, by = "datetime", all.x = TRUE, all.y = FALSE),
#                      list(indicators_sum, indicators_median, indicators_skew))
# indicators <- unique(indicators, by = c("datetime"))
# setorder(indicators, "datetime")



# VISUALIZATION -----------------------------------------------------------
# date segments
GFC <- c("2007-01-01", "2010-01-01")
AFTERGFCBULL <- c("2010-01-01", "2015-01-01")
COVID <- c("2020-01-01", "2021-06-01")
AFTER_COVID <- c("2021-06-01", "2022-01-01")
NEW <- c("2022-01-01", "2022-03-15")

# check individual stocks
sample_ <- close_data[symbol == "AAPL"]
v_buy <- sample_[pr_above_dummy_95_1056 == 1, time, ]
v_sell <- sample_[pr_below_dummy_05_1056 == 1, time, ]
ggplot(sample_, aes(x = time)) +
  geom_line(aes(y = close)) +
  geom_vline(xintercept = v_buy, color = "green") +
  geom_vline(xintercept = v_sell, color = "red")
ggplot(sample_[time %between% GFC], aes(x = time)) +
  geom_line(aes(y = close)) +
  geom_vline(xintercept = v_buy, color = "green") +
  geom_vline(xintercept = v_sell, color = "red")
ggplot(sample_[datetime %between% AFTERGFCBULL], aes(x = datetime)) +
  geom_line(aes(y = close)) +
  geom_vline(xintercept = v_buy, color = "green") +
  geom_vline(xintercept = v_sell, color = "red")
ggplot(sample_[datetime %between% COVID], aes(x = datetime)) +
  geom_line(aes(y = close)) +
  geom_vline(xintercept = v_buy, color = "green") +
  geom_vline(xintercept = v_sell, color = "red")
ggplot(sample_[datetime %between% AFTER_COVID], aes(x = datetime)) +
  geom_line(aes(y = close)) +
  geom_vline(xintercept = v_buy, color = "green") +
  geom_vline(xintercept = v_sell, color = "red")
ggplot(sample_[datetime %between% NEW], aes(x = datetime)) +
  geom_line(aes(y = close)) +
  geom_vline(xintercept = v_buy, color = "green") +
  geom_vline(xintercept = v_sell, color = "red")




# PREPARE BACKTEST --------------------------------------------------------
# plots sum
g1 <- ggplot(backtest_data, aes(x = datetime)) +
  geom_line(aes(y = pr_below_dummy_40))
g2 <- ggplot(backtest_data, aes(x = datetime)) +
  geom_line(aes(y = pr_below_dummy_176))
g3 <- ggplot(backtest_data, aes(x = datetime)) +
  geom_line(aes(y = pr_below_dummy_528))
g4 <- ggplot(backtest_data, aes(x = datetime)) +
  geom_line(aes(y = pr_below_dummy_1056))
g5 <- ggplot(backtest_data, aes(x = datetime)) +
  geom_line(aes(y = pr_below_dummy_2112))
g6 <- ggplot(backtest_data, aes(x = datetime)) +
  geom_line(aes(y = pr_below_dummy_4224))
( g1 | g2 ) / ( g3 | g4 ) / ( g5 | g6)

# plots sum 95 / 05
g1 <- ggplot(backtest_data, aes(x = datetime)) +
  geom_line(aes(y = pr_below_dummy_05_40)) +
  geom_line(aes(y = pr_above_dummy_95_40), color ="red")
g2 <- ggplot(backtest_data, aes(x = datetime)) +
  geom_line(aes(y = pr_below_dummy_05_176)) +
  geom_line(aes(y = pr_above_dummy_95_176), color ="red")
g3 <- ggplot(backtest_data, aes(x = datetime)) +
  geom_line(aes(y = pr_below_dummy_05_528)) +
  geom_line(aes(y = pr_above_dummy_95_528), color ="red")
g4 <- ggplot(backtest_data, aes(x = datetime)) +
  geom_line(aes(y = pr_below_dummy_05_1056)) +
  geom_line(aes(y = pr_above_dummy_95_1056), color ="red")
g5 <- ggplot(backtest_data, aes(x = datetime)) +
  geom_line(aes(y = pr_below_dummy_05_2112)) +
  geom_line(aes(y = pr_above_dummy_95_2112), color ="red")
g6 <- ggplot(backtest_data, aes(x = datetime)) +
  geom_line(aes(y = pr_below_dummy_05_4224)) +
  geom_line(aes(y = pr_above_dummy_95_4224), color ="red")
( g1 | g2 ) / ( g3 | g4 ) / ( g5 | g6)

# net
g1 <- ggplot(backtest_data, aes(x = datetime)) +
  geom_line(aes(y = pr_below_dummy_net_01991056))
g2 <- ggplot(backtest_data, aes(x = datetime)) +
  geom_line(aes(y = pr_below_dummy_05_176)) +
  geom_line(aes(y = pr_above_dummy_95_176), color ="red")
g3 <- ggplot(backtest_data, aes(x = datetime)) +
  geom_line(aes(y = pr_below_dummy_05_528)) +
  geom_line(aes(y = pr_above_dummy_95_528), color ="red")
g4 <- ggplot(backtest_data, aes(x = datetime)) +
  geom_line(aes(y = pr_below_dummy_05_1056)) +
  geom_line(aes(y = pr_above_dummy_95_1056), color ="red")
g5 <- ggplot(backtest_data, aes(x = datetime)) +
  geom_line(aes(y = pr_below_dummy_05_2112)) +
  geom_line(aes(y = pr_above_dummy_95_2112), color ="red")
g6 <- ggplot(backtest_data, aes(x = datetime)) +
  geom_line(aes(y = pr_below_dummy_05_4224)) +
  geom_line(aes(y = pr_above_dummy_95_4224), color ="red")
( g1 | g2 ) / ( g3 | g4 ) / ( g5 | g6)

# # plots sum
# g1 <- ggplot(backtest_data, aes(x = datetime)) +
#   geom_line(aes(y = pr_40_sum ))
# g2 <- ggplot(backtest_data, aes(x = datetime)) +
#   geom_line(aes(y = pr_176_sum ))
# g3 <- ggplot(backtest_data, aes(x = datetime)) +
#   geom_line(aes(y = pr_528_sum ))
# g4 <- ggplot(backtest_data, aes(x = datetime)) +
#   geom_line(aes(y = pr_1056_sum ))
# g5 <- ggplot(backtest_data, aes(x = datetime)) +
#   geom_line(aes(y = pr_2112_sum ))
# g6 <- ggplot(backtest_data, aes(x = datetime)) +
#   geom_line(aes(y = pr_4224_sum ))
# ( g1 | g2 ) / ( g3 | g4 ) / ( g5 | g6)
#
# # plots median
# g1 <- ggplot(backtest_data, aes(x = datetime)) +
#   geom_line(aes(y = pr_40_median ))
# g2 <- ggplot(backtest_data, aes(x = datetime)) +
#   geom_line(aes(y = pr_176_median ))
# g3 <- ggplot(backtest_data, aes(x = datetime)) +
#   geom_line(aes(y = pr_528_median ))
# g4 <- ggplot(backtest_data, aes(x = datetime)) +
#   geom_line(aes(y = pr_1056_median ))
# g5 <- ggplot(backtest_data, aes(x = datetime)) +
#   geom_line(aes(y = pr_2112_median ))
# g6 <- ggplot(backtest_data, aes(x = datetime)) +
#   geom_line(aes(y = pr_4224_median ))
# ( g1 | g2 ) / ( g3 | g4 ) / ( g5 | g6)
#
# # plots sd
# g1 <- ggplot(backtest_data, aes(x = datetime)) +
#   geom_line(aes(y = pr_40_skew ))
# g2 <- ggplot(backtest_data, aes(x = datetime)) +
#   geom_line(aes(y = pr_176_skew ))
# g3 <- ggplot(backtest_data, aes(x = datetime)) +
#   geom_line(aes(y = pr_528_skew ))
# g4 <- ggplot(backtest_data, aes(x = datetime)) +
#   geom_line(aes(y = pr_1056_skew ))
# g5 <- ggplot(backtest_data, aes(x = datetime)) +
#   geom_line(aes(y = pr_2112_skew ))
# g6 <- ggplot(backtest_data, aes(x = datetime)) +
#   geom_line(aes(y = pr_4224_skew ))
# ( g1 | g2 ) / ( g3 | g4 ) / ( g5 | g6)



# backtst function
backtest <- function(returns, indicator, threshold, return_cumulative = TRUE) {
  sides <- vector("integer", length(indicator))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(indicator[i-1])) {
      sides[i] <- NA
    } else if (indicator[i-1] < threshold) {
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

# backtest SMA
backtest_sma_cross <- function(returns, indicator, threshold,
                               return_cumulative = TRUE) {

  # sma series
  sma_short <- SMA(indicator, 8 * 5)
  sma_long <- SMA(indicator,  8 * 22 * 3)
  sides <- vector("integer", length(indicator))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(sma_long[i-1])) {
      sides[i] <- NA
    } else if (sma_short[i-1] > threshold & (sma_short[i-1] > sma_long[i-1])) {
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

# backtest percentiles
backtest_percentiles <- function(returns, indicator,
                                 indicator_percentil, return_cumulative = TRUE) {
  sides <- vector("integer", length(indicator))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(indicator[i-1]) || is.na(indicator_percentil[i-1])) {
      sides[i] <- NA
    } else if (indicator[i-1] > indicator_percentil[i-1]) {
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

# backtest performance
Performance <- function(x) {
  cumRetx = Return.cumulative(x)
  annRetx = Return.annualized(x, scale=252 * 8)
  sharpex = SharpeRatio.annualized(x, scale=252 * 8)
  winpctx = length(x[x > 0])/length(x[x != 0])
  annSDx = sd.annualized(x, scale=252 * 8)

  DDs <- findDrawdowns(x)
  maxDDx = min(DDs$return)
  maxLx = max(DDs$length)

  Perf = c(cumRetx, annRetx, sharpex, winpctx, annSDx, maxDDx, maxLx)
  names(Perf) = c("Cumulative Return", "Annual Return","Annualized Sharpe Ratio",
                  "Win %", "Annualized Volatility", "Maximum Drawdown", "Max Length Drawdown")
  return(Perf)
}




# UNIVARIATE PRA FORECASTING ----------------------------------------------
# TODO:tscount package !!!



# LEVEL TRADING RULE ------------------------------------------------------

# optimizations loop
thresholds <- c(seq(-5, 5, 1))
colnames(backtest_data)
# variables <- colnames(indicators)[grep("below_dummy_\\d+", colnames(indicators))]
variables <- colnames(indicators)[grep("net", colnames(indicators))]
params <- expand.grid(thresholds, variables, stringsAsFactors = FALSE)
returns_strategies <- list()
x <- vapply(1:nrow(params), function(i) backtest(backtest_data$returns,
                                                 backtest_data[, get(params[i, 2])],
                                                 # SMA(backtest_data[, get(params[i, 2])], 4),
                                                 params[i, 1]),
            numeric(1))
optim_results <- cbind(params, cum_return = x)

# inspect
tail(optim_results[order(optim_results$cum_return), ], 50)
ggplot(optim_results[grep("dummy_\\d+$", optim_results$Var2),], aes(x = cum_return)) + geom_histogram() +
  geom_vline(xintercept = 4.1, color = "red") + facet_grid(. ~ Var2)
ggplot(optim_results[grep("01", optim_results$Var2),], aes(x = cum_return)) + geom_histogram() +
  geom_vline(xintercept = 4.1, color = "red") + facet_grid(. ~ Var2)
ggplot(optim_results[grep("03", optim_results$Var2),], aes(x = cum_return)) + geom_histogram() +
  geom_vline(xintercept = 4.1, color = "red") + facet_grid(. ~ Var2)
ggplot(optim_results[grep("03", optim_results$Var2),], aes(x = cum_return)) + geom_histogram() +
  geom_vline(xintercept = 4.1, color = "red") + facet_grid(. ~ Var2)
summary_results <- as.data.table(optim_results)
summary_results[, median(cum_return), by = Var2]

# for net
unique(optim_results$Var2)
ggplot(optim_results[grep("net_40|net_176|net_528|net_1056|net_2112|net_4224", optim_results$Var2),], aes(x = cum_return)) +
  geom_histogram() +
  geom_vline(xintercept = 4.1, color = "red") + facet_grid(. ~ Var2)
ggplot(optim_results[grep("net_01|net_40|net_176|net_528|net_1056|net_2112|net_4224", optim_results$Var2),], aes(x = cum_return)) +
  geom_histogram() +
  geom_vline(xintercept = 4.1, color = "red") + facet_grid(. ~ Var2)
ggplot(optim_results[grep("net_03|net_05", optim_results$Var2),], aes(x = cum_return)) + geom_histogram() +
  geom_vline(xintercept = 4.1, color = "red") + facet_grid(. ~ Var2)


# backtest individual
strategy_returns <- backtest(backtest_data$returns, backtest_data$pr_below_dummy_net_176, -15, return_cumulative = FALSE)
charts.PerformanceSummary(xts(cbind(backtest_data$returns, strategy_returns), order.by = backtest_data$datetime))
charts.PerformanceSummary(xts(cbind(backtest_data$returns, strategy_returns)[30000:length(strategy_returns), ],
                              order.by = backtest_data$datetime[30000:length(strategy_returns)]))
Performance(xts(strategy_returns, order.by = backtest_data$datetime))
Performance(xts(backtest_data$returns, order.by = backtest_data$datetime))




# VECTORBT BACKTEST -------------------------------------------------------



# INDIVIDUAL BACKTEST -----------------------------------------------------
# prepare data for backtest
sample_ <- close_data[symbol == "SPY"]
colnames(sample_)
sample_ <- sample_[, .(datetime, close, pr_above_dummy_95_528, pr_below_dummy_05_528)]
sample_[, returns := close / shift(close) - 1]
sample_ <- na.omit(sample_)
sample_[, signal := ifelse(pr_above_dummy_95_528 == 1, 1, NA)]
sample_[, signal := ifelse(pr_below_dummy_05_528 == 1, 0, signal)]

# calcualte signals
signals <- vector("numeric", nrow(sample_))
signal_ <- sample_$signal
n_ <- 8 * 4
for (i in seq_along(signal_)) {
  if (i %in% 1:n_) {
    signals[i] <- NA
  } else if (any(signal_[(i-n_):(i-1)] == 1, na.rm = TRUE)) {
    # } else if (any(signal_[(i-n_):(i-1)] == 1, na.rm = TRUE) &
    #            (sum(signal_[(i-n_):(i-1)] == 1, na.rm = TRUE) > sum(signal_[(i-n_):(i-1)] == 0, na.rm = TRUE))) {
    # } else if (sum(signal_[(i-15):(i-1)] == 0, na.rm = TRUE) > 2) {
    signals[i] <- 1
  } else {
    signals[i] <- 0
  }
}
sample_[, hold := signals]
sample_[, strategy := hold * returns]
sample_[, strategy := nafill(strategy, fill = 0)]

# backtst results
Return.cumulative(as.xts.data.table(sample_[, .(datetime, returns, strategy)]))
charts.PerformanceSummary(as.xts.data.table(sample_[, .(datetime, returns, strategy)]))
# charts.PerformanceSummary(as.xts.data.table(sample_[1:5000, .(datetime, returns, strategy)]))
# charts.PerformanceSummary(as.xts.data.table(sample_[5000:10000, .(datetime, returns, strategy)]))
# charts.PerformanceSummary(as.xts.data.table(sample_[10000:15000, .(datetime, returns, strategy)]))
Performance(as.xts.data.table(sample_[, .(datetime, returns)]))
Performance(as.xts.data.table(sample_[, .(datetime, strategy)]))


# SMA CROSS TRADING RULE ------------------------------------------------------

# # optimizations loop
# thresholds <- c(seq(0, 50, 2))
# variables <- "below_dummy_sum"
# params <- expand.grid(thresholds, variables, stringsAsFactors = FALSE)
# returns_strategies <- list()
# x <- vapply(1:nrow(params), function(i) backtest_sma_cross(backtest_data$returns,
#                                                            backtest_data[, get(params[i, 2])],
#                                                            params[i, 1]),
#             numeric(1))
# optim_results <- cbind(params, cum_return = x)
#
# # backtest individual
# strategy_returns <- backtest(backtest_data$returns, backtest_data$below_dummy_sum, 10, return_cumulative = FALSE)
# charts.PerformanceSummary(xts(cbind(backtest_data$returns, strategy_returns), order.by = backtest_data$datetime))



# PERCENTILES TRADING RULE ------------------------------------------------

# # calculate percentiles
# backtest_data_ <- copy(backtest_data)
#
# # optimization loop
# p <- seq(0.7, 0.99, 0.01)
# roll_width <- c(8 * 22 * seq(1, 12, 1), 8 * 22 * 12 * seq(2, 6, 1))
# params <- expand.grid(p, roll_width, stringsAsFactors = FALSE)
# plan(multicore(workers = 8))
# returns_strategies <- list()
# x <- future_lapply(1:nrow(params), function(i) {
#
#   # calculate percentiles
#   cols <- colnames(backtest_data_)[4:(ncol(backtest_data_))]
#   cols_new <- paste0('p_', cols)
#   backtest_data_[, (cols_new) := lapply(.SD, function(x) roll::roll_quantile(x, params[i, 2], p = params[i, 1])),
#                 .SDcols = cols]
#   backtests_z <- list()
#   for (z in 1:length(cols)) {
#     col_ <- cols[z]
#     col_new_ <- cols_new[z]
#     backtets <- backtest_percentiles(backtest_data_$returns,
#                                      unlist(backtest_data_[, ..col_]),
#                                      unlist(backtest_data_[, ..col_new_]))
#     backtests_z[[z]] <- cbind.data.frame(col_, backtets)
#   }
#   backtests_p <- rbindlist(backtests_z)
#   backtests_p <- cbind(backtests_p, params[i, ])
#
#   # remove percentiles from sample_
#   remove_cols <- which(colnames(backtest_data_) %in% cols_new)
#   backtest_data_[, (remove_cols) := NULL]
#
#   # return
#   backtests_p
# })
# optimization_results <- rbindlist(x)
# setorder(optimization_results, "backtets")
# # fwrite(optimization_results, paste0("D:/risks/radfagg_optimization/optimization_results-", Sys.Date(), ".csv"))
#
# # optimization summary
# vars_ <- unique(optimization_results$col_)
# vars_ <- vars_[!grepl("p_|returns", vars_)]
# optimization_results_summary <- optimization_results[col_ %in% vars_]
# head(optimization_results_summary[order(optimization_results_summary$backtets, decreasing = TRUE), ], 30)
# opt_summary <- optimization_results_summary[, .(median_score = median(backtets),
#                                                 mean_score = mean(backtets))]
# opt_summary[order(median_score, decreasing = TRUE)]
#
# # results by variable
# ggplot(optimization_results_summary, aes(x = backtets)) + geom_histogram() +
#   geom_vline(xintercept = 4.1, color = "red") + facet_grid(. ~ col_)
# ggplot(optimization_results[col_ == "below_dummy_sum"], aes(Var1, Var2, fill= backtets)) +
#   geom_tile()
# ggplot(optimization_results[col_ == "below_dummy_sum" & Var2 < 1500], aes(Var1, Var2, fill= backtets)) +
#   geom_tile()
#
# # heatmaps
# ggplot(optimization_results, aes(Var1, Var2, fill= backtets)) +
#   geom_tile()
# # heatmaps for best variable
# optimization_results_best <- optimization_results[col_ == "below_dummy_sum"]
# ggplot(optimization_results_best, aes(Var1, Var2, fill= backtets)) +
#   geom_tile()
# ggplot(optimization_results_best[grep("200", id)], aes(Var1, id, fill= backtets)) +
#   geom_tile()
#
# # backtest individual percentiles
# sample_ <- copy(backtest_data) #  & datetime %between% c('2020-01-01', '2021-01-01')]
# cols <- colnames(sample_)[4:(ncol(sample_))]
# cols_new <- paste0('p_', cols)
# sample_[, (cols_new) := lapply(.SD, function(x) roll::roll_quantile(x, 5000, p = 0.90)), .SDcols = cols]
# strategy_returns <- backtest_percentiles(sample_$returns, sample_$below_dummy_sum, sample_$p_below_dummy_sum, return_cumulative = FALSE)
# charts.PerformanceSummary(xts(cbind(sample_$returns, strategy_returns), order.by = sample_$datetime))
# Performance(xts(strategy_returns, order.by = sample_$datetime))
# Performance(xts(sample_$returns, order.by = sample_$datetime))
#
# ind <- 1:15000
# charts.PerformanceSummary(xts(cbind(sample_$returns[ind], strategy_returns[ind]), order.by = sample_$datetime[ind]))
# ind <- 15000:20000
# charts.PerformanceSummary(xts(cbind(sample_$returns[ind], strategy_returns[ind]), order.by = sample_$datetime[ind]))
# ind <- 20000:25000
# charts.PerformanceSummary(xts(cbind(sample_$returns[ind], strategy_returns[ind]), order.by = sample_$datetime[ind]))



# LONG SHORT TREND --------------------------------------------------------
backtest_data


# SAVE DATA ---------------------------------------------------------------
# save data to blob
KEY = Sys.getenv("BLOB-KEY-SNP")
ENDPOINT = Sys.getenv("BLOB-ENDPOINT-SNP")
cols_keep <- c("time", "pr_below_dummy_40", "pr_below_dummy_176",
               "pr_below_dummy_528", "pr_below_dummy_1056", "pr_below_dummy_2112", "pr_below_dummy_4224")
# "pr_below_dummy_01_176", "pr_below_dummy_01_1056", "pr_below_dummy_01_2112", "pr_below_dummy_03_528",
# "pr_below_dummy_net_40", "pr_below_dummy_net_176", "pr_below_dummy_net_528",
# "pr_below_dummy_net_1056", "pr_below_dummy_net_2112", "pr_below_dummy_net_4224")
qc_backtest <- backtest_data[, ..cols_keep]
cols <- setdiff(cols_keep, "time")
qc_backtest[, (cols) := lapply(.SD, shift), .SDcols = cols] # VERY IMPORTANT STEP !
qc_backtest <- na.omit(qc_backtest)
qc_backtest[, time := as.character(time)]
file_name <- paste0("pr500-", format(Sys.time(), format = "%Y%m%d%H%M%S"), ".csv")
bl_endp_key <- storage_endpoint(ENDPOINT, key=KEY)
cont <- storage_container(bl_endp_key, "qc-backtest")
storage_write_csv(qc_backtest, cont, file_name)
# save for SP100
qc_backtest_100 <- backtest_data_100[, ..cols_keep]
cols <- setdiff(cols_keep, "time")
qc_backtest_100[, (cols) := lapply(.SD, shift), .SDcols = cols] # VERY IMPORTANT STEP !
qc_backtest_100 <- na.omit(qc_backtest_100)
qc_backtest_100[, time := as.character(time)]
file_name <- paste0("pr100-", format(Sys.time(), format = "%Y%m%d%H%M%S"), ".csv")
storage_write_csv(qc_backtest_100, cont, file_name)



# FEATURES IMPORTANCE -----------------------------------------------------
# prepare data
# sample_ <- close_data[symbol == "AAPL"]
sample_ <- copy(backtest_data)
keep_cols <- names(which(colMeans(!is.na(as.matrix(sample_))) > 0.9))
print(paste0("Removing coliumns with many NA values: ", setdiff(colnames(backtest_data), keep_cols)))
sample_ <- sample_[, ..keep_cols]
X_lags <- sample_[, shift(.SD, 1:3), .SDcols = colnames(sample_)[4:ncol(sample_)]]
setnames(X_lags, apply(expand.grid(colnames(sample_)[4:ncol(sample_)], "_lag_", 1:3, stringsAsFactors = FALSE), 1, paste0, collapse = ""))
X <- cbind(sample_$returns, X_lags)
X <- na.omit(X)
X <- as.matrix(X)

# f1st
f1st_fi <- f1st(X[, 1, drop = FALSE], X[, -1], kmn = 10, sub = TRUE)
cov_index_f1st <- colnames(X[, -ncol(X)])[f1st_fi[[1]][, 1]]

# f3st_1
f3st_1 <- f3st(X[, 1, drop = FALSE], X[, -1], m = 1, kmn = 10)
cov_index_f3st_1 <- unique(as.integer(f3st_1[[1]][1, ]))[-1]
cov_index_f3st_1 <- cov_index_f3st_1[cov_index_f3st_1 != 0]
cov_index_f3st_1 <- colnames(X[, -ncol(X)])[cov_index_f3st_1]

# data
X <- as.data.table(sample_)
X[, returns := close / shift(close) - 1]

# ABESS
library(abess)
X_ <- as.data.frame(X)
# abess_fit <- abess(returns ~ ., data = X_, support.size = 3)
# abess_fit <- abess(returns ~ ., data = X, support.size = 3)
abess_fit <- abess(X_[, -1], X_[, 1], support.size = 3)
head(coef(abess_fit, sparse = FALSE), 50)
colnames(X_[, which(coef(abess_fit, sparse = FALSE) > 0) + 1] )


# VAR ---------------------------------------------------------------------
# prepare dataset
# keep_cols_var <- cov_index_f1st[grepl(".*", cov_index_f1st)]
keep_cols_var <- colnames(backtest_data)[grepl("pr_below_dummy_40|pr_below_dummy_176", colnames(backtest_data))]
# keep_cols_var <- c("pr_below_dummy_net_03974224", "pr_below_dummy_net_059540")
# keep_cols_var <- c("pr_below_dummy_net_0199176", "pr_above_dummy_95_1056", "pr_above_dummy_40", "pr_below_dummy_net_0397176",
#                    "pr_below_dummy_net_4224", "pr_below_dummy_net_0595528", "pr_below_dummy_net_05951056")
keep_cols_var <- c("datetime", "returns", keep_cols_var)
X <- backtest_data[, ..keep_cols_var]
X <- na.omit(X)
head(X, 5)

# predictions for every period
roll_var <- runner(
  x = X,
  f = function(x) {
    # x <- X[1:300, 1:ncol(X)]
    y <- as.data.frame(x[, 2:ncol(x)])
    y <- y[, which(apply(y, 2 , sd) != 0), drop = FALSE]
    if(length(y) == 1) print("STOP!")
    res <- VAR(y, lag.max = 4, type = "both")
    # coef(res)
    # summary(res)
    p <- predict(res)
    p_last <- p$fcst[[1]][1, 1]
    data.frame(prediction = p_last)
  },
  k = 8 * 22 * 6,
  lag = 0L,
  na_pad = TRUE
)
predictions_var <- lapply(roll_var, as.data.table)
predictions_var <- rbindlist(predictions_var, fill = TRUE)
predictions_var <- cbind(datetime = X[, datetime], predictions_var)
predictions_var <- merge(X, predictions_var, by = "datetime", all.x = TRUE, all.y = FALSE)
predictions_var <- na.omit(predictions_var)
tail(predictions_var, 10)

# backtest apply
backtest_var <- function(returns, indicator, threshold, return_cumulative = TRUE) {
  sides <- vector("integer", length(indicator))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(indicator[i-1])) {
      sides[i] <- NA
    } else if (indicator[i-1] < threshold) { #  & indicator_2[i-1] > 1
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
Return.cumulative(predictions_var$returns)
backtest_var(predictions_var$returns, predictions_var$V1, 0)
x <- backtest_var(predictions_var$returns, predictions_var$V1, 0, FALSE)
charts.PerformanceSummary(as.xts(cbind(predictions_var$returns, x), order.by = predictions_var$datetime))
# subsample
charts.PerformanceSummary(as.xts(cbind(predictions_var$returns, x), order.by = predictions_var$datetime)[1:5000])
charts.PerformanceSummary(as.xts(cbind(predictions_var$returns, x), order.by = predictions_var$datetime)[5000:8000])
charts.PerformanceSummary(as.xts(cbind(predictions_var$returns, x), order.by = predictions_var$datetime)[8000:length(x)])



# TORCH -------------------------------------------------------------------
# load packages
library(torch)
torch::cuda_is_available()


# parameters
n_timesteps = 8 * 5
n_forecast = 1
batch_size = 32

# define train, validation and test set
# features_cols <- colnames(backtest_data[, .(pr_below_dummy_40, pr_below_dummy_176,
#                                             pr_below_dummy_528, pr_below_dummy_1056,
#                                             pr_below_dummy_01_40, pr_below_dummy_01_176,
#                                             pr_below_dummy_01_528, pr_below_dummy_01_1056)])
cols <- c("datetime", "returns", "pr_below_dummy_40", "pr_below_dummy_176",
          "pr_below_dummy_01_40", "pr_below_dummy_01_176", "pr_below_dummy_03_40", "pr_below_dummy_03_176") # cov_index_f1st
X <- backtest_data[, ..cols]
X <- X[100:nrow(X), ]
X_train <- as.matrix(X[1:as.integer((nrow(X) * 0.7)), .SD, .SDcols = !c("datetime")])
X_validation <- as.matrix(X[(nrow(X_train)+1):as.integer((nrow(X) * 0.85)), .SD, .SDcols = !c("datetime") ])
X_test <- as.matrix(X[(as.integer((nrow(X) * 0.85))+1):nrow(X), .SD, .SDcols = !c("datetime") ])
X_test_dates <- X[(as.integer((nrow(X) * 0.85))+1 + (n_timesteps + 1)):nrow(X), .SD, .SDcols = c("datetime") ]

# util values for scaling IGNORE FOR NOW
return_mean <- mean(X_train[, 1])
return_sd <- sd(X_train[, 1])

# torch dataset class
pra_dataset <- dataset(
  name = "pra_dataset",

  initialize = function(x, n_timesteps, sample_frac = 1) {

    self$n_timesteps <- n_timesteps
    self$x <- torch_tensor((x - train_mean) / train_sd)

    n <- length(self$x) - self$n_timesteps - 1

    self$starts <- sort(sample.int(
      n = n,
      size = n * sample_frac
    ))

  },
  # initialize = function(x, n_timesteps, n_forecast, sample_frac = 1) {
  #   self$n_timesteps <- n_timesteps
  #   self$n_forecast <- n_forecast
  #   self$x <- torch_tensor(x)
  #
  #   n <- nrow(self$x) - self$n_timesteps - self$n_forecast + 1
  #
  #   # it seems to me this is relevant only if we use sample of train sample
  #   self$starts <- sort(sample.int(
  #     n = n,
  #     size = n * sample_frac
  #   ))
  # },

  .getitem = function(i) {

    start <- self$starts[i]
    end <- start + self$n_timesteps - 1
    lag <- 1

    list(
      x = self$x[start:end],
      y = self$x[(start+lag):(end+lag)]$squeeze(2)
    )

  },
  #   .getitem = function(i) {
  #
  #   start <- self$starts[i]
  #   end <- start + self$n_timesteps - 1
  #   pred_length <- self$n_forecast
  #
  #   list(
  #     # x = self$x[start:end, 2:ncol(X_train)],
  #     x = self$x[start:end,],
  #     y = self$x[(end + 1):(end + pred_length), 1] # target column is in first column in the taable
  #     )
  # },

  .length = function() {
    length(self$starts)
  }
)

# define train, validation and tset sets for the model
train_ds <- pra_dataset(X_train, n_timesteps, n_forecast, sample_frac = 1)
train_dl <- train_ds %>% dataloader(batch_size = batch_size, shuffle = TRUE)
iter <- train_dl$.iter()
b <- iter$.next()
dim(b$x)
dim(b$y)

valid_ds <- pra_dataset(X_validation, n_timesteps, n_forecast, sample_frac = 1)
valid_dl <- valid_ds %>% dataloader(batch_size = batch_size)

test_ds <- pra_dataset(X_test, n_timesteps, n_forecast)
test_dl <- test_ds %>% dataloader(batch_size = 1)

# model
model <- nn_module(

  initialize = function(type, input_size, hidden_size, linear_size, output_size,
                        num_layers = 1, dropout = 0, linear_dropout = 0) {

    self$type <- type
    self$num_layers <- num_layers
    self$linear_dropout <- linear_dropout

    self$rnn <- if (self$type == "gru") {
      nn_gru(
        input_size = input_size,
        hidden_size = hidden_size,
        num_layers = num_layers,
        dropout = dropout,
        batch_first = TRUE
      )
    } else {
      nn_lstm(
        input_size = input_size,
        hidden_size = hidden_size,
        num_layers = num_layers,
        dropout = dropout,
        batch_first = TRUE
      )
    }

    # self$mlp <- nn_sequential(
    #   nn_linear(hidden_size, linear_size),
    #   nn_dropout(linear_dropout),
    #   nn_linear(linear_size, output_size)
    # )

    self$output <- nn_linear(hidden_size, 1)
  },

  forward = function(x) {

    # x <- self$rnn(x)
    # x[[1]][ ,-1, ..] %>%
    #   self$mlp

    x <- self$rnn(x)[[1]]
    x <- x[ , -1, ..]
    x %>% self$output()
  }
)

# model instantiation
net <- model(
  "lstm", input_size = ncol(X_train), num_layers = 1, hidden_size = 8, linear_size = 512,
  output_size = n_forecast, linear_dropout = 0, dropout = 0.2
)
device <- torch_device(if (cuda_is_available()) "cuda" else "cpu")
net <- net$to(device = device)

# training optimizers and losses
optimizer <- optim_adam(net$parameters, lr = 0.001)
num_epochs <- 10

train_batch <- function(b) {

  optimizer$zero_grad() # If we perform optimization in a loop, we need to make sure to call optimizer$zero_grad() on every step, as otherwise gradients would be accumulated
  output <- net(b$x$to(device = device))
  target <- b$y$to(device = device)

  loss <- nnf_mse_loss(output, target)
  loss$backward() # backward pass calculates the gradients, but does not update the parameters
  optimizer$step() # optimizer actually performs the updates

  loss$item()
}

valid_batch <- function(b) {

  output <- net(b$x$to(device = device))
  target <- b$y$to(device = device)

  loss <- nnf_mse_loss(output, target)
  loss$item()
}

# training loop
for (epoch in 1:num_epochs) {

  net$train()
  train_loss <- c()

  coro::loop(for (b in train_dl) {
    loss <- train_batch(b)
    train_loss <- c(train_loss, loss)
  })

  cat(sprintf("\nEpoch %d, training: loss: %3.5f \n", epoch, mean(train_loss)))

  net$eval()
  valid_loss <- c()

  coro::loop(for (b in valid_dl) {
    loss <- valid_batch(b)
    valid_loss <- c(valid_loss, loss)
  })

  cat(sprintf("\nEpoch %d, validation: loss: %3.5f \n", epoch, mean(valid_loss)))
}

# evaluation
net$eval()
test_preds <- vector(mode = "list", length = length(test_dl))

i <- 1

coro::loop(for (b in test_dl) {

  input <- b$x
  output <- net(input$to(device = device))
  output <- output$to(device = "cpu")
  preds <- as.numeric(output)

  test_preds[[i]] <- preds
  i <<- i + 1
})

# plot test predictipon
predictions <- data.frame(predictions = unlist(test_preds))
predictions$sign <- ifelse(predictions >= 0, 1, 0)
predictions <- cbind(returns = X_test[(n_timesteps + 1):nrow(X_test), "returns"], predictions)
predictions$returns_strategy <- predictions$returns * predictions$sign
predictions <- predictions[, c("returns", "returns_strategy")]
head(predictions)
X_test_predictions <- xts(predictions[-nrow(predictions), ], order.by = X_test_dates[[1]])
PerformanceAnalytics::Return.cumulative(X_test_predictions)
PerformanceAnalytics::charts.PerformanceSummary(X_test_predictions)



# TORCH SEQ2SEQ -----------------------------------------------------------------
# load packages
library(torch)
torch::cuda_is_available()


# parameters
n_timesteps = 8 * 5
n_forecast = 1
batch_size = 32

cols <- c("datetime", "returns", "pr_below_dummy_40", "pr_below_dummy_176",
          "pr_below_dummy_01_40", "pr_below_dummy_01_176", "pr_below_dummy_03_40", "pr_below_dummy_03_176") # cov_index_f1st
X <- backtest_data[, ..cols]
X <- X[100:nrow(X), ]
X_train <- as.matrix(X[1:as.integer((nrow(X) * 0.7)), .SD, .SDcols = !c("datetime")])
X_validation <- as.matrix(X[(nrow(X_train)+1):as.integer((nrow(X) * 0.85)), .SD, .SDcols = !c("datetime") ])
X_test <- as.matrix(X[(as.integer((nrow(X) * 0.85))+1):nrow(X), .SD, .SDcols = !c("datetime") ])
X_test_dates <- X[(as.integer((nrow(X) * 0.85))+1 + (n_timesteps + 1)):nrow(X), .SD, .SDcols = c("datetime") ]

# util values for scaling IGNORE FOR NOW
return_mean <- mean(X_train[, 1])
return_sd <- sd(X_train[, 1])

# torch dataset class
pra_dataset <- dataset(
  name = "pra_dataset",

  initialize = function(x, n_timesteps, sample_frac = 1) {

    self$n_timesteps <- n_timesteps
    self$x <- torch_tensor(x)

    n <- length(self$x) - self$n_timesteps - 1

    self$starts <- sort(sample.int(
      n = n,
      size = n * sample_frac
    ))

  },

  .getitem = function(i) {

    start <- self$starts[i]
    end <- start + self$n_timesteps - 1
    lag <- 1

    list(
      x = self$x[start:end, ],
      y = self$x[(start+lag):(end+lag), 1]
    )

  },

  #   .getitem = function(i) {
  #
  #   start <- self$starts[i]
  #   end <- start + self$n_timesteps - 1
  #   pred_length <- self$n_forecast
  #
  #   list(
  #     # x = self$x[start:end, 2:ncol(X_train)],
  #     x = self$x[start:end,],
  #     y = self$x[(end + 1):(end + pred_length), 1] # target column is in first column in the taable
  #     )
  # },

  .length = function() {
    length(self$starts)
  }
)


x = torch_tensor(X_train)
x$dim()
x = x[1:10, 1, drop = FALSE]
x$dim()
x$squeeze(2)


# define train, validation and tset sets for the model
train_ds <- pra_dataset(X_train, n_timesteps, sample_frac = 1)
train_dl <- train_ds %>% dataloader(batch_size = batch_size, shuffle = TRUE)
iter <- train_dl$.iter()
b <- iter$.next()
dim(b$x)
dim(b$y)

valid_ds <- pra_dataset(X_validation, n_timesteps, sample_frac = 1)
valid_dl <- valid_ds %>% dataloader(batch_size = batch_size)

test_ds <- pra_dataset(X_test, n_timesteps)
test_dl <- test_ds %>% dataloader(batch_size = 1)

# model
encoder_module <- nn_module(

  initialize = function(type, input_size, hidden_size, num_layers = 1, dropout = 0) {

    self$type <- type

    self$rnn <- if (self$type == "gru") {
      nn_gru(
        input_size = input_size,
        hidden_size = hidden_size,
        num_layers = num_layers,
        dropout = dropout,
        batch_first = TRUE
      )
    } else {
      nn_lstm(
        input_size = input_size,
        hidden_size = hidden_size,
        num_layers = num_layers,
        dropout = dropout,
        batch_first = TRUE
      )
    }

  },

  forward = function(x) {

    x <- self$rnn(x)

    # return last states for all layers
    # per layer, a single tensor for GRU, a list of 2 tensors for LSTM
    x <- x[[2]]
    x

  }

)

decoder_module <- nn_module(

  initialize = function(type, input_size, hidden_size, num_layers = 1) {

    self$type <- type

    self$rnn <- if (self$type == "gru") {
      nn_gru(
        input_size = input_size,
        hidden_size = hidden_size,
        num_layers = num_layers,
        batch_first = TRUE
      )
    } else {
      nn_lstm(
        input_size = input_size,
        hidden_size = hidden_size,
        num_layers = num_layers,
        batch_first = TRUE
      )
    }

    self$linear <- nn_linear(hidden_size, 1)

  },

  forward = function(x, state) {

    # input to forward:
    # x is (batch_size, 1, 1)
    # state is (1, batch_size, hidden_size)
    x <- self$rnn(x, state)

    # break up RNN return values
    # output is (batch_size, 1, hidden_size)
    # next_hidden is
    c(output, next_hidden) %<-% x

    output <- output$squeeze(2)
    output <- self$linear(output)

    list(output, next_hidden)

  }

)

seq2seq_module <- nn_module(

  initialize = function(type, input_size, hidden_size, n_forecast, num_layers = 1, encoder_dropout = 0) {

    self$encoder <- encoder_module(type = type, input_size = input_size,
                                   hidden_size = hidden_size, num_layers, encoder_dropout)
    self$decoder <- decoder_module(type = type, input_size = input_size,
                                   hidden_size = hidden_size, num_layers)
    self$n_forecast <- n_forecast

  },

  forward = function(x, y, teacher_forcing_ratio) {

    # prepare empty output
    outputs <- torch_zeros(dim(x)[1], self$n_forecast)$to(device = device)

    # encode current input sequence
    hidden <- self$encoder(x)

    # prime decoder with final input value and hidden state from the encoder
    out <- self$decoder(x[ , n_timesteps, , drop = FALSE], hidden)

    # decompose into predictions and decoder state
    # pred is (batch_size, 1)
    # state is (1, batch_size, hidden_size)
    c(pred, state) %<-% out

    # store first prediction
    outputs[ , 1] <- pred$squeeze(2)

    # iterate to generate remaining forecasts
    for (t in 2:self$n_forecast) {

      # call decoder on either ground truth or previous prediction, plus previous decoder state
      teacher_forcing <- runif(1) < teacher_forcing_ratio
      input <- if (teacher_forcing == TRUE) y[ , t - 1, drop = FALSE] else pred
      input <- input$unsqueeze(3)
      out <- self$decoder(input, state)

      # again, decompose decoder return values
      c(pred, state) %<-% out
      # and store current prediction
      outputs[ , t] <- pred$squeeze(2)
    }
    outputs
  }

)


# model instantiation
net <- seq2seq_module("gru", input_size = 1, hidden_size = 32, n_forecast = n_forecast)

# training RNNs on the GPU currently prints a warning that may clutter
# the console
# see https://github.com/mlverse/torch/issues/461
# alternatively, use
# device <- "cpu"
device <- torch_device(if (cuda_is_available()) "cuda" else "cpu")

net <- net$to(device = device)

# training optimizers and losses
optimizer <- optim_adam(net$parameters, lr = 0.001)
num_epochs <- 10

train_batch <- function(b, teacher_forcing_ratio) {

  optimizer$zero_grad()
  output <- net(b$x$to(device = device), b$y$to(device = device), teacher_forcing_ratio)
  target <- b$y$to(device = device)

  loss <- nnf_mse_loss(output, target)
  loss$backward()
  optimizer$step()

  loss$item()

}

valid_batch <- function(b, teacher_forcing_ratio = 0) {

  output <- net(b$x$to(device = device), b$y$to(device = device), teacher_forcing_ratio)
  target <- b$y$to(device = device)

  loss <- nnf_mse_loss(output, target)

  loss$item()

}

# training loop
for (epoch in 1:num_epochs) {

  net$train()
  train_loss <- c()

  coro::loop(for (b in train_dl) {
    loss <-train_batch(b, teacher_forcing_ratio = 0.3)
    train_loss <- c(train_loss, loss)
  })

  cat(sprintf("\nEpoch %d, training: loss: %3.5f \n", epoch, mean(train_loss)))

  net$eval()
  valid_loss <- c()

  coro::loop(for (b in valid_dl) {
    loss <- valid_batch(b)
    valid_loss <- c(valid_loss, loss)
  })

  cat(sprintf("\nEpoch %d, validation: loss: %3.5f \n", epoch, mean(valid_loss)))
}

# evaluation
net$eval()
test_preds <- vector(mode = "list", length = length(test_dl))

i <- 1

coro::loop(for (b in test_dl) {

  input <- b$x
  output <- net(input$to(device = device))
  output <- output$to(device = "cpu")
  preds <- as.numeric(output)

  test_preds[[i]] <- preds
  i <<- i + 1
})

# plot test predictipon
predictions <- data.frame(predictions = unlist(test_preds))
predictions$sign <- ifelse(predictions >= 0, 1, 0)
predictions <- cbind(returns = X_test[(n_timesteps + 1):nrow(X_test), "returns"], predictions)
predictions$returns_strategy <- predictions$returns * predictions$sign
predictions <- predictions[, c("returns", "returns_strategy")]
head(predictions)
X_test_predictions <- xts(predictions[-nrow(predictions), ], order.by = X_test_dates[[1]])
PerformanceAnalytics::Return.cumulative(X_test_predictions)
PerformanceAnalytics::charts.PerformanceSummary(X_test_predictions)


# predictions for every period
roll_var <- runner(
  x = X,
  f = function(x) {
    # x <- X[1:300, 1:ncol(X)]
    y <- as.data.frame(x[, 2:ncol(x)])
    y <- y[, which(apply(y, 2 , sd) != 0), drop = FALSE]
    if(length(y) == 1) print("STOP!")
    res <- lm("returns ~ .^2", data = y)
    coef(res)
    summary(res)
    p <- predict(res)
    p_last <- tail(p, 1)
    data.frame(prediction = p_last)
  },
  k = 8 * 66,
  lag = 0L,
  na_pad = TRUE
)
predictions_var <- lapply(roll_var, as.data.table)
predictions_var <- rbindlist(predictions_var, fill = TRUE)
predictions_var <- cbind(datetime = X[, datetime], predictions_var)
predictions_var <- merge(X, predictions_var, by = "datetime", all.x = TRUE, all.y = FALSE)
predictions_var <- na.omit(predictions_var)
tail(predictions_var, 10)

# backtest apply
backtest_var <- function(returns, indicator, threshold, indicator_2, return_cumulative = TRUE) {
  sides <- vector("integer", length(indicator))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(indicator[i-1]) || is.na(indicator_2[i-1])) {
      sides[i] <- NA
    } else if (indicator[i-1] < threshold | indicator_2[i-1] > 5) { #  & indicator_2[i-1] > 1
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
Return.cumulative(predictions_var$returns)
backtest_var(predictions_var$returns, predictions_var$V1, 0, predictions_var$pr_below_dummy_528)
x <- backtest_var(predictions_var$returns, predictions_var$V1, 0, predictions_var$pr_below_dummy_528, FALSE)
charts.PerformanceSummary(as.xts(cbind(predictions_var$returns, x), order.by = predictions_var$datetime))
# subsample
charts.PerformanceSummary(as.xts(cbind(predictions_var$returns, x), order.by = predictions_var$datetime)[1:5000])
charts.PerformanceSummary(as.xts(cbind(predictions_var$returns, x), order.by = predictions_var$datetime)[5000:8000])
charts.PerformanceSummary(as.xts(cbind(predictions_var$returns, x), order.by = predictions_var$datetime)[8000:length(x)])




# SPY SOLO ----------------------------------------------------------------
# data
sample_ <- close_data[symbol == "AAPL"]
sample_ <- sample_[, 1:9]
sample_[, returns := close / shift(close) - 1]
sample_ <- na.omit(sample_)
cols_ <- colnames(indicators)[c(1, 8:ncol(indicators))]
sample_ <- merge(sample_, indicators[, ..cols_], by = "datetime", all.x = TRUE, all.y = FALSE)

# calcualte signals
signal_ <- sample_$pr_176
signal_2 <- sample_$pr_2112
signal_3 <- nafill(sample_$pr_below_dummy_40, fill = 0)
signals <- vector("numeric", nrow(sample_))
for (i in seq_along(signal_)) {
  if (i == 1 || is.na(signal_[i-1])) {
    signals[i] <- NA
    # } else if (signal_3[i-1]  > 0 & (signal_[i-1] < 0.15 | signal_2[i-1] < 0.15)) {
  } else if ((signal_[i-1] < 0.15 | signal_2[i-1] < 0.15)) {
    signals[i] <- 0
  } else {
    signals[i] <- 1
  }
}
sample_[, strategy := signals * returns]

# backtst results
Return.cumulative(as.xts.data.table(sample_[, .(datetime, returns, strategy)]))
charts.PerformanceSummary(as.xts.data.table(sample_[, .(datetime, returns, strategy)]))
# charts.PerformanceSummary(as.xts.data.table(sample_[1:5000, .(datetime, returns, strategy)]))
# charts.PerformanceSummary(as.xts.data.table(sample_[5000:10000, .(datetime, returns, strategy)]))
# charts.PerformanceSummary(as.xts.data.table(sample_[10000:15000, .(datetime, returns, strategy)]))
Performance(as.xts.data.table(sample_[, .(datetime, returns)]))


# AUDREX ---------------------------------------------------------------------
# library(audrex)
# # prepare dataset
# keep_cols_var <- colnames(backtest_data)[grepl("below_dummy_40|below_dummy_176|below_dummy_528", colnames(backtest_data))]
# keep_cols_var <- c("datetime", "returns", keep_cols_var)
# X <- backtest_data[, ..keep_cols_var]
# X <- na.omit(X)
# head(X, 50)
#
#
#
# fixed_date_format <- seq.Date(from = as.Date("1993-01-01"), to = as.Date("2015-02-01"), length.out = 266)###IF YOU WANT TO ADD DATES, BE SURE TO HAVE A CLEAR FORMAT
#
# example1 <- audrex(climate_anomalies[, c("GMTA", "GMSL")], n_sample = 1, n_search = 0, seq_len = 15, n_windows = 5,  dates = fixed_date_format, booster = "gbtree", min_set = 10, max_depth = 10, min_child_weight = 0, eta = 1, gamma = 0, subsample = 0.5, colsample_bytree = 0.5, norm = T, n_dim = 2)
#
#
# example1$models
# names(example1$best_model)
# example1$best_model[[4]]
# names(example1$models)



# NONLINEAR TS ------------------------------------------------------------

library(tsDyn)

data(lynx)
grid<-selectSETAR(lynx, m=1, thDelay=0, trim=0.15, criterion="SSR")
set<-setar(lynx, m=1, thDelay=0, th=grid$th)
summary(set)
predict(set)





