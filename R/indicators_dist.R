library(data.table)
library(httr)
library(leanr)
library(QuantTools)
library(ggplot2)
library(TTR)
library(PerformanceAnalytics)
library(equityData)
library(AzureStor)
library(future.apply)
library(patchwork)



# PARAMETERS
frequency = "hour"
windows = c(8 * 22, 8 * 22 * 3, 8 * 22 * 6,  8 * 22 * 12, 8 * 22 * 12 * 2, 8 * 22 * 12 * 5)

# import data
sp500_stocks <- GET("https://financialmodelingprep.com/api/v3/sp500_constituent?apikey=15cd5d0adf4bc6805a724b4417bbaafc")
sp500_stocks <- rbindlist(httr::content(sp500_stocks))
if (frequency == "minute") {
  start_time <- Sys.time()
  market_data <- get_market_equities_minutes('D:/market_data/equity/usa/minute', unique(sp500_stocks$symbol)[1:2])
  end_time <- Sys.time()
  end_time - start_time
  market_data <- market_data[datetime %between% c("09:30:00", "16:00:00")]
  market_data[, time := format.POSIXct(datetime, "%H:%M:%S")]
} else if (frequency == "hour") {
  market_data <- import_lean("D:/market_data/equity/usa/hour/trades_adjusted")
}
setorderv(market_data, c('symbol', 'datetime'))
market_data[, returns := (close / shift(close)) - 1, by = .(symbol)]
spy <- market_data[symbol == "SPY", .(datetime, close, returns)]

# clean market data
keep_symbols <- market_data[, .N, by = symbol][N > min(windows), symbol]
market_data <- market_data[symbol %in% keep_symbols]
symbols <- unique(market_data$symbol)
close_data <- market_data[, .(symbol, datetime, close, returns)]
close_data <- na.omit(close_data)

# calculate main variablea
cols_pr <- paste0("pr_", windows)
close_data[, (cols_pr) := lapply(windows, function(w) roll_percent_rank(close, w)), by = "symbol"]
cols_sd <- paste0("sd_", windows)
close_data[, (cols_sd) := lapply(windows, function(w) QuantTools::roll_sd(returns, w)), by = "symbol"]
cols_max <- paste0("max_", windows)
close_data[, (cols_max) := lapply(windows, function(w) QuantTools::roll_max(close, w)), by = "symbol"]
cols_min <- paste0("min_", windows)
close_data[, (cols_min) := lapply(windows, function(w) QuantTools::roll_min(close, w)), by = "symbol"]
cols_q_lower <- paste0("q_lower_", windows)
close_data[, (cols_q_lower) := lapply(windows, function(w) QuantTools::roll_quantile(returns, w, p = 0.001)), by = "symbol"]
cols_q_upper <- paste0("q_upper_", windows)
close_data[, (cols_q_upper) := lapply(windows, function(w) QuantTools::roll_quantile(returns, w, p = 0.999)), by = "symbol"]

# calculate derivations of main variables
# pr
cols_above <- paste0("pr_above_dummy_", windows)
close_data[, (cols_above) := lapply(.SD, function(x) ifelse(x > 0.999, 1, 0)), .SDcols = cols_pr]
cols_below <- paste0("pr_below_dummy_", windows)
close_data[, (cols_below) := lapply(.SD, function(x) ifelse(x < 0.001, 1, 0)), .SDcols = cols_pr]
# range
cols_min_max_dev <- paste0("min_max_deviation_", windows)
close_data[, (cols_min_max_dev) := (close_data[, ..cols_max] - close_data[, ..cols_min])]
close_data[,  (cols_min_max_dev) := lapply(.SD, function(x) x / close), .SDcols = cols_min_max_dev]

# get risk measures
indicators_pr <- close_data[, lapply(.SD, sum, na.rm = TRUE), .SDcols = c(cols_above, cols_below), by = .(datetime)]
setorder(indicators_pr, "datetime")
indicators_sd <- close_data[, lapply(.SD, sum, na.rm = TRUE), .SDcols = c(cols_sd), by = .(datetime)]
setorder(indicators_sd, "datetime")
indicators_min_max_dev <- close_data[, lapply(.SD, sum, na.rm = TRUE), .SDcols = c(cols_min_max_dev), by = .(datetime)]
setorder(indicators_min_max_dev, "datetime")
# setnames(indicators_sum, colnmaes(indicators_sum)[3:ncol(indicators_sum)])
# indicators_mean <- close_data[, lapply(.SD, mean, na.rm = TRUE), .SDcols = c(cols_above, cols_below), by = .(datetime)]
# indicators_sd <- close_data[, lapply(.SD, sd, na.rm = TRUE), .SDcols = c(cols_above, cols_below), by = .(datetime)]
# indicators <- rbind(indicators_sum, indicators_mean, indicators_sd)

# merge indicators and spy
backtest_data <- spy[indicators_pr, on = 'datetime'][
  indicators_min_max_dev, on = 'datetime'][
    indicators_sd, on = 'datetime']
backtest_data <- na.omit(backtest_data)
setorder(backtest_data, "datetime")

# plots
# pr
g1 <- ggplot(backtest_data, aes(x = datetime)) +
  geom_line(aes(y = pr_below_dummy_1056)) +
  ggtitle("Below Dummy Sum")
g2 <- ggplot(backtest_data, aes(x = datetime, y = dummy_sum_net)) +
  geom_line() +
  ggtitle("Dummy Sum Net")
g3 <- ggplot(backtest_data, aes(x = datetime, y = pr_mean)) +
  geom_line() +
  ggtitle("PR Mean")
g4 <- ggplot(backtest_data, aes(x = datetime, y = pr_sd)) +
  geom_line() +
  ggtitle("PR sd")
( g1 | g2 ) / ( g3 | g4 )
# min max dev
g1 <- ggplot(backtest_data, aes(x = datetime)) +
  geom_line(aes(y = min_max_deviation_176)) +
  ggtitle("Below Dummy Sum")
g2 <- ggplot(backtest_data, aes(x = datetime)) +
  geom_line(aes(y = min_max_deviation_528)) +
  ggtitle("Below Dummy Sum")
g3 <- ggplot(backtest_data, aes(x = datetime)) +
  geom_line(aes(y = min_max_deviation_1056)) +
  ggtitle("Below Dummy Sum")
g4 <- ggplot(backtest_data, aes(x = datetime)) +
  geom_line(aes(y = min_max_deviation_2112)) +
  ggtitle("Below Dummy Sum")
( g1 | g2 ) / ( g3 | g4 )
ggplot(backtest_data[datetime %between% c("2020-01-01", "2021-01-01")], aes(x = datetime)) +
  geom_line(aes(y = min_max_deviation_176)) +
  ggtitle("Below Dummy Sum")
ggplot(backtest_data[datetime %between% c("2018-01-01", "2019-01-01")], aes(x = datetime)) +
  geom_line(aes(y = min_max_deviation_176)) +
  ggtitle("Below Dummy Sum")
# sd
g1 <- ggplot(backtest_data, aes(x = datetime)) +
  geom_line(aes(y = sd_528)) +
  ggtitle("Below Dummy Sum")
ggplot(backtest_data[datetime %between% c("2020-01-01", "2021-01-01")], aes(x = datetime)) +
  geom_line(aes(y = sd_528)) +
  ggtitle("Below Dummy Sum")
ggplot(backtest_data[datetime %between% c("2021-01-01", "2021-06-01")], aes(x = datetime)) +
  geom_line(aes(y = sd_528)) +
  ggtitle("Below Dummy Sum")



# backtst function
backtest <- function(returns, indicator, threshold, return_cumulative = TRUE) {
  sides <- vector("integer", length(indicator))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(indicator[i-1])) {
      sides[i] <- NA
    } else if (indicator[i-1] > threshold) {
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
  annRetx = Return.annualized(x, scale=252)
  sharpex = SharpeRatio.annualized(x, scale=252)
  winpctx = length(x[x > 0])/length(x[x != 0])
  annSDx = sd.annualized(x, scale=252)

  DDs <- findDrawdowns(x)
  maxDDx = min(DDs$return)
  maxLx = max(DDs$length)

  Perf = c(cumRetx, annRetx, sharpex, winpctx, annSDx, maxDDx, maxLx)
  names(Perf) = c("Cumulative Return", "Annual Return","Annualized Sharpe Ratio",
                  "Win %", "Annualized Volatility", "Maximum Drawdown", "Max Length Drawdown")
  return(Perf)
}


# LEVEL TRADING RULE ------------------------------------------------------

# PR

# optimizations loop
thresholds <- c(seq(0, 30, 1))
colnames(backtest_data)
variables <- colnames(indicators)[grep("_below_", colnames(indicators))]
params <- expand.grid(thresholds, variables, stringsAsFactors = FALSE)
returns_strategies <- list()
x <- vapply(1:nrow(params), function(i) backtest(backtest_data$returns,
                                                 backtest_data[, get(params[i, 2])],
                                                 # EMA(backtest_data[, get(params[i, 2])], 1),
                                                 params[i, 1]),
            numeric(1))
optim_results <- cbind(params, cum_return = x)

# inspect
optim_results[order(optim_results$cum_return), ]
ggplot(optim_results, aes(x = cum_return)) + geom_histogram() +
  geom_vline(xintercept = 4.1, color = "red") + facet_grid(. ~ Var2)
optim_results[optim_results$Var2 == "pr_below_dummy_176", ]
optim_results[optim_results$Var2 == "pr_below_dummy_1056", ]
optim_results[optim_results$Var2 == "pr_below_dummy_2112", ]

# backtest individual
strategy_returns <- backtest(backtest_data$returns, backtest_data$pr_below_dummy_176, 11, return_cumulative = FALSE)
charts.PerformanceSummary(xts(cbind(backtest_data$returns, strategy_returns), order.by = backtest_data$datetime))
Performance(xts(strategy_returns, order.by = backtest_data$datetime))
Performance(xts(backtest_data$returns, order.by = backtest_data$datetime))

# MIN MAX DEV
thresholds <- c(seq(100, 1000, 5))
colnames(backtest_data)
variables <- colnames(backtest_data)[grep("min_max", colnames(backtest_data))]
params <- expand.grid(thresholds, variables, stringsAsFactors = FALSE)
returns_strategies <- list()
x <- vapply(1:nrow(params), function(i) backtest(backtest_data$returns,
                                                 backtest_data[, get(params[i, 2])],
                                                 # EMA(backtest_data[, get(params[i, 2])], 1),
                                                 params[i, 1]),
            numeric(1))
optim_results_min_max <- cbind(params, cum_return = x)
optim_results_min_max[order(optim_results_min_max$cum_return, decreasing = TRUE), ]

# SD
thresholds <- c(seq(100, 1000, 5))
colnames(backtest_data)
variables <- colnames(backtest_data)[grep("min_max", colnames(backtest_data))]
params <- expand.grid(thresholds, variables, stringsAsFactors = FALSE)
returns_strategies <- list()
x <- vapply(1:nrow(params), function(i) backtest(backtest_data$returns,
                                                 backtest_data[, get(params[i, 2])],
                                                 # EMA(backtest_data[, get(params[i, 2])], 1),
                                                 params[i, 1]),
            numeric(1))
optim_results_min_max <- cbind(params, cum_return = x)
optim_results_min_max[order(optim_results_min_max$cum_return, decreasing = TRUE), ]
