library(data.table)
library(ggplot2)
library(xts)
library(PerformanceAnalytics)
library(TTR)
library(tidyr)
library(leanr)
library(patchwork)

# (0.1 * (36140302 - 5634579)) / 60 / 60 / 24

# import exuber data
exuber_path <- "D:/risks/radf_hour"
exuber_paths <- list.files(exuber_path, full.names = TRUE)
exuber_paths <- exuber_paths[grep("-100-", exuber_paths)]
exuber_files <- lapply(exuber_paths, list.files, full.names = TRUE)
names(exuber_files) <- gsub(".*/", "", exuber_paths)
exuber_files <- exuber_files[lengths(exuber_files) >= 500]
exuber_dfs <- lapply(exuber_files, function(x) rbindlist(lapply(x, fread)))
exuber_data <- rbindlist(exuber_dfs, idcol = TRUE)
exuber_data[, radf_sum := adf + sadf + gsadf + badf + bsadf]

# price data
sp500_stocks <- import_lean("D:/market_data/equity/usa/hour/trades_adjusted")
sp500_stocks[, returns := (close / shift(close)) - 1, by = .(symbol)]
spy <- sp500_stocks[symbol == "SPY"]
sp500_stocks <- sp500_stocks[symbol %in% exuber_data$symbol]
sp500_stocks[, cum_returns := frollsum(returns, 10 * 8), by = .(symbol)]
sp500_stocks[, cum_returns := shift(cum_returns, type = "lag"), by = .(symbol)]
sp500_stocks <- na.omit(sp500_stocks)
sp500_stocks <- sp500_stocks[, .(symbol, datetime, returns, cum_returns)]

# merge exuber and stocks
exuber_data <- merge(exuber_data, sp500_stocks, by = c("symbol", "datetime"), all.x = TRUE, a.y = FALSE)
setorderv(exuber_data, c(".id", "symbol", "datetime"))

# define indicators based on exuber
radf_vars <- colnames(exuber_data)[4:ncol(exuber_data)]
indicators_median <- exuber_data[, lapply(.SD, median, na.rm = TRUE), by = c('.id', 'datetime'), .SDcols = radf_vars]
colnames(indicators_median)[3:ncol(indicators_median)] <- paste0("median_", colnames(indicators_median)[3:ncol(indicators_median)])
indicators_sd <- exuber_data[, lapply(.SD, sd, na.rm = TRUE), by = c('.id', 'datetime'), .SDcols = radf_vars]
colnames(indicators_sd)[3:ncol(indicators_sd)] <- paste0("sd_", colnames(indicators_sd)[3:ncol(indicators_sd)])
indicators_mean <- exuber_data[, lapply(.SD, mean, na.rm = TRUE), by = c('.id', 'datetime'), .SDcols = radf_vars]
colnames(indicators_mean)[3:ncol(indicators_mean)] <- paste0("mean_", colnames(indicators_mean)[3:ncol(indicators_mean)])
indicators_sum <- exuber_data[, lapply(.SD, sum, na.rm = TRUE), by = c('.id', 'datetime'), .SDcols = radf_vars]
colnames(indicators_sum)[3:ncol(indicators_sum)] <- paste0("sum_", colnames(indicators_sum)[3:ncol(indicators_sum)])

# merge indicators
indicators <- merge(indicators_sd, indicators_median, by = c(".id", "datetime"))
indicators <- merge(indicators, indicators_mean, by = c(".id", "datetime"))
indicators <- merge(indicators, indicators_sum, by = c(".id", "datetime"))
setorderv(exuber_data, c(".id", "datetime"))
indicators <- na.omit(indicators)

# merge spy and indicators
spy <- spy[, .(datetime, close, returns)]
spy <- indicators[spy, on = "datetime"]
spy <- na.omit(spy)
setorderv(spy, c(".id", "datetime"))

# plots
variable_name <- "sd_radf_sum"
variables_ <- c(".id", "datetime", variable_name)
data_plot <- melt(spy[, ..variables_], id.vars = c(".id", "datetime"), measure.vars = variable_name)
sma_width <- 16
g1 <- ggplot(data_plot, aes(x = datetime, y = value, color = .id)) +
  geom_line()
g2 <- ggplot(data_plot[datetime %between% c("2015-01-01", "2016-01-01")], aes(x = datetime, y = SMA(value, sma_width), color = .id)) +
  geom_line()
g3 <- ggplot(data_plot[datetime %between% c("2008-01-01", "2010-03-01")], aes(x = datetime, y = SMA(value, sma_width), color = .id)) +
  geom_line()
g4 <- ggplot(data_plot[datetime %between% c("2020-01-01", "2021-01-01")], aes(x = datetime, y = SMA(value, sma_width), color = .id)) +
  geom_line()
g5 <- ggplot(data_plot[datetime %between% c("2021-01-01", "2021-05-10")], aes(x = datetime, y = SMA(value, sma_width), color = .id)) +
  geom_line()
g2 / g3 / g4 / g5

# optimization
thresholds <- c(seq(0, 4, 0.1))
variables <- colnames(spy)[3:(ncol(spy))]
params <- expand.grid(thresholds, variables, stringsAsFactors = FALSE)
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

# optimizations loop
returns_strategies <- list()
unique_ids <- unique(spy$.id)
for (j in seq_along(unique_ids)) {
  print(unique_ids[j])
  sample_ <- spy[.id == unique_ids[j]]
  sample_ <- sample_[order(datetime)]
  sample_ <- unique(sample_)
  x <- vapply(1:nrow(params), function(i) backtest(sample_$returns,
                                                   SMA(sample_[, get(params[i, 2])], 2),
                                                   params[i, 1]),
              numeric(1))
  returns_strategies[[j]] <- cbind(radf_id = unique_ids[j], params, x)
}
optimization_results <- rbindlist(returns_strategies)
optimization_results_order <- optimization_results[order(x, decreasing = TRUE)]
head(optimization_results[order(optimization_results$x, decreasing = TRUE), ], 20)

# optimization summary
opt_summary <- optimization_results[, median(x), by = .(Var2)]
opt_summary[order(V1, decreasing = TRUE)]
ggplot(optimization_results, aes(Var1, Var2, fill= x)) +
  geom_tile()

# backtest individual
backtest_data <- spy[.id == "1-100-1"]
strategy_returns <- backtest(backtest_data$returns, backtest_data$sd_gsadf, 0.85, return_cumulative = FALSE)
charts.PerformanceSummary(xts(cbind(backtest_data$returns, strategy_returns), order.by = backtest_data$datetime))



# OPTIMIZATION SD ---------------------------------------------------------

# optimization params
# thresholds <- c(seq(0.7, 1.4, 0.01))
# variables <- colnames(spy)[3:7]
thresholds <- c(seq(3, 5, 0.05))
variables <- colnames(spy)[8]
sma_window <- c(1, 2, 4, 8, 16)
params <- expand.grid(thresholds, variables, sma_window, stringsAsFactors = FALSE)

# optimizations loop
returns_strategies <- list()
unique_ids <- unique(spy$.id)
for (j in seq_along(unique_ids)) {
  print(unique_ids[j])
  sample_ <- spy[.id == unique_ids[j]]
  sample_ <- sample_[order(datetime)]
  sample_ <- unique(sample_)
  x <- vapply(1:nrow(params), function(i) backtest(sample_$returns,
                                                   SMA(sample_[, get(params[i, 2])], params[i, 3]),
                                                   params[i, 1]),
              numeric(1))
  returns_strategies[[j]] <- cbind(radf_id = unique_ids[j], params, x)
}
optimization_results <- rbindlist(returns_strategies)
optimization_results_order <- optimization_results[order(x, decreasing = TRUE)]
head(optimization_results[order(optimization_results$x, decreasing = TRUE), ], 50)
nrow(optimization_results_order)

# optimization summary
opt_summary <- optimization_results[, median(x), by = .(Var2)]
opt_summary[order(V1, decreasing = TRUE)]
ggplot(optimization_results, aes(Var1, Var3, fill= x)) +
  geom_tile()
ggplot(optimization_results[radf_id == "1-100-1"], aes(Var1, Var3, fill= x)) +
  geom_tile()
ggplot(optimization_results[radf_id == "1-100-5"], aes(Var1, Var3, fill= x)) +
  geom_tile()
ggplot(optimization_results[radf_id == "1-100-1"], aes(Var1, Var2, fill= x)) +
  geom_tile()

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

# backtest individual
threshold <- 3.4
backtest_data <- spy[.id == "1-100-5"]
strategy_returns <- backtest(backtest_data$returns, backtest_data$sd_radf_sum, threshold, return_cumulative = FALSE)
charts.PerformanceSummary(xts(cbind(backtest_data$returns, strategy_returns), order.by = backtest_data$datetime))
Performance(xts(strategy_returns, order.by = backtest_data$datetime))

backtest_data <- spy[.id == "1-100-5" & datetime %between% c("2008-01-01", "2010-01-01")]
strategy_returns <- backtest(backtest_data$returns, backtest_data$sd_radf_sum, threshold, return_cumulative = FALSE)
charts.PerformanceSummary(xts(cbind(backtest_data$returns, strategy_returns), order.by = backtest_data$datetime))

backtest_data <- spy[.id == "1-100-5" & datetime %between% c("2020-01-01", "2021-01-01")]
strategy_returns <- backtest(backtest_data$returns, backtest_data$sd_radf_sum, threshold, return_cumulative = FALSE)
charts.PerformanceSummary(xts(cbind(backtest_data$returns, strategy_returns), order.by = backtest_data$datetime))

backtest_data <- spy[.id == "1-100-5" & datetime %between% c("2015-01-01", "2017-01-01")]
strategy_returns <- backtest(backtest_data$returns, backtest_data$sd_radf_sum, threshold, return_cumulative = FALSE)
charts.PerformanceSummary(xts(cbind(backtest_data$returns, strategy_returns), order.by = backtest_data$datetime))

backtest_data <- spy[.id == "1-100-5" & datetime %between% c("2016-01-01", "2017-01-01")]
strategy_returns <- backtest(backtest_data$returns, backtest_data$sd_radf_sum, threshold, return_cumulative = FALSE)
charts.PerformanceSummary(xts(cbind(backtest_data$returns, strategy_returns), order.by = backtest_data$datetime))

backtest_data <- spy[.id == "1-100-5" & datetime %between% c("2021-01-01", "2021-07-01")]
strategy_returns <- backtest(backtest_data$returns, backtest_data$sd_radf_sum, threshold, return_cumulative = FALSE)
charts.PerformanceSummary(xts(cbind(backtest_data$returns, strategy_returns), order.by = backtest_data$datetime))
