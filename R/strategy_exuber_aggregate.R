library(data.table)
library(ggplot2)
library(xts)
library(PerformanceAnalytics)
library(TTR)
library(tidyr)
library(leanr)


# import exuber data
exuber_path <- "D:/risks/radf_hour/1-200-10"
exuber_data <- lapply(list.files(exuber_path, full.names = TRUE), fread)
exuber_data <- rbindlist(exuber_data)
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
exuber_data[, negative_dummy := ifelse(bsadf > 1.25 & cum_returns < 0, 1, 0)]
exuber_data[, positive_dummy := ifelse(bsadf > 1.25 & cum_returns > 0, 1, 0)]
setorderv(exuber_data, c("symbol", "datetime"))

# define indicators based on exuber
radf_vars <- colnames(exuber_data)[3:ncol(exuber_data)]
indicators_median <- exuber_data[, lapply(.SD, median, na.rm = TRUE), by = 'datetime', .SDcols = radf_vars]
colnames(indicators_median)[2:ncol(indicators_median)] <- paste0("median_", colnames(indicators_median)[2:ncol(indicators_median)])
indicators_sd <- exuber_data[, lapply(.SD, sd, na.rm = TRUE), by = 'datetime', .SDcols = radf_vars]
colnames(indicators_sd)[2:ncol(indicators_sd)] <- paste0("sd_", colnames(indicators_sd)[2:ncol(indicators_sd)])
indicators_mean <- exuber_data[, lapply(.SD, mean, na.rm = TRUE), by = 'datetime', .SDcols = radf_vars]
colnames(indicators_mean)[2:ncol(indicators_mean)] <- paste0("mean_", colnames(indicators_mean)[2:ncol(indicators_mean)])
indicators_sum <- exuber_data[, lapply(.SD, sum, na.rm = TRUE), by = 'datetime', .SDcols = radf_vars]
colnames(indicators_sum)[2:ncol(indicators_sum)] <- paste0("sum_", colnames(indicators_sum)[2:ncol(indicators_sum)])

# merge indicators
indicators <- indicators_median[indicators_sd, on = "datetime"]
indicators <- indicators_mean[indicators, on = "datetime"]
indicators <- indicators_sum[indicators, on = "datetime"]
indicators <- indicators[order(datetime)]
indicators <- indicators[order(datetime)]
indicators <- na.omit(indicators)

# merge spy and indicators
spy <- spy[, .(datetime, close, returns)]
spy <- indicators[spy, on = "datetime"]
spy <- na.omit(spy)
spy <- spy[order(datetime)]

# plots radf_sum
g1 <- ggplot(spy, aes(x = datetime)) +
  geom_line(aes(y = sum_radf_sum)) +
  geom_line(aes(y = SMA(sum_radf_sum, 10), color = "red"))
g1
# ggplot(spy[datetime %between% c("2020-01-01", "2021-01-01")], aes(x = datetime)) +
#   geom_line(aes(y = sd_radf_sum)) +
#   geom_line(aes(y = SMA(sd_radf_sum, 10), color = "red"))
# ggplot(spy[datetime %between% c("2008-01-01", "2010-03-01")], aes(x = datetime)) +
#   geom_line(aes(y = sd_radf_sum)) +
#   geom_line(aes(y = SMA(sd_radf_sum, 10), color = "red"))
#
#
# ggplot(spy[datetime %between% c("2020-01-01", "2021-01-01")], aes(x = datetime)) +
#   geom_line(aes(y = radf_sum)) +
#   geom_line(aes(y = radf_ema), color = 'red') +
#   geom_line(aes(y = radf_sma), color = 'blue') +
#   geom_line(aes(y = radf_sma_long), color = 'brown') +
#   geom_line(aes(y = radf_ema_long), color = 'purple')
# ggplot(spy[datetime %between% c("2008-01-01", "2009-01-01")], aes(x = datetime)) +
#   geom_line(aes(y = radf_sum)) +
#   geom_line(aes(y = radf_ema), color = 'red') +
#   geom_line(aes(y = radf_sma), color = 'blue') +
#   geom_line(aes(y = radf_sma_long), color = 'brown') +
#   geom_line(aes(y = radf_ema_long), color = 'purple')

# plots sadf
# buy_dates <- factor(spy$median_bsadf > 0)
# ggplot(spy, aes(x = datetime, y = close, group = buy_dates, color = buy_dates)) + geom_line()
# buy_dates <- spy[datetime %between% c("2020-01-01", "2021-01-01")]
# buy_dates <- factor(buy_dates$median_bsadf > 0)
# ggplot(spy[datetime %between% c("2020-01-01", "2021-01-01")], aes(x = datetime, y = close, group = buy_dates, color = buy_dates)) + geom_line()
#
# ggplot(spy, aes(x = datetime)) +
#   geom_line(aes(y = sum_positive_dummy))
# ggplot(spy, aes(x = datetime)) +
#   geom_line(aes(y = mean_returns))
# ggplot(spy[datetime %between% c("2020-01-01", "2021-01-01")], aes(x = datetime)) +
#   geom_line(aes(y = mean_returns)) +
#   geom_line(aes(y = SMA(sum_bsadf, 20)), color = "red")
# ggplot(spy[datetime %between% c("2008-01-01", "2009-01-01")], aes(x = datetime)) +
#   geom_line(aes(y = mean_returns))

# optimization
thresholds <- c(-0.5, 0, 0.5, 1, 1.5, 2, 2.5, 3, 4, 5, 10, 50, 100, 200)
variables <- colnames(spy)[2:(ncol(spy) - 2)]
params <- expand.grid(thresholds, variables, stringsAsFactors = FALSE)
optimization_data <- na.omit(spy)
backtest <- function(returns, indicator, threshold) {
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
  PerformanceAnalytics::Return.cumulative(returns_strategy)

}
returns_strategies <- vapply(1:nrow(params), function(i) backtest(optimization_data$returns,
                                                                  SMA(optimization_data[, get(params[i, 2])], 8),
                                                                  params[i, 1]),
                             numeric(1))
returns_strategies <- cbind(params, returns_strategies)
head(returns_strategies[order(returns_strategies$returns_strategies, decreasing = TRUE), ], 20)


# individual backtests
backtest_individual <- function(returns, indicator, threshold) {
  sides <- vector("integer", length(indicator))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(indicator[i-1])) {
      sides[i] <- NA
    } else if (indicator[i-1] > threshold) { # | indicator[i-1] < -threshold) {
      sides[i] <- 0
    } else {
      sides[i] <- 1
    }
  }
  sides <- ifelse(is.na(sides), 1, sides)
  print(table(sides))
  returns_strategy <- returns * sides
  return(returns_strategy)
}
returns_strategy <- backtest(backtest_data$returns, SMA(backtest_data$sum_bsadf, 5), 8)
backtest_data[, returns_strategy := returns_strategy]
backest_xts <- xts(cbind(backtest_data$returns, returns_strategy), order.by = backtest_data$datetime)
charts.PerformanceSummary(backest_xts, plot.engine = "ggplot2")

# individual backtests
x <- backtest_data[, .(datetime, returns_strategy, SMA(sum_radf_sum, 2))]
x <- x[datetime %between% c("2008-01-01", "2010-01-01")]
x[101:200]


# save for Quantconnect
# save_data <- spy[, .(datetime, adf, sadf, gsadf, badf, bsadf, radf_sum)]
# save_data[, datetime := format(datetime, "%Y-%m-%d %H")]
fwrite(spy, "D:/risks/exuber_data.csv", col.names = FALSE)
colnames(spy)
