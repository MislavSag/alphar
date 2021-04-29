# fundamental
library(data.table)
library(purrr)
# data
library(fmpcloudr)
# wrangling help packages
library(runner)
library(matrixStats)
library(roll)
library(ggplot2)
library(tidyr)
library(PerformanceAnalytics)
library(TTR)
library(mlr3verse)
library(fst)
library(TSrepr)
# backtest
library(esback)
# performance
library(parallel)
library(foreach)
library(future.apply)
# vilatility modeling
library(rugarch)
source('C:/Users/Mislav/Documents/GitHub/alphar/R/import_data.R')




# IMPORT DATA -------------------------------------------------------------

# import data
sp500_symbols <- import_sp500()
sp500_stocks <- import_locally("D:/market_data/equity/usa/hour/trades_adjusted", "csv")
spy <- import_blob(
  symbols = 'SPY',
  upsample = 1,
  trading_hours = TRUE,
  use_cache = TRUE,
  combine_data = FALSE,
  save_path = 'D:/market_data/equity/usa/hour/trades',
  container = "equity-usa-hour-trades"
)[[1]]

# calculate returns
spy <- na.omit(as.data.table(spy)[, returns := (close / shift(close)) - 1][])
sp500_stocks[, returns := (close / shift(close)) - 1, by = .(symbol)]



# OLD APPROACH ------------------------------------------------------------

# calculate sum of extra negative and positive returns
spy <- na.omit(as.data.table(spy)[, returns := (close / shift(close)) - 1][])
sp500_stocks[, returns := (close / shift(close)) - 1, by = .(symbol)]
sp500_stocks[, p_999 := roll_quantile(returns, 255*8*2, p = 0.999), by = .(symbol)]
sp500_stocks[, p_001 := roll_quantile(returns, 255*8*2, p = 0.001), by = .(symbol)]
sp500_stocks[, above := ifelse(returns > p_999, returns - p_999, 0), by = .(symbol)]
sp500_stocks[, below := ifelse(returns < p_001, abs(returns - p_001), 0), by = .(symbol)]
sp500_stocks[, above_dummy := ifelse(returns > p_999, 1, 0), by = .(symbol)]
sp500_stocks[, below_dummy := ifelse(returns < p_001, 1, 0), by = .(symbol)]

# get tail risk mesures
sp500_indicators <- sp500_stocks[, .(below_sum = sum(below, na.rm = TRUE),
                                     above_sum = sum(above, na.rm = TRUE)
                                     ), by = .(datetime)]
sp500_indicators <- sp500_indicators[order(datetime)]
sp500_indicators[, `:=`(excess = above_sum - below_sum)]
sp500_indicators[, excess_sma := SMA(excess, 8 * 9)]
sp500_indicators[, excess_sma_short := SMA(excess, 8 * 1)]
sp500_indicators <- as.data.table(spy)[, .(index, close, returns, volume)][sp500_indicators, on = c(index = 'datetime')]
sp500_indicators <- na.omit(sp500_indicators)

# analyse results
vetical_lines_sell <- sp500_indicators[below_sum > 0.7, index]
vetical_lines_buy <- sp500_indicators[above_sum > 0.7, index]
ggplot(sp500_indicators, aes(x = index, y = close)) +
  geom_line() +
  geom_vline(xintercept = vetical_lines_sell, color = 'red') +
  geom_vline(xintercept = vetical_lines_buy, color = 'green')
ggplot(sp500_indicators[index %between% c('2020-01-01', '2020-06-01')], aes(x = index, y = close)) +
  geom_line() +
  geom_vline(xintercept = vetical_lines_sell, color = 'red') +
  geom_vline(xintercept = vetical_lines_buy, color = 'green')
ggplot(sp500_indicators, aes(x = index, y = excess_sma)) +
  geom_line()
ggplot(sp500_indicators[index %between% c('2020-01-01', '2020-06-01')], aes(x = index, y = excess_sma)) +
  geom_line()
# plot excess sma tail returns
ggplot(sp500_indicators, aes(x = index)) +
  geom_line(aes(y = excess_sma), color = 'blue') +
  geom_line(aes(y = excess_sma_long), color = 'red') +
  geom_line(aes(y = excess_sma), color = 'green')
ggplot(sp500_indicators[index %between% c('2020-01-01', '2020-06-01')], aes(x = index)) +
  geom_line(aes(y = excess_sma), color = 'blue') +
  geom_line(aes(y = excess_sma_long), color = 'red') +
  geom_line(aes(y = excess_sma_short), color = 'green')
ggplot(sp500_indicators[index %between% c('2015-08-20', '2015-09-15')], aes(x = index)) +
  geom_line(aes(y = excess_sma), color = 'blue') +
  geom_line(aes(y = excess_sma_long), color = 'red') +
  geom_line(aes(y = excess_sma), color = 'green')
ggplot(sp500_indicators[index %between% c('2007-01-01', '2009-01-01')], aes(x = index)) +
  geom_line(aes(y = excess_sma), color = 'blue') +
  geom_line(aes(y = excess_sma_long), color = 'red') +
  geom_line(aes(y = excess_sma_short), color = 'green')


# trading rule
# side <- rep(NA, nrow(sp500_indicators))
# side[which(sp500_indicators$below > 0.6) + 1] <- 0
# side[which(sp500_indicators$above > 0.5) + 1] <- 1
# side[1] <- 1
# side <- na.locf(side, na.rm = FALSE)
# side <- ifelse(shift(sp500_indicators$excess_sma) < -0.01, 0, side)
# zero_to_one <- frollsum(side, 8*30)
# side[which(zero_to_one == 0)] <- 1
# short
# side <- ifelse(side == 0, -1, side)
# simple trading rule: when excess dif lower than threshold, sell
side <- ifelse(shift(sp500_indicators$excess_sma_short) < -0.005, 0, 1)
# exces diff cross moving average
# side <- ifelse(shift(sp500_indicators$excess_sma_short) < -0.015 &
#                  shift(sp500_indicators$excess_sma_short) < shift(sp500_indicators$excess_sma_long),
#                0, 1)

# backtest
returns_strategy <- side * sp500_indicators$returns
beckteset_data <- cbind(sp500_indicators[, .(index, returns)], returns_strategy)
beckteset_data <- na.omit(beckteset_data)
mlr3measures::acc(as.factor(ifelse(beckteset_data$returns[shift(sp500_indicators$excess_sma) < -0.001] > 0, 1, 0))[-1],
                  as.factor(ifelse(beckteset_data$returns_strategy[shift(sp500_indicators$excess_sma) < -0.001] > 0, 1, 0))[-1])
charts.PerformanceSummary(beckteset_data)



# OPTIMIZE STRATEGY -------------------------------------------------------

# params
sma_width <- 1:60
threshold <- seq(-0.00001, -0.01, by = -0.00001)
paramset <- expand.grid(sma_width, threshold)
colnames(paramset) <- c('sma_width', 'threshold')

# backtset
optimize_strategy <- function(sma_width, threshold) {
  excess_sma <- SMA(sp500_indicators$excess, sma_width)
  side <- ifelse(shift(excess_sma) < threshold, 0, 1)
  returns_strategy <- side * sp500_indicators$returns
  beckteset_data <- cbind(sp500_indicators[, .(returns)], returns_strategy)
  beckteset_data <- na.omit(beckteset_data)
  cum_return <- PerformanceAnalytics::Return.cumulative(beckteset_data)[, 2]
}
cum_returns <- future_vapply(1:nrow(paramset),
                             function(x) optimize_strategy(paramset[x, 1], paramset[x, 2]), numeric(1))
results <- as.data.table(cbind(paramset, cum_returns))

# n best
head(results[order(cum_returns, decreasing = TRUE), ], 50)
tail(results[order(cum_returns, decreasing = TRUE), ], 50)

# plot
ggplot(results, aes(x = sma_width, y = threshold, fill = cum_returns)) +
  geom_tile()
ggplot(results[sma_width %in% 10:25 & threshold %between% c(-0.001, -0.00001)],
       aes(x = sma_width, y = threshold, fill = cum_returns)) +
  geom_tile()
ggplot(results[sma_width %in% 1:10 & threshold %between% c(-0.01, -0.0001)],
       aes(x = sma_width, y = threshold, fill = cum_returns)) +
  geom_tile()
ggplot(results, aes(cum_returns)) +
  geom_histogram()

# try one backtest
sp500_indicators[, excess_sma := SMA(excess, 35)]
# sp500_indicators[, excess_sma_long := SMA(excess, 8 * 10)]
# side <- ifelse(shift(sp500_indicators$excess_sma) < -0.007 | shift(sp500_indicators$excess_sma_long < -0.005), 0, 1)
side <- ifelse(shift(sp500_indicators$excess_sma < -0.0002), 0, 1)
# side <- ifelse(shift(sp500_indicators$excess_sma < -0.007), 0, 1)
returns_strategy <- side * sp500_indicators$returns
beckteset_data <- cbind(sp500_indicators[, .(index, returns)], returns_strategy)
beckteset_data <- na.omit(beckteset_data)
charts.PerformanceSummary(beckteset_data, plot.engine = 'ggplot2')



# OPTIMIZE STRATEGY WITH SHORT AND LONG -----------------------------------

# params
threshold_short <- seq(-0.001, -0.01, by = -0.001)
threshold_long <- seq(-0.001, -0.01, by = -0.001)
paramset <- expand.grid(threshold_short, threshold_long)
colnames(paramset) <- c('threshold_short', 'threshold_long')

# backtset
optimize_strategy <- function(threshold_short, threshold_long) {
  side <- ifelse(shift(sp500_indicators$excess_sma_short) < threshold_short |
                   shift(sp500_indicators$excess_sma < threshold_long), 0, 1)
  returns_strategy <- side * sp500_indicators$returns
  beckteset_data <- cbind(sp500_indicators[, .(returns)], returns_strategy)
  beckteset_data <- na.omit(beckteset_data)
  cum_return <- PerformanceAnalytics::Return.cumulative(beckteset_data)[, 2]
}
plan(multiprocess(workers = availableCores() - 16))
cum_returns <- future_vapply(1:nrow(paramset), function(x) optimize_strategy(paramset[x, 1], paramset[x, 2]), numeric(1))
results <- as.data.table(cbind(paramset, cum_returns))

# n best
head(results[order(cum_returns, decreasing = TRUE), ], 50)

# plot
ggplot(results, aes(x = threshold_short, y = threshold_long, fill = cum_returns)) +
  geom_tile()
ggplot(results[sma_width %in% 1:10 & threshold %between% c(-0.02, -0.0001)],
       aes(x = sma_width, y = threshold, fill = cum_returns)) +
  geom_tile()



# PREDICT EXCESS ----------------------------------------------------------

library(forecast)

# train test split
sample <- unlist(sp500_indicators[((255*8*2)+481):nrow(sp500_indicators), 'excess_sma'], use.names = FALSE)
split_ratio <- 0.6
train <- sample[1:floor(length(sample) * split_ratio)]
test <- sample[(floor(length(sample) * split_ratio)+1):length(sample)]

# Multi-step forecasts with re-estimation
h <- 5 # forecasting horizont
n <- length(test) - h + 1
fit <- auto.arima(train)
order <- arimaorder(fit)
fcmat <- matrix(0, nrow=n, ncol=h)
for (i in 1:n) {
  x <- window(hsales, end=1989.99 + (i-1)/12)
  refit <- Arima(x, order=order[1:3], seasonal=order[4:6])

}

arima_forecasts <- runner(
  x = sample[1:1020],
  f = function(x) {
    fit <- auto.arima(x)
    forecast(fit, h = 5)$mean
  },
  k = 1000,
  na_pad = FALSE
)
arima_forecasts[1, ]

# compare forecasts and real



# CRYPTO ------------------------------------------------------------------

# load data
crypto <- as.data.table(read_fst('data/crypto.fst'))
setkey(crypto, crypto_id)
crypto_ids <- unique(crypto$crypto_id)
sample <- crypto[.(crypto_ids[2:200])]
sample[, date_time := as.POSIXct(date_time, "%Y-%m-%d %H:%M:%S")]

# calculate sum of extra negative and positive returns
bitcoin <- crypto[.(crypto_ids[1])]
bitcoin[, date_time := as.POSIXct(date_time, "%Y-%m-%d %H:%M:%S")]
bitcoin <- na.omit(bitcoin[, returns := (price / shift(price)) - 1][])
sample[, returns := (price / shift(price)) - 1, by = .(crypto_id)]
sample[, p_999 := roll_quantile(returns, 255*8*2, p = 0.999), by = .(crypto_id)]
sample[, p_001 := roll_quantile(returns, 255*8*2, p = 0.001), by = .(crypto_id)]
sample[, above := ifelse(returns > p_999, returns - p_999, 0), by = .(crypto_id)]
sample[, below := ifelse(returns < p_001, abs(returns - p_001), 0), by = .(crypto_id)]
sample[, above_dummy := ifelse(returns > p_999, 1, 0), by = .(crypto_id)]
sample[, below_dummy := ifelse(returns < p_001, 1, 0), by = .(crypto_id)]

# get tail risk mesures
indicators <- sample[, .(below = sum(below, na.rm = TRUE),
                         above = sum(above, na.rm = TRUE)), by = .(date_time)]
indicators[, excess := above - below]
indicators[, excess_sma := SMA(excess, 8 * 9)]
indicators <- bitcoin[, .(date_time, price, returns)][indicators, on = 'date_time']
indicators <- indicators[order(date_time)]
indicators <- na.omit(indicators)

# analyse results
vetical_lines_sell <- indicators[below > 0.4, date_time]
vetical_lines_buy <- indicators[above > 0.4, date_time]
ggplot(indicators, aes(x = date_time, y = price)) +
  geom_line() +
  geom_vline(xintercept = vetical_lines_sell, color = 'red') +
  geom_vline(xintercept = vetical_lines_buy, color = 'green')

# optimize params
sma_width <- 1:40
threshold <- seq(-0.001, -1, by = -0.01)
paramset <- expand.grid(sma_width, threshold)
colnames(paramset) <- c('sma_width', 'threshold')

# optimize
optimize_strategy <- function(sma_width, threshold) {
  excess_sma <- SMA(indicators$excess, 8 * sma_width)
  side <- ifelse(shift(excess_sma) < threshold, 0, 1)
  returns_strategy <- side * indicators$returns
  beckteset_data <- cbind(indicators[, .(returns)], returns_strategy)
  beckteset_data <- na.omit(beckteset_data)
  cum_return <- PerformanceAnalytics::Return.cumulative(beckteset_data)[, 2]
}
cum_returns <- future_vapply(1:nrow(paramset),
                             function(x) optimize_strategy(paramset[x, 1], paramset[x, 2]), numeric(1))
results <- as.data.table(cbind(paramset, cum_returns))

# n best
head(results[order(cum_returns, decreasing = TRUE), ], 50)
tail(results[order(cum_returns, decreasing = TRUE), ], 50)

# plot
ggplot(results, aes(x = sma_width, y = threshold, fill = cum_returns)) +
  geom_tile()
ggplot(results[sma_width %in% 1:10 & threshold %between% c(-0.02, -0.0001)],
       aes(x = sma_width, y = threshold, fill = cum_returns)) +
  geom_tile()
ggplot(results[sma_width %in% 1:10 & threshold %between% c(-0.01, -0.0001)],
       aes(x = sma_width, y = threshold, fill = cum_returns)) +
  geom_tile()
ggplot(results, aes(cum_returns)) +
  geom_histogram()

# try one backtest
indicators[, excess_sma_long := SMA(excess, 8 * 2)]
# side <- ifelse(shift(sp500_indicators$excess_sma) < -0.007 | shift(sp500_indicators$excess_sma_long < -0.005), 0, 1)
side <- ifelse(shift(indicators$excess_sma < -0.005), 0, 1)
table(side)
# side <- ifelse(shift(sp500_indicators$excess_sma < -0.007), 0, 1)
returns_strategy <- side * indicators$returns
beckteset_data <- cbind(indicators[, .(date_time, returns)], returns_strategy)
beckteset_data <- na.omit(beckteset_data)
charts.PerformanceSummary(beckteset_data)




# ES AND VAR ESTIMATION --------------------------------------------------------------

# import data and merge
gpd_risks_left_tail <- fread(file.path(risk_path, 'gpd_risks_left_tail.csv'), sep = ';')
gpd_risks_right_tail <- fread(file.path(risk_path, 'gpd_risks_right_tail.csv'))
cols <- colnames(gpd_risks_right_tail)[grep('q_|e_', colnames(gpd_risks_right_tail))]
gpd_risks_right_tail[, (cols) := lapply(.SD, as.numeric), .SDcols = cols]
gpd_risks_left_tail[, (cols) := lapply(.SD, as.numeric), .SDcols = cols]
colnames(gpd_risks_left_tail) <- paste0("left_", colnames(gpd_risks_left_tail))
colnames(gpd_risks_right_tail) <- paste0("right_", colnames(gpd_risks_right_tail))

# merge spy stocks and risk measures
sp500_stocks <- sp500_stocks[gpd_risks_left_tail[left_symbol != 'SPY'], on = c(.id = 'left_symbol', index = 'left_date')]
sp500_stocks <- sp500_stocks[gpd_risks_right_tail[right_symbol != 'SPY'], on = c(.id = 'right_symbol', index = 'right_date')]

# calculate indicators
left_q_cols <- colnames(sp500_stocks)[grep('left_q', colnames(sp500_stocks))]
right_q_cols <- colnames(sp500_stocks)[grep('right_q', colnames(sp500_stocks))]
excess_cols <- gsub("right", "excess", colnames(sp500_stocks)[grep('right_q', colnames(sp500_stocks))])
excess_cols_dummy <- paste0('dummy_', excess_cols)
sp500_stocks[, (excess_cols) := sp500_stocks[, ..left_q_cols] - sp500_stocks[, ..right_q_cols]]
sp500_stocks[, (excess_cols_dummy) := lapply(.SD, function(x) ifelse(x > 0, 1, 0)), .SDcols = excess_cols]
sp500_stocks[, paste0(excess_cols, '_median') := lapply(.SD, function(x) roll_mean(x, 300, min_obs = 100)),
             by = .(.id),
             .SDcols = excess_cols]

# alterative indicators
sample <- sp500_stocks[, .(.id, index)]
sample[, indicator := rowSums(sp500_stocks[, ..excess_cols_dummy], na.rm = TRUE)]
sample <- sample[order(indicator, decreasing = TRUE)]
sample <- sample[, date := lubridate::date(index)]
test <- sample[, sum(indicator, na.rm = TRUE), by = c('.id', 'date')]
test <- test[order(V1, decreasing = TRUE)]
test[1:99]

#
sample <- sp500_stocks[, ..excess_cols] - sp500_stocks[, paste0(excess_cols, '_median'), with = FALSE]
sample <- data.table(excess_mean = rowMeans(as.matrix(sample), na.rm = TRUE))
sample <- cbind(.id = sp500_stocks$.id, index = sp500_stocks$index, sample)
sample <- sample[order(excess_mean, decreasing = TRUE)]
sample <- sample[index %between% c('2015-01-01', '2020-12-01')]
sample <- na.omit(sample)
sample[, date := lubridate::date(index)]
# sample <- sample[excess_mean %between% c(2, 10)]
sample[(nrow(sample)-199):(nrow(sample) - 100)]

test <- sample[, median(excess_mean, na.rm = TRUE), by = c('.id', 'date')]
test <- test[order(V1, decreasing = TRUE)]
test <- test[V1 %between% c(-20, 20)]

x <- test[.id == 'KSS']
x <- x[order(date)]
plot(x$date, x$V1)

# strategy
symbol <- 'TSLA'
sample <- sp500_stocks[.id == symbol, .(index, close, returns, volume, right_q_9990_500_15, left_q_9990_500_15)]
sample[, premium := right_q_9990_500_15 - left_q_9990_500_15]
sample[, premium_median := roll_quantile(premium, 1000, p = c(0.90), min_obs = 100)]
sample <- na.omit(sample)
# ggplot(sample, aes(index)) + geom_line(aes(y = premium_median * 100)) +
#   geom_line(aes(y = premium * 100)) + geom_line(aes(y = close))

# backtest
sample[, side := ifelse(shift(premium) > shift(premium_median), 1, 0)]
sample[, returns_strategy := side * returns]
PerformanceAnalytics::charts.PerformanceSummary(sample[, .(index, returns, returns_strategy)], plot.engine = 'ggplot2')
charts.PerformanceSummary(sample[index %between% c('2017-01-01', '2018-01-01'), .(index, returns, returns_strategy)],
                          plot.engine = 'ggplot2')

# graph analzsis
alpha <- cbind(sample,
               es_left = gpd_risks_left_tail$e_9990_500_10,
               es_right = gpd_risks_right_tail$e_9990_500_10,
               q_left = gpd_risks_left_tail$q_9990_500_10,
               q_right = gpd_risks_right_tail$q_9990_500_10
               )
alpha <- alpha[order(date)]
alpha <- alpha[, .(es_right_mean = mean(es_right, na.rm = TRUE),
                   es_left_mean = mean(es_left, na.rm = TRUE),
                   es_left_median = median(es_left, na.rm = TRUE),
                   es_right_median = median(es_right, na.rm = TRUE),
                   es_right_sum = sum(es_right, na.rm = TRUE),
                   es_left_sum = sum(es_left, na.rm = TRUE),
                   q_right_mean = mean(q_right, na.rm = TRUE),
                   q_left_mean = mean(q_left, na.rm = TRUE),
                   q_left_median = median(q_left, na.rm = TRUE),
                   q_right_median = median(q_right, na.rm = TRUE),
                   q_right_sum = sum(q_right, na.rm = TRUE),
                   q_left_sum = sum(q_left, na.rm = TRUE)
                   ), by = .(date)]
alpha <- alpha[spy, on = c('date' = 'index')]
alpha[, colnames(alpha)[2:12] := lapply(.SD, na.locf, na.rm = FALSE), .SDcols = (colnames(alpha)[2:12])]
alpha[, `:=`(es_excess_mean = es_right_mean - es_left_mean,
             es_excess_median = es_right_median - es_left_median,
             es_excess_sum = es_right_sum - es_left_sum,
             q_excess_mean = q_right_mean - q_left_mean,
             q_excess_median = q_right_median - q_left_median,
             q_excess_sum = q_right_sum - q_left_sum
             )]
alpha <- alpha[es_excess_mean < 100 & es_excess_sum < 100 & es_excess_mean > -100 & es_excess_sum > -100]

# basic series
ggplot(alpha, aes(x = date)) +
  # geom_line(aes(y = es_excess_mean * 1000), color = 'blue') +
  # geom_line(aes(y = es_excess_median * 1000), color = 'red') +
  # geom_line(aes(y = es_excess_sum * 10), color = 'orange') +
  geom_line(aes(y = q_excess_mean * 10000), color = 'purple') +
  geom_line(aes(y = q_excess_median * 10000), color = 'brown') +
  # geom_line(aes(y = q_excess_sum * 10), color = 'green') +
  geom_line(aes(y = close))
ggplot(alpha[date %between% c('2007-01-01', '2010-01-01')], aes(x = date)) +
  geom_line(aes(y = es_excess_mean * 100), color = 'blue') +
  geom_line(aes(y = es_excess_median * 1000), color = 'red') +
  geom_line(aes(y = es_excess_sum * 10), color = 'orange') +
  geom_line(aes(y = q_excess_mean * 1000), color = 'purple') +
  geom_line(aes(y = q_excess_median * 1000), color = 'brown') +
  geom_line(aes(y = q_excess_sum * 100), color = 'green') +
  geom_line(aes(y = close))
ggplot(alpha[date %between% c('2020-01-01', '2021-06-01')], aes(x = date)) +
  geom_line(aes(y = es_excess_mean * 1000), color = 'blue') +
  geom_line(aes(y = es_excess_median * 1000), color = 'red') +
  geom_line(aes(y = es_excess_sum * 100), color = 'orange') +
  geom_line(aes(y = q_excess_mean * 10000), color = 'purple') +
  geom_line(aes(y = q_excess_median * 10000), color = 'brown') +
  geom_line(aes(y = q_excess_sum * 100), color = 'green') +
  geom_line(aes(y = close))

# diff
ggplot(alpha, aes(x = date)) +
  geom_line(aes(y = (es_excess) * 1000), color = 'blue') +
  geom_line(aes(y = close))
ggplot(alpha[date %between% c('2007-06-01', '2010-01-01')], aes(x = date)) +
  geom_line(aes(y = (es_right - es_left) * 1000), color = 'blue') +
  geom_line(aes(y = close))
ggplot(alpha[date %between% c('2020-01-01', '2021-01-01')], aes(x = date)) +
  geom_line(aes(y = (es_excess) * 1000), color = 'blue') +
  geom_line(aes(y = close))
ggplot(alpha[date %between% c('2015-01-01', '2016-01-01')], aes(x = date)) +
  geom_line(aes(y = (es_excess) * 1000), color = 'blue') +
  geom_line(aes(y = close))
# sma
ggplot(alpha, aes(x = date)) +
  geom_line(aes(y = SMA(es_left, 10) * 1000), color = 'blue') +
  geom_line(aes(y = SMA(es_left, 200) * 1000), color = 'red') +
  geom_line(aes(y = close))
ggplot(alpha[date %between% c('2020-01-01', '2021-01-01')], aes(x = date)) +
  geom_line(aes(y = SMA(es_left, 10) * 1000), color = 'blue') +
  geom_line(aes(y = SMA(es_left, 200) * 1000), color = 'red') +
  geom_line(aes(y = close))
ggplot(alpha[date %between% c('2007-01-01', '2010-01-01')], aes(x = date)) +
  geom_line(aes(y = SMA(es_left, 10) * 1000), color = 'blue') +
  geom_line(aes(y = SMA(es_left, 200) * 1000), color = 'red') +
  geom_line(aes(y = close))


# backtest
side <- ifelse(shift(alpha$es_excess * 1000) < -25, 0, 1)
table(side)
returns_strategy <- side * alpha$returns
beckteset_data <- cbind(alpha[, .(date, returns)], returns_strategy)
beckteset_data <- na.omit(beckteset_data)
charts.PerformanceSummary(beckteset_data)



# PERFORMANCE ANALYTICS ---------------------------------------------------

#
PerformanceAnalytics::CVaR()


# GARCH MODEL -------------------------------------------------------------

# generate forecasts using rugarch package
.all_ok <- function(fit, spec) {
  tryCatch({
    valid_messages <- c( # nlminb
      'X-convergence (3)',
      'relative convergence (4)',
      'both X-convergence and relative convergence (5)',
      'singular convergence (7)',
      'false convergence (8)'
    )
    all_ok <- all(is.null(fit) == FALSE, inherits(fit, 'try-error') == FALSE)
    if (all_ok) {
      message <- fit@fit$message
      if (is.null(message)) {
        all_ok <- TRUE
      } else {
        all_ok <- (message %in% valid_messages)
      }
    }
    if (all_ok) {
      spec_fix <- spec
      setfixed(spec_fix) <- as.list(fit@fit$solver$sol$par)
      filter <- ugarchfilter(spec=spec_fix, data=fit@model$modeldata$data)
      forc <- ugarchforecast(spec_fix, fit@model$modeldata$data, n.ahead=1)
      all_ok <- all(c(is.finite(c(fitted(forc), sigma(forc), quantile(forc, 0.01))), abs(quantile(forc, 0.01)) < 1))
    }
    if (all_ok) {
      z <- (filter@model$modeldata[[2]] - fitted(filter)) / sigma(filter)
      all_ok <- all(all(is.finite(z)), all(abs(z) < 1e6))
    }

    all_ok
  }, error=function(cond) {FALSE})
}

get_forcecasts <- function(r, spec, alpha, win, refit) {
  n <- nrow(r)
  end_values <- seq(win + refit, n, by = refit)
  nr_windows <- length(end_values)
  out.sample <- rep(refit, nr_windows)
  if (end_values[nr_windows] < n) {
    end_values <- c(end_values, n)
    nr_windows <- nr_windows + 1
    out.sample <- c(out.sample, end_values[nr_windows] - end_values[nr_windows - 1])
  }

  forc <- foreach(i = 1:nr_windows, .combine = 'rbind') %do% {
    # Subset data
    r_ <- r[(end_values[i]-win-refit+1):end_values[i], ]

    # Fit the model
    fit <- try(ugarchfit(spec=spec, data=r_, out.sample=out.sample[i], solver='nlminb'), silent=TRUE)
    if (!.all_ok(fit, spec)) {
      fit <- try(ugarchfit(spec=spec, data=df, out.sample=out.sample[i], solver='solnp'), silent=TRUE)
    }

    if (.all_ok(fit, spec)) {
      last_working_fit <- fit
    } else {
      print(paste0('restored model at window = ', i))
      fit <- last_working_fit
    }

    # Forecasts of mean and variance
    spec_fix <- spec
    setfixed(spec_fix) <- as.list(fit@fit$solver$sol$par)
    forc <- ugarchforecast(spec_fix, r_, n.ahead = 1, n.roll = out.sample[i] - 1, out.sample = out.sample[i])

    m <- fitted(forc)
    s <- sigma(forc)

    m <- xts(as.numeric(m), order.by = tail(index(r_), out.sample[i]))
    s <- xts(as.numeric(s), order.by = tail(index(r_), out.sample[i]))

    # Quantile function of the innovations
    qf <- function(x, spec) qdist(distribution=spec@model$modeldesc$distribution, p=x, mu=0, sigma=1,
                                  shape=spec@model$fixed.pars$shape, skew=spec@model$fixed.pars$skew,
                                  lambda=spec@model$fixed.pars$ghlambda)

    # VaR and ES of the innovations
    vq <- qf(alpha, spec_fix)
    ve <- integrate(qf, 0, alpha, spec = spec_fix)$value / alpha

    # VaR and ES of the returns
    q <- m + s * vq
    e <- m + s * ve

    df <- cbind(tail(r_, out.sample[i]), m, s, q, e)
    colnames(df) <- c('r', 'm', 's', 'q', 'e')
    df
  }

  forc
}



# RISK FACTORS ------------------------------------------------------------

# prepare data
spy <- na.omit(as.data.table(spy)[, returns := (close / shift(close)) - 1][])
sp500_stocks[, returns := (close / shift(close)) - 1, by = .(`.id`)]
returns <- na.omit(sp500_stocks[, .(.id, index, returns)])
setnames(returns, colnames(returns), c('symbol', 'date', 'return'))
returns <- returns[returns[, .N, by = .(symbol)][N > 3000], on = "symbol"] # stock have at least 1 year of data
sorted_tickers <- unique(sp500_stocks$.id)[1:2]

# garch ES and VaR estimates
setup <- expand.grid(
  variance.model     = c('gjrGARCH'), # c('gjrGARCH', 'csGARCH')
  distribution.model = c('std'), # c('std', 'ged', 'jsu')
  last_x             = 2500,
  win                = c(1000, 2000),
  refit              = c(5),
  alpha              = 0.025,
  symbol             = sorted_tickers,
  stringsAsFactors = FALSE
)

tmp <- foreach(row = seq_along(seq_len(nrow(setup))), .errorhandling = 'pass') %dopar% {
  st <- c(setup[row,])
  file <- paste0('backtests/results/', paste(st, collapse = '-'), '.rds')
  if (file.exists(file)) {next()} else {file.create(file)}
  print(paste0('Processing ', st$symbol, ', win = ', st$win, ', refit = ', st$refit))

  r <- returns %>%
    dplyr::filter(symbol == st$symbol) %>%
    tbl2xts::tbl_xts()
  # r <- as.xts.data.table(r)

  spec <- ugarchspec(
    mean.model = list(armaOrder = c(0, 0), include.mean = FALSE),
    variance.model = list(model = st$variance.model),
    distribution.model = st$distribution.model
  )

  forc <- get_forcecasts(r = r, spec = spec, alpha = st$alpha, win = st$win, refit = st$refit)

  saveRDS(list(forc = forc, settings = st), file)
  print(paste0('Save results to ', file))
}
stopCluster(cl)

plan(multiprocess(4))
tmp <- future_lapply(1:nrow(setup), function(x) {
  st <- c(setup[x,])
  r <- returns[symbol == st$symbol]
  setcolorder(r, c("date", "return"))
  r <- as.xts.data.table(r)
  spec <- ugarchspec(
    mean.model = list(armaOrder = c(0, 0), include.mean = FALSE),
    variance.model = list(model = st$variance.model),
    distribution.model = st$distribution.model
  )
  forc <- get_forcecasts(r = r, spec = spec, alpha = st$alpha, win = st$win, refit = st$refit)
  return(forc)
})

