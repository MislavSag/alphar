library(data.table)
library(highfrequency)
library(quantmod)
library(backCUSUM)
library(future.apply)
source('R/parallel_functions.R')
source('R/outliers.R')
source('R/import_data.R')



# IMPORT DATA -------------------------------------------------------------

# Import data
market_data <- import_mysql(
  contract = 'SPY5',
  save_path = 'D:/pobrisati',
  RMySQL::MySQL(),
  dbname = 'odvjet12_market_data_usa',
  username = 'odvjet12_mislav',
  password = 'Theanswer0207',
  host = '91.234.46.219'
)

# convert to xts
market_data <- xts::xts(market_data[, 2:(ncol(market_data)-1)], order.by = market_data$date)

# Remove outliers
market_data <- remove_outlier_median(quantmod::OHLC(market_data), median_scaler = 25)

# change frequency
# market_data <- xts::to.period(market_data, period = 'minutes', k = 5)

# time distribution of OHLCV
time_dist <- table(as.ITime(zoo::index(market_data)))

# Volatility
vol1 <- spotVol(Cl(market_data), method = 'detPer', on = 'minutes', k = 5, marketOpen = '15:30:00', marketClose = '21:00:00', tz = 'UTC')
plot(vol1)
head(vol1$spot)
head(vol1$daily)
market_data <- merge(market_data, vol1$spot)
market_data <- na.omit(market_data)
market_data$vol1.spot_lag <- data.table::shift(market_data$vol1.spot)
market_data <- na.omit(market_data)
head(market_data, 10)


# backcusum parameters
backcusum_rolling_window <- 100

# backcusum roll
bqp <- slider_parallel(
  .x = as.data.table(market_data),
  .f =   ~ {
    bq <- backCUSUM::BQ.test(.$vol1.spot ~ 1, alternative = "greater")
  },
  .before = backcusum_rolling_window - 1,
  .after = 0L,
  .complete = TRUE,
  n_cores = -1
)
detector <- lapply(bqp, purrr::pluck, 'detector')
detector <- lapply(detector, tail, 1)
detector <- unlist(detector)
plot(detector)

# results
market_data_sample <- market_data[(nrow(market_data)-length(detector)+1):nrow(market_data)]
results <- cbind.data.frame(
  market_data_sample,
  detector = detector)
mean(results$detector)
max(results$detector)

# alpha
critical_value = 0.25
for (i in seq_along(results$detector)) {
  if (i == 1) {
    sign <- NA
  } else if (results$detector[i-1] < critical_value) {
    sign <- c(sign, 1)
  } else {
    sign <- c(sign, 0)
  }
}
results$sign <- sign
table(sign)

# backtest
results$returns <- (results$close - shift(results$close)) / shift(results$close)
results$returns_strategy <- results$returns * results$sign
results_xts <- xts::xts(results[, c('returns', 'returns_strategy')], order.by = zoo::index(market_data_sample))
PerformanceAnalytics::charts.PerformanceSummary(results_xts, plot.engine = 'ggplot2')

# make function for plumber

