library(evir)
library(data.table)
library(xts)
library(quantmod)
library(parallel)
library(naniar)
library(ggplot2)
library(mlfinance)
library(highfrequency)
library(PerformanceAnalytics)
library(DBI)
library(RMySQL)
library(runner)
source('C:/Users/Mislav/Documents/GitHub/alphar/R/parallel_functions.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/outliers.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/import_data.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/features.R')

# performance
plan(multiprocess(workers = availableCores() - 16))  # multiprocess(workers = availableCores() - 8)



# UTILS -------------------------------------------------------------------

# preprocessing function
preprocessing <- function(data, outliers = 'above_daily', add_features = TRUE) {

  # change colnames for spy
  col_change <- grepl('SPY_', colnames(data))
  colnames(data)[col_change] <- gsub('SPY_', '', colnames(data)[col_change])

  # remove outliers
  if (outliers == 'above_daily') {
    data <- remove_outlier_median(data, median_scaler = 25)
  }

  # add features
  if (isTRUE(add_features)) {
    data <- add_features(data)
  }

  data <- na.omit(data)

  return(data)
}

# connect to RMySQL
connect <- function() {
  DBI::dbConnect(
    RMySQL::MySQL(),
    dbname = 'odvjet12_equity_usa_minute_trades',
    username = 'odvjet12_mislav',
    password = 'Theanswer0207',
    host = '91.234.46.219'
  )
}


# IMPORT DATA -------------------------------------------------------------

# import intraday market data
market_data <- import_mysql(
  symbols = c('AAPL'),
  upsample = 60,
  trading_hours = TRUE,
  use_cache = FALSE,
  combine_data = FALSE,
  save_path = 'D:/market_data/usa/ohlcv',
  RMySQL::MySQL(),
  dbname = 'odvjet12_equity_usa_minute_trades',
  username = 'odvjet12_mislav',
  password = 'Theanswer0207',
  host = '91.234.46.219'
)

# clean data
market_data <- lapply(market_data, preprocessing, add_features = TRUE)



# DYNAMIC PROGRAMING SEGMENTS STRATEGY ------------------------------------

# params
window_length <- seq(100, 1000, 100)
p <- seq(0.05, 0.5, 0.05)
params <- expand.grid(window_length, p)

# dpseg roll
sample <- as.data.table(market_data[[1]])[, .(index, close)][, index := as.numeric(index)]
dpseg_roll <- function(window_size, p) {
  runner(
    x = sample,
    f = function(x) {
      library(dpseg)
      segs <- dpseg(x$index, x$close, jumps=FALSE, verb=FALSE, P = p)
      slope_last <- segs$segments$slope[length(segs$segments$slope)]
      r2 <- segs$segments$r2[length(segs$segments$r2)]
      cbind(slope_last = slope_last, r2 = r2)
    },
    k = window_size,
    na_pad = TRUE
  )
}
results_dpseg <- dpseg_roll(500, 0.3)
results_dpseg <- rbindlist(lapply(results_dpseg, as.data.table), fill = TRUE)
results_dpseg[, V1 := NULL]

results_dpseg <- lapply(1:nrow(params), function(x) {
  dpseg_roll(market_data[[1]], 'close', params[x, 1], params[x, 2])
})


# merge
results <- cbind(as.data.table(market_data[[1]]), results_dpseg)
results[, dpseg_sd := roll::roll_sd(results$slope_last, 10)]
results[, dpseg_sma := SMA(results$slope_last, 8*2)]
results <- na.omit(results)

# plots
# slope_last
ggplot(results, aes(x = index)) +
  geom_line(aes(y = slope_last))
ggplot(results[index %between% c('2020-01-01', '2020-06-01')], aes(x = index)) +
  geom_line(aes(y = slope_last), color = 'blue')
ggplot(results[index %between% c('2008-01-01', '2009-01-01')], aes(x = index)) +
  geom_line(aes(y = slope_last), color = 'blue')
# sd
ggplot(results, aes(x = index)) +
  geom_line(aes(y = dpseg_sd))
ggplot(results[index %between% c('2020-01-01', '2020-06-01')], aes(x = index)) +
  geom_line(aes(y = dpseg_sd), color = 'blue')
ggplot(results[index %between% c('2008-01-01', '2009-01-01')], aes(x = index)) +
  geom_line(aes(y = dpseg_sd), color = 'blue')
# sd
ggplot(results, aes(x = index)) +
  geom_line(aes(y = dpseg_sma))
ggplot(results[index %between% c('2020-01-01', '2020-06-01')], aes(x = index)) +
  geom_line(aes(y = dpseg_sma), color = 'blue')
ggplot(results[index %between% c('2008-01-01', '2009-01-01')], aes(x = index)) +
  geom_line(aes(y = dpseg_sma), color = 'blue')

# backtest
side <- vector(mode = 'integer', length = nrow(results))
slopes <- unlist(results$dpseg_sma, use.names = FALSE)
slopes_sd <- unlist(results$dpseg_sd, use.names = FALSE)
# qrolls <- as.vector(zoo::coredata(results$quantile_roll))
# r2 <- as.vector(zoo::coredata(results$r2))
for (i in 1:nrow(results)) {
  if (i == 1) {
    side[i] <- NA
  } else if (slopes[i-1] > 0) {
    side[i] <- 0
  } else {
    side[i] <- 1
  }
}
table(side)
results$side <- side
results$returns_strategy <- results$returns * results$side
charts.PerformanceSummary(results[, .(index, returns, returns_strategy)], plot.engine = 'ggplot2')
