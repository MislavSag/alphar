library(xts)
library(IBrokers)
library(quantmod)
library(purrr)
library(dplyr)
library(data.table)
library(DBI)
library(RMySQL)
library(anytime)
library(exuber)
library(dpseg)
library(PerformanceAnalytics)
library(runner)
library(ggplot2)
library(tidyr)
library(AlpacaforR)
library(future.apply)
source('R/parallel_functions.R')
plan(multiprocess)



# IMPORT DATA -------------------------------------------------------------

tws <- twsConnect(clientId = 2, host = '127.0.0.1', port = 7496)
contract <- twsEquity('SPY','SMART','ISLAND')
market_day <- reqHistoricalData(tws, contract, barSize = '1 day', duration = '25 Y')
market_hour <- reqHistoricalData(tws, contract, barSize = '1 hour', duration = '25 Y')
market_minute <- reqHistoricalData(tws, contract, barSize = '1 minute', duration = '25 Y')
plot(market_hour$SPY.Close)
twsDisconnect(tws)

# remove outliers
dim(market_hour)
market_hour <- remove_outlier_median(quantmod::OHLC(market_hour), quantmod::OHLC(market_day), median_scaler = 25)
dim(market_hour)



# EXUBER ------------------------------------------------------------------

# parameters
# adf_lag <- 1:5
rolling_window <- 600
exuber_minw <- as.integer((0.01 + (1.8/sqrt(rolling_window))) * rolling_window)

# alpha
radf_buble_last <- function(...) {
  rsim_data <- exuber::radf(...)
  last_bsadf <- tail(rsim_data$bsadf, 1)
  return(last_bsadf)
}

# daily roll
radf_daily <- lapply(1:5, function(adf_lag) {
  frollapply_parallel(
    y = zoo::coredata(close_daily),
    n_cores = 7,
    roll_window = rolling_window,
    FUN = function(y_subsample) {
      radf_buble_last(data = y_subsample,
                      minw = exuber_minw,
                      lag = adf_lag)
    }
  )})
radf_d <- lapply(radf_daily, function(x) do.call(c, x))

# hourly roll
radf_hourly <- lapply(1:2, function(adf_lag) {
  frollapply_parallel(
    y = zoo::coredata(close_hourly),
    n_cores = 20,
    roll_window = rolling_window,
    FUN = function(y_subsample) {
      radf_buble_last(data = y_subsample,
                      minw = exuber_minw,
                      lag = adf_lag)
    }
  )})
radf_h <- lapply(radf_hourly, function(x) do.call(c, x))

# minute roll
# radf_minute <- lapply(1:5, function(adf_lag) {
#   frollapply_parallel(
#     y = zoo::coredata(contract_ohlcv$close),
#     n_cores = 25,
#     roll_window = rolling_window,
#     FUN = function(y_subsample) {
#       radf_buble_last(data = y_subsample,
#                       minw = exuber_minw,
#                       lag = adf_lag)
#     }
#   )})
# radf_m <- lapply(radf_minute, function(x) do.call(c, x))

# radf to xts with close price
radf_to_xts <- function(price, radf_series) {
  close_sample <- as.vector(zoo::coredata(price))
  close_sample <- close_sample[(rolling_window):length(close_sample)]
  time_index <- zoo::index(price)[(rolling_window):length(zoo::index(price))]
  results <- cbind.data.frame(close = close_sample, radf = na.omit(radf_series))
  results <- xts::xts(results, order.by = time_index)
  results
}

# save radf values
for (i in seq_along(radf_d)) {
  x <- radf_to_xts(close_daily, radf_d[[i]])
  write.zoo(x, file=paste0("D:/algo_trading_files/exuber/radf_d_adf_", i, ".csv"), sep=";")
}
for (i in seq_along(radf_h)) {
  x <- radf_to_xts(close_hourly, radf_h[[i]])
  write.zoo(x, file=paste0("D:/algo_trading_files/exuber/radf_h_adf_", i, ".csv"), sep=";")
}

# backtest
exuber_backtest <- function(price, radf_series, cv_conf = '90%', exuber_minw, use_short = FALSE) {
  critical_value <- radf_crit[[exuber_minw]]$gsadf_cv[cv_conf]
  close_sample <- as.vector(zoo::coredata(price))
  close_sample <- close_sample[(rolling_window):length(close_sample)]
  time_index <- zoo::index(price)[(rolling_window):length(zoo::index(price))]
  results <- cbind.data.frame(close = close_sample, radf = na.omit(radf_series))
  results$returns <- (results$close - shift(results$close, 1L)) / shift(results$close, 1L)
  if (use_short) {
    results$sign <- ifelse(results$radf > critical_value & results$returns < 0, -1, 1)
  } else {
    results$sign <- ifelse(results$radf > critical_value & results$returns < 0, 0, 1)
  }
  results$returns_strategy <- results$returns * results$sign
  results$sign <- shift(results$sign, 1L, type = 'lead')
  results <- na.omit(results)
  perf <- as.xts(cbind.data.frame(benchmark = results$returns, strategy = results$returns_strategy),
                 order.by = time_index[((length(time_index) + 1) - length(results$returns)):length(time_index)])
  return(perf)
}
backtest_adfseq <- lapply(radf_h, function(x) {
  exuber_backtest(close_hourly, x, cv_conf = '90%', exuber_minw = exuber_minw, use_short = TRUE)
})
strategies <- do.call(cbind, backtest_adfseq)
colnames(strategies)[grep('strate', colnames(strategies))] <- paste0('strategy_adf_', 1:5)
strategies <- strategies[, !(grepl('benchmark.', colnames(strategies)))]

# Performance analytics statistics
charts.PerformanceSummary(strategies, plot.engine = 'ggplot2')
table.Stats(strategies)
table.AnnualizedReturns(strategies)
table.Arbitrary(strategies)
table.CalendarReturns(strategies)


# MOVE THESE TO TESTS
# result_parallel <- na.omit(do.call(c, roll_par))
# results <- na.omit(rolling_radf)
# length(results)
# length(result_parallel)
# length(results) - length(result_parallel)
#
# all(head(result_parallel, 50) == head(results, 50))
# all(result_parallel == results)
# head(result_parallel, 50)
# head(results, 50)
# tail(result_parallel[510:515])
# tail(results[510:515])



# DPSEG --------------------------------------------------------------------

# alpha
dpseg_roll <- function(data) {
  p <- estimateP(x=data$time, y=data$price, plot=FALSE)
  # segs <- dpseg(data$time, data$price, jumps=jumps, P=p, type=type, store.matrix=TRUE, verb=FALSE)
  # slope_last <- segs$segments$slope[length(segs$segments$slope)]
  #return(slope_last)
}

# daily roll
dpseg_data <- cbind.data.frame(time = zoo::index(close_daily), price = coredata(close_daily))
colnames(dpseg_data) <- c('time', 'price')
dpseg_data$time <- as.numeric(dpseg_data$time)
DT <- as.data.table(dpseg_data)
roll_par <- data.table::frollapply(
  x = dpseg_data,
  n = 600,
  FUN = function(x) {dpseg_roll(x)},
  fill = NA,
  align = 'right'
)


DT[,
   slope := runner(
     x = .SD,
     f = dpseg_roll,
     k = 600,
     na_pad = TRUE
   )]
DT <- DT[!is.na(slope)]


radf_daily <- lapply(1:5, function(adf_lag) {
  frollapply_parallel(
    y = zoo::coredata(close_daily),
    n_cores = 7,
    roll_window = rolling_window,
    FUN = function(y_subsample) {
      radf_buble_last(data = y_subsample,
                      minw = exuber_minw,
                      lag = adf_lag)
    }
  )})
radf_d <- lapply(radf_daily, function(x) do.call(c, x))
