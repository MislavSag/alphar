library(data.table)
library(purrr)
library(xts)
library(IBrokers)
library(quantmod)
library(exuber)
library(dpseg)
library(PerformanceAnalytics)
library(runner)
library(future.apply)
source('R/parallel_functions.R')
source('R/outliers.R')
plan(multiprocess)  # for multithreading




# PARAMETERS --------------------------------------------------------------


symbols <- c('SPY', 'AMZN', 'T')
freq <- c('1 day', '1 hour')  # 1 day is mandatory
cache_path <- 'D:/algo_trading_files/ib_cache_r'
dpseg_rolling <- c(600, 1000)


# IMPORT DATA -------------------------------------------------------------


# create tw connection and import data for every symbol
tws <- twsConnect(clientId = 2, host = '127.0.0.1', port = 7496)
contracts <- lapply(symbols, twsEquity, exch = 'SMART', primary = 'ISLAND')
market_data <- lapply(contracts, function(x) {
  lapply(freq, function(y) {
    ohlcv <- reqHistoricalData(tws, x, barSize = y, duration = '5 Y')
    })
  })
market_data <- purrr::flatten(market_data)  # unlist second level
names(market_data) <- lapply(symbols, function(x) paste(x, gsub(' ', '_', freq), sep = '_'))
twsDisconnect(tws)

# remove outliers from intraday market data
market_data <- lapply(market_data, function(x) {
  remove_outlier_median(quantmod::OHLC(x), median_scaler = 25)
  })



# EXUBER STRATEGY ---------------------------------------------------------

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




# DYNAMIC PROGRAMING SEGMENTS STRATEGY ------------------------------------


# Alpha function
dpseg_last <- function(data, p = NA) {
  if (is.na(p)) {
    p <- estimateP(x=data$time, y=data$price, plot=FALSE)
  }
  segs <- dpseg(data$time, data$price, jumps=FALSE, P=p, type='var', store.matrix=TRUE, verb=FALSE)
  slope_last <- segs$segments$slope[length(segs$segments$slope)]
  return(slope_last)
}

# Daily roll
dpseg_roll <- function(data, rolling_window = 600) {
  dpseg_data <- cbind.data.frame(time = zoo::index(data), price = Cl(data))
  colnames(dpseg_data) <- c('time', 'price')
  DT <- as.data.table(dpseg_data)
  DT[, time := as.numeric(time)]
  DT[,
     slope := runner(
       x = .SD,
       f = dpseg_last,
       k = rolling_window,
       na_pad = TRUE
     )]
  results <- xts::xts(DT[, c(2, 3)], order.by = dpseg_data$time)
  results <- na.omit(results)
  return(results)
}
results_dpseg <- lapply(market_data, function(x) {
  dpseg_roll(x, dpseg_rolling[1])
  })

# alpha execution
alpha_dpseg <- function(x) {
  x$returns <- (x$price - shift(x$price, 1L)) / shift(x$price, 1L)
  x$sign <- ifelse(x$slope < 0, 0L, NA)
  x$sign <- ifelse(x$slope > 0L & is.na(x$sign), 1L, x$sign)
  print('Hold distribution')
  print(table(x$sign))
  x$returns_strategy <- x$returns * x$sign
  x$sign <- shift(x$sign, 1L, type = 'lead')
  x <- na.omit(x)
  x
}
alpha_results <- lapply(results_dpseg, alpha_dpseg)
names(alpha_results) <- unlist(lapply(symbols, function(x) paste(x, gsub(' ', '_', freq), sep = '_')))



# CHOW TEST ---------------------------------------------------------------



# BACKTEST ----------------------------------------------------------------

charts.PerformanceSummary(alpha_results[[6]][, c('returns', 'returns_strategy')],
                          main = names(alpha_results)[6])
