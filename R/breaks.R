# fundamental
library(data.table)
library(purrr)
library(xts)
library(tsbox)
library(slider)
# data
library(IBrokers)
# alpha packages
library(exuber)
library(dpseg)
library(strucchange)
# backtest and finance
library(quantmod)
library(PerformanceAnalytics)
# other
library(runner)
library(future.apply)
source('R/parallel_functions.R')
source('R/outliers.R')
plan(multiprocess)  # for multithreading




# PARAMETERS --------------------------------------------------------------


# market data
symbols <- c('SPY', 'AMZN')
freq <- c('1 day', '1 hour')
data_length <- '5 Y'
cache_path <- 'D:/algo_trading_files/ib_cache_r'
# exuber
exuber_rolling_window <- c(300, 600)
adf_lags <- 1:2
minw <- exuber::psy_minw(exuber_rolling_window)
keep_last <- TRUE
# dpseg
dpseg_rolling <- c(600, 1000)
dpseg_type <- 'var'



# IMPORT DATA -------------------------------------------------------------


# create tw connection and import data for every symbol
tws <- twsConnect(clientId = 2, host = '127.0.0.1', port = 7496)
contracts <- lapply(symbols, twsEquity, exch = 'SMART', primary = 'ISLAND')
market_data <- lapply(contracts, function(x) {
  lapply(freq, function(y) {
    ohlcv <- reqHistoricalData(tws, x, barSize = y, duration = data_length)
    })
  })
market_data <- purrr::flatten(market_data)  # unlist second level
names(market_data) <- unlist(lapply(symbols, function(x) paste(x, gsub(' ', '_', freq), sep = '_')))
twsDisconnect(tws)

# remove outliers from intraday market data
market_data <- lapply(market_data, function(x) {
  remove_outlier_median(quantmod::OHLC(x), median_scaler = 25)
  })



# EXUBER STRATEGY ---------------------------------------------------------


# radf roll
# radf roll
radf_roll <- lapply(market_data[c(1, 3)], function(x) {
  lapply(exuber_rolling, function(y) {
    lapply(adf_lags, function(z) {
      x <- as.data.frame(as.data.table(Cl(x)))
      minw_ <- exuber::psy_minw(y)
      colnames(x) <- c('time', 'close')
      slider::slide(x, ~ {
        radf_ <- exuber::radf(.x$close, minw = minw_, lag = z)
        radf_ <- radf_$bsadf
        radf_ <- cbind.data.frame(.[(minw_ + z + 1):nrow(.), ], radf_)
        if (keep_last) {tail(radf_, 1)}
      },
      .before = y - 1,
      .complete = TRUE)
    })
  })
})
radf_roll <- purrr::flatten(purrr::flatten(radf_roll))
lapply(radf_est, function(x) na.omit(do.call(c, x)))



# radf roll
radf_roll <- lapply(market_data[c(1, 3)], function(x) {
  lapply(exuber_rolling, function(y) {
    lapply(adf_lags, function(z) {
      print(paste0('Window: ', y, ', lag: ', z))
      radf_series <- frollapply_parallel(
        y = Cl(x),
        n_cores = 8,
        roll_window = y,
        FUN = function(y_subsample) {
          radf_ <- exuber::radf(data = y_subsample,
                       minw = exuber::psy_minw(y),
                       lag = z)
          radf_ <- xts::xts(radf_, order.by = zoo::index(y_subsample))
          merge(y_subsample, radf_)
        })
      radf_to_xts(Cl(x), radf_series)
    })
  })
})
radf_est <- purrr::flatten(purrr::flatten(radf_roll))
radf_est <- lapply(radf_est, function(x) na.omit(do.call(c, x)))







df <- as.data.frame(as.data.table(Cl(data)))
colnames(df) <- c('time', 'close')
test <- slider::slide(df,
                      ~ {
                        radf_ <- exuber::radf(.x$close, minw = minw, lag = adf_lag)
                        radf_ <- radf_$bsadf
                        radf_ <- cbind.data.frame(.[(minw + adf_lag + 1):nrow(.), ], radf_)
                        if (keep_last) {tail(radf_, 1)}
                      },
                      .before = roll_window - 1,
                      .complete = TRUE)
length(test)
length(test[[601]])
test <- slider::slide(df, ~ mean(.x$close), .before = 599, complete = TRUE)

df <- as.data.frame(as.data.table(Cl(data)))
colnames(df) <- c('time', 'close')
x <- exuber::radf(df$close, 50, lag = 1)$bsadf
cbind.data.frame(df[(50+1+1):nrow(df), ], x)
length(x)
length(df$close)

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
dpseg_last <- function(data, ...) {
  if (is.na(p)) {
    p <- estimateP(x=data$time, y=data$price, plot=FALSE)
  }
  segs <- dpseg(data$time, data$price, jumps=FALSE, store.matrix=TRUE, verb=FALSE, ...)
  slope_last <- segs$segments$slope[length(segs$segments$slope)]
  return(slope_last)
}

# Daily roll
dpseg_roll <- function(data, rolling_window = 600, ...) {
  dpseg_data <- cbind.data.frame(time = zoo::index(data), price = Cl(data))
  colnames(dpseg_data) <- c('time', 'price')
  DT <- as.data.table(dpseg_data)
  DT[, time := as.numeric(time)]
  DT[,
     slope := runner(
       x = .SD,
       f = function(x) {
         dpseg_last(x, ...)
         },
       k = rolling_window,
       na_pad = TRUE
     )]
  results <- xts::xts(DT[, c(2, 3)], order.by = dpseg_data$time)
  results <- na.omit(results)
  return(results)
}
results_dpseg <- lapply(market_data, function(x) {
  dpseg_roll(x, dpseg_rolling[1], type = 'var', P = 0.5)
  })


# alpha execution
alpha_dpseg <- function(x) {
  x$returns <- (x$price - shift(x$price, 1L)) / shift(x$price, 1L)
  x$sign <- ifelse(x$slope < 0, 0L, NA)
  x$sign <- ifelse(x$slope >= 0 & is.na(x$sign), 1L, x$sign)
  # print('Hold distribution')
  # print(table(x$sign))
  x$sign <- shift(x$sign, 1L, type = 'lag')
  x$returns_strategy <- x$returns * x$sign
  x <- na.omit(x)
  x
}
alpha_results <- lapply(results_dpseg, alpha_dpseg)
names(alpha_results) <- unlist(lapply(symbols, function(x) paste(x, gsub(' ', '_', freq), sep = '_')))
head(alpha_results$SPY_1_hour$slope)
hist(alpha_results$SPY_1_hour$slope)
quantile(alpha_results$SPY_1_hour$slope)



# STRUCCHANGE -------------------------------------------------------------






# BACKTEST ----------------------------------------------------------------

charts.PerformanceSummary(alpha_results[[3]][, c('returns', 'returns_strategy')],
                          main = names(alpha_results)[3])

head(alpha_results[[6]])
tail(alpha_results[[4]])
