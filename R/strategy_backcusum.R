library(evir)
library(data.table)
library(xts)
library(quantmod)
library(parallel)         # for the GASS package
library(naniar)
library(ggplot2)
library(mlfinance)
library(highfrequency)
library(PerformanceAnalytics)
library(DBI)
library(RMySQL)
library(runner)
library(parallel)
source('C:/Users/Mislav/Documents/GitHub/alphar/R/parallel_functions.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/outliers.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/import_data.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/features.R')



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
  upsample = 10,
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



# (BACK)CUSUM ---------------------------------------------------------------

# parallel window backcusum estimation
x <- as.matrix(as.data.table(market_data[[1]])[, 'standardized_returns'])
window_lengths <- seq(50, 400, 50)
bcrolls <- list()
cl <- makeCluster(availableCores() - 16)
clusterExport(cl, c('x', 'window_length'))
for (i in seq_along(window_lengths)) {
  print(i)
  bcroll <- runner(
    x = x,
    f = function(y) {
      backCUSUM::SBQ.test(as.formula('y ~ 1'), alternative = 'greater')[['statistic']]
    },
    k = window_lengths[i],
    na_pad = TRUE,
    cl = cl
  )

  bcrolls[[i]] <- bcroll
}
stopCluster(cl)
results <- cbind(as.data.table(market_data[[1]]), do.call(cbind, lapply(bcrolls, as.data.table)))
results <- na.omit(results)
colnames(results)[(ncol(results) - length(window_lengths) + 1):ncol(results)] <- paste0('bcroll_', window_lengths)
head(results)

# trading rule
trade <- function(indicator, q_threshold) {
  side <- vector(mode = 'integer', length = length(indicator))
  for (i in seq_along(indicator)) {
    if (is.na(q_threshold[i]) || is.na(q_threshold[i-1])) {
      side[i] <- NA
    } else if (indicator[i - 1] > q_threshold[i - 1]) {
      side[i] <- 0L
    } else {
      side[i] <- 1L
    }
  }
  return(side)
}

# optimization
back_cusum_cols <- colnames(results)[grep('bcroll', colnames(results))]
sma_width <- seq(2, 45, 5)
p <- seq(0.9, 0.99, 0.01)
paramset <- expand.grid(back_cusum_cols, sma_width, p, stringsAsFactors = FALSE)
colnames(paramset) <- c('back_cusum_col', 'sma_width', 'threshold')

# optimize
optimize <- function(x, sma_width, p, returns) {
  indicator <- unlist(results[, ..x], use.names = FALSE)
  bcroll_sma <- SMA(indicator, sma_width)
  q_thres <- roll_quantile(indicator, width = 12*8*200, p = p, min_obs = 12*8*30)
  side <- trade(bcroll_sma, q_thres)
  returns_strategy <- side * returns
  cum_return <- Return.cumulative(na.omit(returns_strategy))
}
cum_returns <- future_vapply(1:nrow(paramset),
                             function(x) optimize(paramset[x, 1],
                                                  paramset[x, 2],
                                                  paramset[x, 3],
                                                  results$returns),
                             numeric(1))
benchmark_cum_returns <- Return.cumulative(na.omit(results$returns))

max(cum_returns)
paramset[which.max(cum_returns), ]

# plot opt results
ggplot(as.data.table(cum_returns), aes(cum_returns)) + geom_histogram() + geom_vline(xintercept = benchmark_cum_returns)


# backtest
indicator <- unlist(results[, bcroll_200], use.names = FALSE)
bcroll_sma <- SMA(indicator, 12)
q_thres <- roll_quantile(indicator, width = 12*8*200, p = 0.96, min_obs = 12*8*30)
side <- trade(bcroll_sma, q_thres)
returns_strategy <- side * results$returns
perf <- na.omit(cbind.data.frame(index = results$index, returns = results$returns, returns_strategy))
perf <- xts(perf[, 2:3], order.by = perf[, 1])
charts.PerformanceSummary(perf, e)
