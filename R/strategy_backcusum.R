library(data.table)
library(Rcpp)
library(purrr)
library(ggplot2)
library(highfrequency)
library(quantmod)
library(dpseg)
library(future.apply)
library(anytime)
library(PerformanceAnalytics)
library(roll)
library(TTR)
library(simfinapi)
library(parallel)  # for the GASS package
source('C:/Users/Mislav/Documents/GitHub/alphar/R/parallel_functions.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/outliers.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/import_data.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/execution.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/features.R')

# performance
plan(multiprocess(workers = availableCores() - 4))  # multiprocess(workers = availableCores() - 8)




# PARAMETERS --------------------------------------------------------------

# before backcusum function
contract = 'SPY'
freq = 5  # 1 is one minute, 5 is 5 minutes, 60 is hour, 480 is day
upsample = FALSE
add_fundamentals = FALSE
add_vix = FALSE
# model parameters
roll_window = 150
moving = 'none'  # none, sma, ema
moving_window = 20
var_threshold = -0.002
# file name prefix
now <- gsub(' |:', '-', as.character(Sys.time()))
file_name <- paste0('var-', contract, '_', now)



# IMPORT DATA -------------------------------------------------------------

if (freq < 60) {
  paste0(contract, freq)
  # HFD
  market_data <- import_mysql(
    contract = paste0(contract, freq),
    save_path = 'D:/market_data/usa/ohlcv',
    trading_days = TRUE,
    upsample = upsample,
    RMySQL::MySQL(),
    dbname = 'odvjet12_market_data_usa',
    username = 'odvjet12_mislav',
    password = 'Theanswer0207',
    host = '91.234.46.219'
  )
  if (add_vix) {
    vix <- import_mysql(
      contract = paste0(contract, freq),
      save_path = 'D:/market_data/usa/ohlcv',
      trading_days = TRUE,
      upsample = upsample,
      RMySQL::MySQL(),
      dbname = 'odvjet12_market_data_usa',
      username = 'odvjet12_mislav',
      password = 'Theanswer0207',
      host = '91.234.46.219'
    )
  }
} else {
  # LFD
  market_data <- import_ib(
    contract = contract,
    frequencies = ifelse(freq == 60, '1 hour', '1 day'),
    duration = '30 Y',
    type = 'equity',
    trading_days = FALSE
  )
  colnames(market_data) <- c('open', 'high', 'low', 'close', 'volume', 'WAP', 'hasGaps', 'count')
  if (add_vix) {
    vix <- import_ib(
      contract = 'VIX',
      frequencies = ifelse(freq == 60, '1 hour', '1 day'),
      duration = '30 Y',
      type = 'index',
      trading_days = FALSE
    )
    colnames(vix) <- c('open', 'high', 'low', 'close', 'volume', 'WAP', 'hasGaps', 'count')
  }
}

# FUNDAMENTALS
if (add_fundamentals) {
  fundamentals <- simfinapi::sfa_get_statement(Ticker = contract,
                                               statement = 'all',
                                               period = 'quarters',
                                               ttm = TRUE,
                                               shares = TRUE,
                                               api_key = '8qk9Xc9scFc0Rbpfrx6PLdaiomvi7Dxc')
  if (!is.null(fundamentals)) {
    fundamentals <- janitor::clean_names(fundamentals)
    fundamentals <- fundamentals[, .(publish_date, shares, sales_per_share)]
    fundamentals <- xts::xts(fundamentals[, 2:ncol(fundamentals)], order.by = as.POSIXct(fundamentals$publish_date))
    market_data <- merge(market_data, fundamentals, all = TRUE)
    market_data$sales_per_share <- zoo::na.locf(market_data$sales_per_share)
    market_data <- market_data[!is.na(market_data$close), ]
    market_data$ps <- market_data$close / market_data$sales_per_share
  }
}



# PREPROCESSING -----------------------------------------------------------

# Remove outliers
market_data <- remove_outlier_median(market_data, median_scaler = 25)

# merge market data and VIX
if (add_vix) {
  market_data <- merge(market_data, vix[, 'close'], join = 'left')
  colnames(market_data)[ncol(market_data)] <- 'vix'
  market_data <- na.omit(market_data)
}

# Add features
market_data <- add_features(market_data)

# Remove NA values
market_data <- na.omit(market_data)



# (BACK)CUSUM ---------------------------------------------------------------

# backcusum roll
back_cusum_test <- function(data, col_name, window_size = 100, side = c('greater', 'less')) {
  bc <- slider_parallel(
    .x = as.data.table(data),
    .f =   ~ {
      formula_ <- paste0('.$', col_name, ' ~ 1')
      backCUSUM::SBQ.test(stats::as.formula(formula_), alternative = side)[['statistic']]
    },
    .before = window_size - 1,
    .complete = TRUE,
    n_cores = -1
  )
  bc <- unlist(bc)
  bc <- c(rep(NA, window_size - 1), bc)
  return(bc)
}

# backcusum roll for chooosen variables
market_data$standardized_returns_greater <- back_cusum_test(market_data, 'standardized_returns', 200, side = 'greater')
market_data$standardized_returns_greater_sma <- TTR::SMA(market_data$standardized_returns_greater, 100)
# plot(market_data$standardized_returns_greater_sma[1:10000])
market_data$standardized_returns_greater_sma_q <- roll::roll_quantile(
  market_data$standardized_returns_greater_sma,
  width = 12*8*200, p = 0.95, min_obs = 12*8*30)

# plots
autoplot(market_data[, c('standardized_returns_greater_q', 'standardized_returns_greater_sma_q')])
autoplot(market_data[, c('standardized_returns_greater_q', 'standardized_returns_greater_sma_q')])

# generate sides
results_backcusum <- na.omit(market_data)
side <- vector(mode = 'integer', length = nrow(results_backcusum))
indicator <- as.vector(zoo::coredata(results_backcusum$standardized_returns_greater_sma))
indicator_quantile <- as.vector(zoo::coredata(results_backcusum$standardized_returns_greater_sma_q))
for (i in 1:nrow(results_backcusum)) {
  if (i == 1) {
    side[i] <- NA
  } else if (indicator[i -1] > indicator_quantile[i-1]) {
    side[i] <- 0L
  } else {
    side[i] <- 1L
  }
}
results_backcusum$side_backcusum <- side
table(results_backcusum$side_backcusum)
results_backcusum$returns_strategy_backcusum <- results_backcusum$returns * results_backcusum$side_backcusum
PerformanceAnalytics::charts.PerformanceSummary(
  results_backcusum[, c('returns', "returns_strategy_backcusum")], plot.engine = 'ggplot2')


# # Backtest results
# backtest_results <- lapply(
#   results[, grepl('bc_', colnames(results))],
#   function(x) {
#     lapply(results[, sum_returns_colnames],
#            function(y) {
#              backtest(results, x, y)
#            })
#   })
# backtest_results <- purrr::pluck(purrr::flatten(purrr::flatten(backtest_results)))
# backtest_charts <- backtest_results[seq(2, length(backtest_results), by = 2)]
# backtest_returns <- backtest_results[seq(1, length(backtest_results), by = 2)]
#
# # save charts
# backtest_charts_names <- lapply(
#   colnames(results)[grepl('bc_', colnames(results))],
#   function(x) {
#     lapply(sum_returns_colnames,
#            function(y) {
#              paste0(file_name, x, y, '.png')
#            })
#   })
# backtest_charts_names <- purrr::flatten(backtest_charts_names)
# purrr::map2(backtest_charts_names, backtest_charts, ~ ggsave(filename = .x, plot = .y))
#
# # scalars
# lapply(backtest_returns, function(x) {
#   daily_returns <- apply.monthly(x, Return.cumulative)
#   # sharpe ratio
#   sr <- PerformanceAnalytics::SharpeRatio.annualized(daily_returns)
#   cat("sharpe_ratio:", sr[1, 2], "\n")
#   # cumulative return
#   cum_return <- PerformanceAnalytics::Return.cumulative(daily_returns)
#   cat("cumulative_return:", cum_return[1, 2], "\n")
# })
