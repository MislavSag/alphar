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
source('C:/Users/Mislav/Documents/GitHub/alphar/R/parallel_functions.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/outliers.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/import_data.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/features.R')

# performance
plan(multiprocess(workers = availableCores() - 8))  # multiprocess(workers = availableCores() - 8)




# PARAMETERS --------------------------------------------------------------

# before backcusum function
contract = 'SPY5'
upsample = FALSE
# model parameters
roll_window = 150
moving = 'none'  # none, sma, ema
moving_window = 20
var_threshold = -0.002
# file name prefix
now <- gsub(' |:', '-', as.character(Sys.time()))
file_name <- paste0('var-', contract, '_', now)



# IMPORT DATA -------------------------------------------------------------


# HFD
market_data <- import_mysql(
  contract = contract,
  save_path = 'D:/market_data/usa/ohlcv',
  trading_days = TRUE,
  upsample = upsample,
  RMySQL::MySQL(),
  dbname = 'odvjet12_market_data_usa',
  username = 'odvjet12_mislav',
  password = 'Theanswer0207',
  host = '91.234.46.219'
)
# vix <- import_mysql(
#   contract = 'VIX5',
#   save_path = 'D:/market_data/usa/ohlcv',
#   trading_days = TRUE,
#   upsample = upsample,
#   RMySQL::MySQL(),
#   dbname = 'odvjet12_market_data_usa',
#   username = 'odvjet12_mislav',
#   password = 'Theanswer0207',
#   host = '91.234.46.219'
# )

# LFD
market_data <- import_ib(
  contract = 'SPY',
  frequencies = '1 hour',
  duration = '16 Y',
  type = 'equity',
  trading_days = FALSE
)
colnames(market_data) <- c('open', 'high', 'low', 'close', 'volume', 'WAP', 'hasGaps', 'count')
# vix <- import_ib(
#   contract = 'VIX',
#   frequencies = '1 hour',
#   duration = '16 Y',
#   type = 'index',
#   trading_days = FALSE
# )
# colnames(vix) <- c('open', 'high', 'low', 'close', 'volume', 'WAP', 'hasGaps', 'count')

# FUNDAMENTALS
# fundamentals <- simfinapi::sfa_get_statement(Ticker = gsub('\\d+', '', contract),
#                                              statement = 'all',
#                                              period = 'quarters',
#                                              ttm = TRUE,
#                                              shares = TRUE,
#                                              api_key = '8qk9Xc9scFc0Rbpfrx6PLdaiomvi7Dxc')
# if (!is.null(fundamentals)) {
#   fundamentals <- janitor::clean_names(fundamentals)
#   fundamentals <- fundamentals[, .(publish_date, shares, sales_per_share)]
#   fundamentals <- xts::xts(fundamentals[, 2:ncol(fundamentals)], order.by = as.POSIXct(fundamentals$publish_date))
#   market_data <- merge(market_data, fundamentals, all = TRUE)
#   market_data$sales_per_share <- zoo::na.locf(market_data$sales_per_share)
#   market_data <- market_data[!is.na(market_data$close), ]
#   market_data$ps <- market_data$close / market_data$sales_per_share
# }



# PREPROCESSING -----------------------------------------------------------


# Remove outliers
market_data <- remove_outlier_median(market_data, median_scaler = 25)

# merge market data and VIX
# market_data <- merge(market_data, vix[, 'close'], join = 'left')
# colnames(market_data)[ncol(market_data)] <- 'vix'
# market_data <- na.omit(market_data)

# Add features
market_data <- add_features(market_data)

# Remove NA values
market_data <- market_data[, c('close', 'returns')]
market_data <- na.omit(market_data)



# VAR ---------------------------------------------------------------------

var_roll <- function(data, var = 'close', window_size = 100) {
  columns <- c('index', var)
  DT <- as.data.table(data)[, columns, with = FALSE]
  colnames(DT) <- c('time', 'price')
  DT <- DT[, time := as.numeric(time)]
  roll_f <- slider_parallel(
    .x = DT,
    .f =   ~ {
      PerformanceAnalytics::VaR(.x$price, p = 0.95, method = 'modified', clean = 'none', portfolio_method = 'single')
    },
    .before = window_size - 1,
    .after = 0L,
    .complete = TRUE,
    n_cores = -1
  )
  roll_f <- unlist(roll_f)
  roll_f <- c(rep(NA, window_size - 1), roll_f)
  return(roll_f)
}
varroll <- var_roll(market_data, 'returns', roll_window)
market_data$varroll <- varroll
sum(is.na(market_data$varroll))
market_data$varroll <- na.locf(market_data$varroll)
market_data$varroll_sma <- TTR::SMA(market_data$varroll, moving_window)
market_data$varroll_ema <- TTR::EMA(market_data$varroll, moving_window)

# merge all
voll_results <- market_data
voll_results <- stats::na.omit(voll_results)

# backtest
side <- vector(mode = 'integer', length = nrow(voll_results))
if (moving == 'none') {
  varq <- as.vector(zoo::coredata(voll_results$varroll))
} else if (moving == 'sma') {
  varq <- as.vector(zoo::coredata(voll_results$varroll_sma))
} else if (moving == 'sma') {
  varq <- as.vector(zoo::coredata(voll_results$varroll_ema))
}
quantile(varq, seq(0, 1, 0.05))
for (i in 1:nrow(voll_results)) {
  if (i %in% c(1, 2)) {
    side <- c(NA, 1)
  } else if (varq[i-1] < var_threshold) {  # } else if (varq[i-1] < var_threshold) {
    side[i] <- 0
  } else {
    side[i] <- 1
  }
}
table(side)
voll_results$side <- side
voll_results$returns_strategy <- voll_results$returns * voll_results$side

# save graph with cumulative returns and drowdown
p <- charts.PerformanceSummary(voll_results[, c('returns', 'returns_strategy')], plot.engine = 'ggplot2')
ggplot2::ggsave(paste0(file_name, '.png'), p)

# save xcalars
strategy_return <- Return.cumulative(voll_results$returns_strategy, geometric = TRUE)
cat(paste0('cumulative_return: ', round(strategy_return, 3)))
