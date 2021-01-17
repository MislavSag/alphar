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
source('C:/Users/Mislav/Documents/GitHub/alphar/R/execution.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/features.R')

# performance
plan(multiprocess(workers = availableCores() - 8))  # multiprocess(workers = availableCores() - 8)



# PARAMETERS --------------------------------------------------------------

# before backcusum function
contract = 'SPY5'
upsample = FALSE
# dpseg
dpseg_p = 0.1
window_dpseg = 500
quantiles_dpseg = 0.96
quantiles_dpseg_width = 5*12*8*1000  # 1000 days (3 years)
# file name prefix
now <- gsub(' |:', '-', as.character(Sys.time()))
file_name <- paste0(contract, '_', now)



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
vix <- import_mysql(
  contract = 'VIX5',
  save_path = 'D:/market_data/usa/ohlcv',
  trading_days = TRUE,
  upsample = upsample,
  RMySQL::MySQL(),
  dbname = 'odvjet12_market_data_usa',
  username = 'odvjet12_mislav',
  password = 'Theanswer0207',
  host = '91.234.46.219'
)

# FUNDAMENTALS
fundamentals <- simfinapi::sfa_get_statement(Ticker = gsub('\\d+', '', contract),
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



# PREPROCESSING -----------------------------------------------------------


# Remove outliers
market_data <- remove_outlier_median(market_data, median_scaler = 25)

# merge market data and VIX
market_data <- merge(market_data, vix[, 'close'], join = 'left')
colnames(market_data)[ncol(market_data)] <- 'vix'
market_data <- na.omit(market_data)

# Add features
market_data <- add_features(market_data)

# lags
market_data$returns_lag <- data.table::shift(market_data$returns)
market_data$vix_lag <- data.table::shift(market_data$vix)

# Remove NA values
market_data <- na.omit(market_data)



# VIX QUANTILES --------------------------------------------------------------

# vix quantiles
market_data$vix_quantile_90 <- roll::roll_quantile(market_data$vix, width = quantiles_dpseg_width, p = 0.90, min_obs = 50)
results <- na.omit(market_data)

# backtest
vixv <- as.vector(zoo::coredata(results$vix))
vix_quant <- as.vector(zoo::coredata(results$vix_quantile_90))
for (i in 1:nrow(results)) {
  if (i == 1) {
    side[i] <- NA
  } else if (vixv[i-1] > vix_quant[i-1]) {
    side[i] <- 0
  } else {
    side[i] <- 1
  }
}
table(side)
results$side_vix_quantile <- side
results$returns_strategy <- results$returns * results$side_vix_quantile
charts.PerformanceSummary(results[, c('returns', "returns_strategy")], plot.engine = 'ggplot2')



# VIX GROWTHS -------------------------------------------------------------

# vix quantiles
market_data$vix_growths <- (market_data$vix - data.table::shift(market_data$vix, 5)) / data.table::shift(market_data$vix, 5)
results <- na.omit(market_data)

# backtest
vixg <- as.vector(zoo::coredata(results$vix_growths))
for (i in 1:nrow(results)) {
  if (i == 1) {
    side[i] <- NA
  } else if (vixg[i-1] > 0.01) {
    side[i] <- 0
  } else if (side[i-1] == 0 & vixg[i-1] > -0.02) {
    side[i] <- 0
  } else {
    side[i] <- 1
  }
}
table(side)
results$side_vix_quantile <- side
results$returns_strategy <- results$returns * results$side_vix_quantile
charts.PerformanceSummary(results[, c('returns', "returns_strategy")], plot.engine = 'ggplot2')

