library(data.table)
library(Rcpp)
library(purrr)
library(ggplot2)
library(quantmod)
library(future.apply)
library(anytime)
library(PerformanceAnalytics)
source('./R/parallel_functions.R')
source('./R/outliers.R')
source('./R/import_data.R')
source('./R/execution.R')



# PARAMETERS --------------------------------------------------------------


# before backcusum function
contract = 'SPY5'  # SPY for minute data, SPY5 for 5 minute data
upsample = FALSE  # e.g. 10 to aggregate 5 min to 10 min data



# IMPORT DATA -------------------------------------------------------------


# Import data
market_data <- import_mysql(
  contract = contract,
  save_path = 'D:/LUKA/Academic/aplharDta',
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
  save_path = 'D:/LUKA/Academic/aplharDta',
  trading_days = TRUE,
  upsample = upsample,
  RMySQL::MySQL(),
  dbname = 'odvjet12_market_data_usa',
  username = 'odvjet12_mislav',
  password = 'Theanswer0207',
  host = '91.234.46.219'
)



# PREPROCESSING -----------------------------------------------------------


# Remove outliers
market_data <- remove_outlier_median(market_data, median_scaler = 25)

# merge market data and VIX
market_data <- merge(market_data, vix[, 'close'], join = 'left')
colnames(market_data)[ncol(market_data)] <- 'vix'
market_data <- na.omit(market_data)


