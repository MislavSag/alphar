# IMPORT DATA -------------------------------------------------------------


# Import data
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

# market_data <- import_ib(
#   contract = 'SPY',
#   frequencies = '1 hour',
#   duration = '15 Y'
# )

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



# PREPROCESSING -----------------------------------------------------------


# Remove outliers
market_data <- remove_outlier_median(market_data, median_scaler = 25)

# merge market data and VIX
market_data <- merge(market_data, vix[, 'close'], join = 'left')
colnames(market_data)[ncol(market_data)] <- 'vix'
market_data <- na.omit(market_data)
