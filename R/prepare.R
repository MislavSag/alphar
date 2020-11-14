library(xts)
library(quantmod)
library(fmlr)
library(tseries)
library(reticulate)
source('R/import_data.R')
source('R/features.R')
source('R/outliers.R')
source('R/filtering.R')
# python packages
py <- reticulate::use_python('C:/ProgramData/Anaconda3')
ml <- reticulate::import('mlfinlab')
pd <- reticulate::import('pandas')



# PARAMETERS --------------------------------------------------------------

de_prado_fracdiff = TRUE
filtering = 'cusum'


# ANALYSIS ----------------------------------------------------------------

# Import data
market_data <- import_mysql(
  contract = 'SPY_IB',
  save_path = 'D:/market_data/usa/ohlcv',
  RMySQL::MySQL(),
  dbname = 'odvjet12_market_data_usa',
  username = 'odvjet12_mislav',
  password = 'Theanswer0207',
  host = '91.234.46.219'
)

# convert to xta
market_data_xts <- xts::xts(market_data[, 2:(ncol(market_data)-1)], order.by = market_data$date)

# Remove outliers
market_data_xts <- remove_outlier_median(quantmod::OHLC(market_data_xts), median_scaler = 25)

# Add features
market_data_xts <- add_features(market_data_xts)

# Remove constant columns (from https://stackoverflow.com/questions/15068981/removal-of-constant-columns-in-r)
market_data_xts <- market_data_xts[,!apply(market_data_xts, MARGIN = 2, function(x) max(x, na.rm = TRUE) == min(x, na.rm = TRUE))]

############# FRACDIFF TO DO
# Fractionally differentiated series
# test <- fmlr::fracDiff(market_data$close, d = 0.5)

# adf_tests <- lapply(market_data_xts, adf.test, alternative = 'stationary')
# p_values <- lapply(adf_tests, purrr::pluck, 'p.value')
#
# plot(market_data$high)
#
# values <- seq(0, 1, 0.1)
# for (v in values) {
#   adf_test <- adf.test(market_data$close, alternative = 'stationary')
#   if (adf_test$p.value > 0.1) {
#     test <- fmlr::fracDiff(market_data$close, d = 0.5)
#   }
# }
############# FRACDIFF TO DO

# filtering
data <- market_data_xts$close

POS <- NEG <- 0
index <- NULL
diff_data <- diff(data)
for (i in 1:length(diff_data)){
  POS <- max(0,POS+diff_data[i])
  NEG <- min(0,POS+diff_data[i])
  if(max(POS,-NEG)>=h){
    index <- c(index, i)
    POS <- NEG <- 0
  }
}
return(index+1)


daily_volume <- data.table::frollapply(market_data$close, n = 50, function(x) sd(x), fill = NA)
bar_data <- as.data.table(market_data_xts$close)
head(bar_data)
cusum_events <- CUSUM_Price(zoo::coredata(market_data_xts$close), h = mean(daily_volume, na.rm = TRUE))
head(cusum_events)



# t_events
#
# t_events = []
# s_pos = 0
# s_neg = 0
#
# # log returns
# raw_time_series = pd.DataFrame(raw_time_series)  # Convert to DataFrame
# raw_time_series.columns = ['price']
# raw_time_series['log_ret'] = raw_time_series.price.apply(np.log).diff()
# if isinstance(threshold, (float, int)):
#   raw_time_series['threshold'] = threshold
# elif isinstance(threshold, pd.Series):
#   raw_time_series.loc[threshold.index, 'threshold'] = threshold
# else:
#   raise ValueError('threshold is neither float nor pd.Series!')
#
# raw_time_series = raw_time_series.iloc[1:]  # Drop first na values
#
# # Get event time stamps for the entire series
# for tup in raw_time_series.itertuples():
#   thresh = tup.threshold
# pos = float(s_pos + tup.log_ret)
# neg = float(s_neg + tup.log_ret)
# s_pos = max(0.0, pos)
# s_neg = min(0.0, neg)
#
# if s_neg < -thresh:
#   s_neg = 0
# t_events.append(tup.Index)
#
# elif s_pos > thresh:
#   s_pos = 0
# t_events.append(tup.Index)
#
# # Return DatetimeIndex or list
# if time_stamps:
#   event_timestamps = pd.DatetimeIndex(t_events)
# return event_timestamps
#
# return t_events
