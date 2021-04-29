#' Remove outliers
#'
#' @param ohlcv xts object with intraday frequency
#' @param ohlcv_daily xts object with daily frequency
#' @param median_scaler median scaler
#'
#' @return xts object without outliers
#' @export
#'
#' @import xts
#' @import quantmod
#'
#' @examples
remove_outlier_median <- function(data, median_scaler = 20) {

  # extract ohlcv from data
  ohlcv <- quantmod::OHLC(data)
  ohlcv <- ohlcv[, c('open', 'high', 'low', 'close')]

  # if data is higher than daily don't remove outliers
  if (deltat(ohlcv) >= 3600) {
    print('The method works only for intraday data.')
    return(ohlcv)
  }

  # con if intraday data
  ohlcv_daily <- xts::to.daily(ohlcv)
  ohlcv_daily <- ohlcv_daily[unique(as.Date(zoo::index(ohlcv)))]
  ohlcv_daily <- Cl(ohlcv_daily)

  daily_diff <- na.omit(abs(diff(ohlcv_daily)) + 0.05) * median_scaler
  daily_diff <- data.frame(date = as.Date(as.character(zoo::index(daily_diff))), zoo::coredata(daily_diff))
  data_test <- na.omit(diff(ohlcv))
  data_test <- data.frame(date_time = as.POSIXct(zoo::index(data_test)), zoo::coredata(data_test))
  data_test$date <- as.Date(data_test$date_time)
  data_test_diff <- base::merge(data_test, daily_diff, by = 'date', all.x = TRUE, all.y = FALSE)
  indexer <- abs(Cl(data_test_diff)[1]) < abs(Cl(data_test_diff)[2]) &
    abs(Op(data_test_diff)) < abs(Cl(data_test_diff)[2]) &
    abs(Hi(data_test_diff)) < abs(Cl(data_test_diff)[2]) &
    abs(Lo(data_test_diff)) < abs(Cl(data_test_diff)[2])
  ohlcv <- ohlcv[which(indexer), ]

  # merge with data
  data <- merge(ohlcv, data)
  data <- na.omit(data)
  data <- data[, -which(grepl('\\.1', colnames(data)))]

  return(data)
}
