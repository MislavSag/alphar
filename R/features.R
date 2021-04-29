library(PerformanceAnalytics)
library(data.table)
library(roll)
library(xts)
library(future.apply)
library(RollingWindow)


# ohlc <- market_data

add_features <- function(ohlc, window_sizes = c(5, 10, 20, 40, 80, 150, 300)) {

  # add ohlc transformations
  ohlc$hi_lo <- ohlc$high - ohlc$low
  ohlc$cl_op <- ohlc$close - ohlc$open
  cl_ath <- cummax(ohlc$close)
  ohlc$cl_ath_dev <- (cl_ath - ohlc$close) / cl_ath

# returns
ohlc$returns <- PerformanceAnalytics::Return.calculate(ohlc$close)
ohlc$returns_squared <- (ohlc$returns)^2

# rolling volatility
stds <- future.apply::future_sapply(
  X = window_sizes,
  FUN = function(x) roll::roll_sd(zoo::coredata(ohlc$returns), x)
)
colnames(stds) <- paste0('std_', window_sizes)
ohlc <- cbind(ohlc, stds)

# Close-to-Close Volatility
n_ <- 5
ohlc$ctc <- volatility(OHLC(ohlc), n = n_, calc = "close")
ohlc$parkinson <- volatility(OHLC(ohlc), n = n_, calc = "parkinson")
ohlc$rogers <- volatility(OHLC(ohlc), n = n_, calc = "rogers.satchell")
ohlc$gkyz <- volatility(OHLC(ohlc), n = n_, calc = "gk.yz")
ohlc$yz <- volatility(OHLC(ohlc), n = n_, calc = "yang.zhang")

# skewness
skews <- future.apply::future_sapply(
  window_sizes,
  function(x) RollingWindow::RollingSkew(zoo::coredata(ohlc$returns), window = x, na_method = 'ignore')
)
colnames(skews) <- paste0('skewness_', window_sizes)
ohlc <- cbind(ohlc, xts::xts(skews, order.by = zoo::index(ohlc)))

# kurtosis
kurts <- future.apply::future_sapply(
  window_sizes,
  function(x)  RollingWindow::RollingKurt(zoo::coredata(ohlc$returns), window = x, na_method = 'ignore')
)
colnames(kurts) <- paste0('kurtosis_', window_sizes)
ohlc <- cbind(ohlc, xts::xts(kurts, order.by = zoo::index(ohlc)))

# Skewness-Kurtosis ratio
sk_ratio <- ohlc[, paste0('skewness_', window_sizes)] / ohlc[, paste0('kurtosis_', window_sizes)]
colnames(sk_ratio) <- paste0('skewness_kurtosis_', window_sizes)
ohlc <- cbind(ohlc, xts::xts(sk_ratio, order.by = zoo::index(ohlc)))

# standardized returns
ohlc$standardized_returns <- ohlc$returns / ohlc$std_5
ohlc$standardized_returns_squared <- (ohlc$standardized_returns)^2

return(ohlc)
}
