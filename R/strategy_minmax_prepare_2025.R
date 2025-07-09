library(fastverse)
library(roll)
library(finutils)
library(PerformanceAnalytics)
library(moments)
library(ggplot2)
library(lubridate) # Remove afer vorrecting in finutils


# Import QC hourly data
prices = qc_hour(
  file_path = "F:/lean/data/sp_500_hour.csv",
  min_obs = 1000*7,
  add_dv_rank = TRUE,
  duplicates = "fast"
)
spy = prices[.("spy")]

# Keep only columns we need
prices = prices[, .(symbol, date, close, close_raw, volume, returns)]

# Calculate rolling quantiles
windows = c(22, 66, 125, 252, 504, 1000)
# windows = c(22, 66)
windows = windows * 7
quantiles = c(0.999, 0.99, 0.975, 0.95, 0.9)
# quantiles = c(0.999, 0.99)
quantiles = c(quantiles,  1 - quantiles)
new_col_names = lapply(windows, function(w) {
  sapply(quantiles, function(q) {
    paste0("p_", q*1000, "_", w)
  })
})
new_col_names = c("date", unlist(new_col_names))
minmax_dt = prices[, sapply(windows, function(w) {
  lapply(quantiles, function(q) {
    roll::roll_quantile(returns, w, p = q)
  })
}), by = symbol]
setnames(minmax_dt, c("symbol", new_col_names[new_col_names != "date"]))
prices = cbind(prices, minmax_dt[, .SD, .SDcols = -"symbol"])

# Extreme returns:
# 1) Returns above upper percentile - percentile
# 2) Returns below lower percentile - percentile
cols = colnames(prices)[grep("^p_9", colnames(prices))]
cols_new_up = paste0("above_", cols)
prices[, (cols_new_up) := lapply(.SD, function(x) ifelse(returns > shift(x), returns - shift(x), 0)),
       by = .(symbol), .SDcols = cols]
cols = colnames(prices)[grep("^p_[0-8]", colnames(prices))]
cols_new_down = paste0("below_", cols)
prices[, (cols_new_down) := lapply(.SD, function(x) ifelse(returns < shift(x), abs(returns - shift(x)), 0)),
   by = .(symbol), .SDcols = cols]

# Help function to calcualte tail risk measures from panel
# We can define how to represent distribution by using FUN argument. For example
# if we use mean, we get mean of all values below or above the percentile.
# If we use sd, we get standard deviation of all values below or above the percentile.
tail_risk = function(dt, FUN = mean, cols_prefix = "mean_") {
  cols = colnames(dt)[grep("below_p|above_p", colnames(dt))]
  indicators_ = dt[, lapply(.SD, function(x) f(x, na.rm = TRUE)),
                   by = .(date), .SDcols = cols,
                   env = list(f = FUN)]
  colnames(indicators_) = c("date", paste0(cols_prefix, cols))
  setorder(indicators_, date)
  above_sum_cols = colnames(indicators_)[grep("above", colnames(indicators_))]
  below_sum_cols = colnames(indicators_)[grep("below", colnames(indicators_))]
  excess_sum_cols = gsub("above", "excess", above_sum_cols)
  indicators_[, (excess_sum_cols) := indicators_[, ..above_sum_cols] - indicators_[, ..below_sum_cols]]
}

# Get tail risk mesures
indicators_mean      = tail_risk(prices, FUN = "mean", cols_prefix = "mean_")
indicators_sd        = tail_risk(prices, FUN = "sd", cols_prefix = "sd_")
indicators_sum       = tail_risk(prices, FUN = "sum", cols_prefix = "sum_")
indicators_skewness  = tail_risk(prices, FUN = "skewness", cols_prefix = "skewness_")
indicators_kurtosis  = tail_risk(prices, FUN = "kurtosis", cols_prefix = "kurtosis_")

# Merge indicators and spy
indicators = Reduce(function(x, y) merge(x, y, by = "date", all.x = TRUE, all.y = FALSE),
                    list(indicators_mean, indicators_sd, indicators_sum,
                         indicators_skewness, indicators_kurtosis))

# Merge SPY and indicators
back = indicators[spy[, .(date, close, returns)], on = "date"]

# Save prices and back
fwrite(prices, "D:/strategies/minmax/minmax_prices.csv", row.names = FALSE)
fwrite(back, "D:/strategies/minmax/minmax_back.csv", row.names = FALSE)
