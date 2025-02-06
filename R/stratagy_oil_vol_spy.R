library(findata)
library(fastverse)
library(fredr)
library(tinyplot)
library(ggplot2)
library(roll)
library(finutils)
library(PerformanceAnalytics)


# OIL WTI DATA ------------------------------------------------------------
# Set up
path_ = "F:/temp"
if (!fs::dir_exists(path_)) {
  fs::dir_create(path_)
}
macro = MacroData$new(path_to_dump = path_)

# Import Oil prices
# IMPORTANT: We ignore vintage dates
oil = fredr_series_observations(
  series_id = "DCOILWTICO",
  observation_start = as.Date("1900-01-01"),
  observation_end = Sys.Date()
)
setDT(oil)

# Keep columns we need
oil = oil[, .(date, price = value)]

# Remove missing values
oil = na.omit(oil, cols = "price")

# Plot oil prices
plt(price ~ date, data = oil, type = "l")

# Calcualte returns
oil[, oil_returns := price / shift(price) - 1]

# Calcualte volatility
windows = c(5, 10, 22, 66, 125, 252)
cols = paste0("vol_ret_", windows)
oil[, (cols) := lapply(windows, function(x) roll_sd(oil_returns, x))]
cols_vol_price = paste0("vol_price_", windows)
oil[, (cols_vol_price) := lapply(windows, function(x) roll_sd(price, x))]
melt(oil, id.vars = "date", measure.vars = cols) |>
  ggplot(aes(x = date, y = value, color = variable)) + geom_line()
melt(oil, id.vars = "date", measure.vars = cols_vol_price) |>
  ggplot(aes(x = date, y = value, color = variable)) + geom_line()

# Calcualte bad volatility
cols_bad = paste0("bad_vol", windows)
oil[oil_returns < 0, (cols_bad) := lapply(windows, function(x) roll_sd(oil_returns, x))]
cols_bad_vol_price = paste0("bad_vol_price_", windows)
oil[oil_returns < 0, (cols_bad_vol_price) := lapply(windows, function(x) roll_sd(price, x))]

# Plot bad volatilities
melt(oil, id.vars = "date", measure.vars = c(cols_vol_price[3], cols_bad_vol_price[3])) |>
  ggplot(aes(x = date, y = value, color = variable)) + geom_line()



# SPY DATA ----------------------------------------------------------------
# Import SPY data
spy = qc_daily(file_path = "F:/lean/data/stocks_daily.csv", symbols = "spy")

# Merge spy and oil
dt = oil[spy, on = "date"]

# nalocf all volatilities
setnafill(dt, type = "locf", cols = c(cols, cols_bad, cols_vol_price, cols_bad_vol_price))

# Additional steps
cols_lag = paste0(c(cols, cols_bad, cols_vol_price, cols_bad_vol_price), "_lag")
dt[, (cols_lag) := lapply(.SD, function(x) shift(x)),
   .SDcols = c(cols, cols_bad, cols_vol_price, cols_bad_vol_price)]
dt = na.omit(dt)


# ANALYSE -----------------------------------------------------------------
# # Create scatterplot that of lag oil volatilities and spy returns
# ggplot(dt, aes(x = vol_ret_5_lag, y = returns)) + geom_point() + geom_smooth(method = "lm")
# ggplot(dt, aes(x = vol_ret_10_lag, y = returns)) + geom_point() + geom_smooth(method = "lm")
# ggplot(dt, aes(x = vol_ret_22_lag, y = returns)) + geom_point() + geom_smooth(method = "lm")
# ggplot(dt, aes(x = vol_ret_66_lag, y = returns)) + geom_point() + geom_smooth(method = "lm")
# ggplot(dt, aes(x = vol_ret_125_lag, y = returns)) + geom_point() + geom_smooth(method = "lm")
# ggplot(dt, aes(x = vol_ret_252_lag, y = returns)) + geom_point() + geom_smooth(method = "lm")
#
# # Create scatterplot that of lag oil bad volatilities and spy returns
# ggplot(dt, aes(x = bad_vol5_lag, y = returns)) + geom_point() + geom_smooth(method = "lm")
# ggplot(dt, aes(x = bad_vol10_lag, y = returns)) + geom_point() + geom_smooth(method = "lm")
# ggplot(dt, aes(x = bad_vol22_lag, y = returns)) + geom_point() + geom_smooth(method = "lm")
# ggplot(dt, aes(x = bad_vol66_lag, y = returns)) + geom_point() + geom_smooth(method = "lm")
# ggplot(dt, aes(x = bad_vol125_lag, y = returns)) + geom_point() + geom_smooth(method = "lm")
# ggplot(dt, aes(x = bad_vol252_lag, y = returns)) + geom_point() + geom_smooth(method = "lm")

# Create scatterplot that of lag oil bad volatilities and spy returns
ggplot(dt, aes(x = vol_price_5_lag , y = returns)) + geom_point() + geom_smooth(method = "lm")
ggplot(dt, aes(x = vol_price_10_lag , y = returns)) + geom_point() + geom_smooth(method = "lm")
ggplot(dt, aes(x = vol_price_22_lag , y = returns)) + geom_point() + geom_smooth(method = "lm")
ggplot(dt, aes(x = vol_price_66_lag , y = returns)) + geom_point() + geom_smooth(method = "lm")
ggplot(dt, aes(x = vol_price_125_lag , y = returns)) + geom_point() + geom_smooth(method = "lm")
ggplot(dt, aes(x = vol_price_252_lag , y = returns)) + geom_point() + geom_smooth(method = "lm")

# Create scatterplot that of lag oil bad volatilities and spy returns
ggplot(dt, aes(x = bad_vol_price_5_lag  , y = returns)) + geom_point() + geom_smooth(method = "lm")
ggplot(dt, aes(x = bad_vol_price_10_lag , y = returns)) + geom_point() + geom_smooth(method = "lm")
ggplot(dt, aes(x = bad_vol_price_22_lag , y = returns)) + geom_point() + geom_smooth(method = "lm")
ggplot(dt, aes(x = bad_vol_price_66_lag , y = returns)) + geom_point() + geom_smooth(method = "lm")
ggplot(dt, aes(x = bad_vol_price_125_lag, y = returns)) + geom_point() + geom_smooth(method = "lm")
ggplot(dt, aes(x = bad_vol_price_252_lag, y = returns)) + geom_point() + geom_smooth(method = "lm")

# # SPY returns over volatility percentiles
# dt[, vol5_lag_q := cut(vol_ret_5_lag, breaks = quantile(vol5_lag, probs = seq(0, 1, 0.1), na.rm = TRUE))]
# dt[, vol10_lag_q := cut(vol10_lag, breaks = quantile(vol10_lag, probs = seq(0, 1, 0.1), na.rm = TRUE))]
# dt[, vol22_lag_q := cut(vol22_lag, breaks = quantile(vol22_lag, probs = seq(0, 1, 0.1), na.rm = TRUE))]
# dt[, vol66_lag_q := cut(vol66_lag, breaks = quantile(vol66_lag, probs = seq(0, 1, 0.1), na.rm = TRUE))]
# dt[, vol125_lag_q := cut(vol125_lag, breaks = quantile(vol125_lag, probs = seq(0, 1, 0.1), na.rm = TRUE))]
# dt[, vol252_lag_q := cut(vol252_lag, breaks = quantile(vol252_lag, probs = seq(0, 1, 0.1), na.rm = TRUE))]
#
# # SPY returns over bad volatility percentiles
# dt[, bad_vol5_lag_q := cut(bad_vol5_lag, breaks = quantile(bad_vol5_lag, probs = seq(0, 1, 0.1), na.rm = TRUE))]
# dt[, bad_vol10_lag_q := cut(bad_vol10_lag, breaks = quantile(bad_vol10_lag, probs = seq(0, 1, 0.1), na.rm = TRUE))]
# dt[, bad_vol22_lag_q := cut(bad_vol22_lag, breaks = quantile(bad_vol22_lag, probs = seq(0, 1, 0.1), na.rm = TRUE))]
# dt[, bad_vol66_lag_q := cut(bad_vol66_lag, breaks = quantile(bad_vol66_lag, probs = seq(0, 1, 0.1), na.rm = TRUE))]
# dt[, bad_vol125_lag_q := cut(bad_vol125_lag, breaks = quantile(bad_vol125_lag, probs = seq(0, 1, 0.1), na.rm = TRUE))]
# dt[, bad_vol252_lag_q := cut(bad_vol252_lag, breaks = quantile(bad_vol252_lag, probs = seq(0, 1, 0.1), na.rm = TRUE))]

# SPY returns over volatility percentiles
dt[, vol_price_5_lag_q := cut(vol_price_5_lag, breaks = quantile(vol_price_5_lag, probs = seq(0, 1, 0.1), na.rm = TRUE))]
dt[, vol_price_10_lag_q := cut(vol_price_10_lag, breaks = quantile(vol_price_10_lag, probs = seq(0, 1, 0.1), na.rm = TRUE))]
dt[, vol_price_22_lag_q := cut(vol_price_22_lag, breaks = quantile(vol_price_22_lag, probs = seq(0, 1, 0.1), na.rm = TRUE))]
dt[, vol_price_66_lag_q := cut(vol_price_66_lag, breaks = quantile(vol_price_66_lag, probs = seq(0, 1, 0.1), na.rm = TRUE))]
dt[, vol_price_125_lag_q := cut(vol_price_125_lag, breaks = quantile(vol_price_125_lag, probs = seq(0, 1, 0.1), na.rm = TRUE))]
dt[, vol_price_252_lag_q := cut(vol_price_252_lag, breaks = quantile(vol_price_252_lag, probs = seq(0, 1, 0.1), na.rm = TRUE))]

# # Plot
# ggplot(na.omit(dt), aes(x = vol5_lag_q, y = returns)) + geom_boxplot()
# ggplot(na.omit(dt), aes(x = vol10_lag_q, y = returns)) + geom_boxplot()
# ggplot(na.omit(dt), aes(x = vol22_lag_q, y = returns)) + geom_boxplot()
# ggplot(na.omit(dt), aes(x = vol66_lag_q, y = returns)) + geom_boxplot()
# ggplot(na.omit(dt), aes(x = vol125_lag_q, y = returns)) + geom_boxplot()
# ggplot(na.omit(dt), aes(x = vol252_lag_q, y = returns)) + geom_boxplot()
#
# # Plot bad volatilites
# ggplot(na.omit(dt), aes(x = bad_vol5_lag_q, y = returns)) + geom_boxplot()
# ggplot(na.omit(dt), aes(x = bad_vol10_lag_q, y = returns)) + geom_boxplot()
# ggplot(na.omit(dt), aes(x = bad_vol22_lag_q, y = returns)) + geom_boxplot()
# ggplot(na.omit(dt), aes(x = bad_vol66_lag_q, y = returns)) + geom_boxplot()
# ggplot(na.omit(dt), aes(x = bad_vol125_lag_q, y = returns)) + geom_boxplot()
# ggplot(na.omit(dt), aes(x = bad_vol252_lag_q, y = returns)) + geom_boxplot()

# Plot
ggplot(na.omit(dt), aes(x = vol_price_5_lag_q, y = returns)) + geom_boxplot()
ggplot(na.omit(dt), aes(x = vol_price_10_lag_q, y = returns)) + geom_boxplot()
ggplot(na.omit(dt), aes(x = vol_price_22_lag_q, y = returns)) + geom_boxplot()
ggplot(na.omit(dt), aes(x = vol_price_66_lag_q, y = returns)) + geom_boxplot()
ggplot(na.omit(dt), aes(x = vol_price_125_lag_q, y = returns)) + geom_boxplot()
ggplot(na.omit(dt), aes(x = vol_price_252_lag_q, y = returns)) + geom_boxplot()

# # Mean and meadian returns by volatility percentiles
# na.omit(dt)[, .(mean_returns = mean(returns)), by = vol5_lag_q] |>
#   ggplot(aes(x = vol5_lag_q, y = mean_returns)) + geom_bar(stat = "identity")
# na.omit(dt)[, .(median_returns = median(returns)), by = vol10_lag_q] |>
#   ggplot(aes(x = vol10_lag_q, y = median_returns)) + geom_bar(stat = "identity")
# na.omit(dt)[, .(mean_returns = mean(returns)), by = vol22_lag_q] |>
#   ggplot(aes(x = vol22_lag_q, y = mean_returns)) + geom_bar(stat = "identity")
# na.omit(dt)[, .(median_returns = median(returns)), by = vol66_lag_q] |>
#   ggplot(aes(x = vol66_lag_q, y = median_returns)) + geom_bar(stat = "identity")
# na.omit(dt)[, .(mean_returns = mean(returns)), by = vol125_lag_q] |>
#   ggplot(aes(x = vol125_lag_q, y = mean_returns)) + geom_bar(stat = "identity")
# na.omit(dt)[, .(median_returns = median(returns)), by = vol252_lag_q] |>
#   ggplot(aes(x = vol252_lag_q, y = median_returns)) + geom_bar(stat = "identity")
#
# # Mean and meadian returns by bad volatility percentiles
# na.omit(dt)[, .(mean_returns = mean(returns)), by = bad_vol5_lag_q] |>
#   ggplot(aes(x = bad_vol5_lag_q, y = mean_returns)) + geom_bar(stat = "identity")
# na.omit(dt)[, .(median_returns = median(returns)), by = bad_vol10_lag_q] |>
#   ggplot(aes(x = bad_vol10_lag_q, y = median_returns)) + geom_bar(stat = "identity")
# na.omit(dt)[, .(mean_returns = mean(returns)), by = bad_vol22_lag_q] |>
#   ggplot(aes(x = bad_vol22_lag_q, y = mean_returns)) + geom_bar(stat = "identity")
# na.omit(dt)[, .(median_returns = median(returns)), by = bad_vol66_lag_q] |>
#   ggplot(aes(x = bad_vol66_lag_q, y = median_returns)) + geom_bar(stat = "identity")
# na.omit(dt)[, .(mean_returns = mean(returns)), by = bad_vol125_lag_q] |>
#   ggplot(aes(x = bad_vol125_lag_q, y = mean_returns)) + geom_bar(stat = "identity")
# na.omit(dt)[, .(median_returns = median(returns)), by = bad_vol252_lag_q] |>
#   ggplot(aes(x = bad_vol252_lag_q, y = median_returns)) + geom_bar(stat = "identity")

# Mean and meadian returns by volatility percentiles
na.omit(dt)[, .(mean_returns = mean(returns)), by = vol_price_5_lag_q] |>
  ggplot(aes(x = vol_price_5_lag_q, y = mean_returns)) + geom_bar(stat = "identity")
na.omit(dt)[, .(median_returns = median(returns)), by = vol_price_10_lag_q] |>
  ggplot(aes(x = vol_price_10_lag_q, y = median_returns)) + geom_bar(stat = "identity")
na.omit(dt)[, .(mean_returns = mean(returns)), by = vol_price_22_lag_q] |>
  ggplot(aes(x = vol_price_22_lag_q, y = mean_returns)) + geom_bar(stat = "identity")
na.omit(dt)[, .(median_returns = median(returns)), by = vol_price_66_lag_q] |>
  ggplot(aes(x = vol_price_66_lag_q, y = median_returns)) + geom_bar(stat = "identity")
na.omit(dt)[, .(mean_returns = mean(returns)), by = vol_price_125_lag_q] |>
  ggplot(aes(x = vol_price_125_lag_q, y = mean_returns)) + geom_bar(stat = "identity")
na.omit(dt)[, .(median_returns = median(returns)), by = vol_price_252_lag_q] |>
  ggplot(aes(x = vol_price_252_lag_q, y = median_returns)) + geom_bar(stat = "identity")

# Simple backtest
ggplot(dt, aes(x = date, y = bad_vol_price_66_lag)) + geom_line()
setorder(dt, date)
back = dt[, .(date, signal = ifelse(vol_price_22_lag < 4, 1, 0), returns)]
back = back[, .(date, strategy = signal * returns, benchamrk = returns)]
charts.PerformanceSummary(as.xts.data.table(back))


charts.PerformanceSummary(as.xts.data.table(dt[, .(date, returns)]))


# MONYHLY ANALYSIS --------------------------------------------------------
# Calculate monthly realized volatility and badd realized monthly volatility
oilm = oil[, .(date, price, oil_returns)]
oilm[, month := data.table::yearmon(date)]
oilm[, vol := sum(oil_returns^2), by = month]
oilm[, bad_vol := sum(oil_returns[oil_returns < 0]^2, na.rm = TRUE), by = month]

# Downsample to monthly data
oilm = oilm[, .(
  date = data.table::last(date),
  price = data.table::last(price),
  vol = sum(vol),
  bad_vol = sum(bad_vol)
), by = month]
oilm = na.omit(oilm)

# Plot
ggplot(melt(oilm, id.vars = "date", measure.vars = c("vol", "bad_vol")),
       aes(x = date, y = value, color = variable)) +
  geom_line()

# Downsample SPY
spym = spy[, .(date, close)]
spym[, month := data.table::yearmon(date)]
spym = spym[, .(
  date = data.table::last(date),
  close = data.table::last(close)
), by = month]
spym = na.omit(spym)
spym[, returns := close / shift(close) - 1]

# Merge SPY and oil
dtm = oilm[spym, on = "month"]

# Shift
dtm[, vol_lag := shift(vol)]
dtm[, bad_vol_lag := shift(bad_vol)]
dtm = na.omit(dtm)

# Plot scatterplot between bad volatility and returns
ggplot(dtm, aes(x = vol_lag, y = returns)) + geom_point() + geom_smooth(method = "lm")
ggplot(dtm, aes(x = bad_vol_lag, y = returns)) + geom_point() + geom_smooth(method = "lm")
ggplot(dtm[vol_lag < 5], aes(x = vol_lag, y = returns)) + geom_point() + geom_smooth(method = "lm")
ggplot(dtm[bad_vol_lag < 5], aes(x = bad_vol_lag, y = returns)) + geom_point() + geom_smooth(method = "lm")
dtm[, sum(vol_lag < 5) / .N]
dtm[, sum(bad_vol_lag < 5) / .N]

# Backtest with volatility
back = dtm[, .(date, signal = ifelse(vol_lag < 1.5, 1, 0), returns)]
back = back[, .(date, strategy = signal * returns, benchamrk = returns)]
charts.PerformanceSummary(as.xts.data.table(back))

# Backtest with badvolatility
back = dtm[, .(date, signal = ifelse(bad_vol_lag < 0.5, 1, 0), returns)]
back = back[, .(date, strategy = signal * returns, benchamrk = returns)]
charts.PerformanceSummary(as.xts.data.table(back))

# Backtest with expected value from linear regression
dtm[, coef := roll_lm(bad_vol_lag, returns, width = .N, min_obs = 36)$coefficients[, 2]]
dtm[, prediction := coef * bad_vol_lag]
dtm[, signal := ifelse(prediction > -0.01, 1, 0)]
back = dtm[, .(date, signal, returns)]
back = back[, .(date, strategy = signal * returns, benchamrk = returns)]
charts.PerformanceSummary(as.xts.data.table(na.omit(back)))


# ARCHIVE ----------------------------------------------------------------
# # Get Oil prices from Fred
# id_ = "DCOILWTICO"
# fredr::fredr_set_key(Sys.getenv("FRED-KEY"))
# vin_dates = fredr_series_vintagedates(id_)
# date_vec = vin_dates[[1]]
# oil = macro$get_alfred(id_, date_vec)
#
# # Remove missing value
# oil = na.omit(oil, cols = "value")
#
# # Check duplicates
# anyDuplicated(oil[, date])
# oil[duplicated(date) | duplicated(date, fromLast = TRUE)]
# oil = oil[, .SD[date < realtime_start], by = date]
# anyDuplicated(oil[, date])
