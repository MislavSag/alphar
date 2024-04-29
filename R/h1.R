library(data.table)
library(fs)
library(glue)
library(arrow)
library(lubridate)
library(roll)
library(finfeatures)
library(ggplot2)
library(QuantTools)
library(reticulate)


# SET UP ------------------------------------------------------------------
# python environment
reticulate::use_virtualenv("C:/Users/Mislav/projects_py/pyquant", required = TRUE)
tsfel = reticulate::import("tsfel", convert = FALSE)
tsfresh = reticulate::import("tsfresh", convert = FALSE)
warnigns = reticulate::import("warnings", convert = FALSE)
warnigns$filterwarnings('ignore')

# paths
PATH_PREDICTORS = "F:/predictors/intradayml"


# DATA --------------------------------------------------------------------
# Import Databento minute data
prices = read_parquet("F:/databento/minute_dt.parquet")
setDT(prices)

# Change timezone
attr(prices$ts_event, "tz")
prices[, ts_event := with_tz(ts_event, tz = "America/New_York")]
attr(prices$ts_event, "tz")

# Filter trading minutes
prices = prices[as.ITime(ts_event) %between% c(as.ITime("09:30:00"), as.ITime("16:00:00"))]

# Keep observations with at least one month of observations
id_n = prices[, .N, by = symbol]
remove_symbols = id_n[N < (60 * 8 * 22), unique(symbol)]
prices = prices[symbol %notin% remove_symbols]

# Remove columns we don't need
prices[, let(rtype = NULL, publisher_id = NULL)]

# Remove observation with missing close
prices = na.omit(prices, cols = "close")


# UNIVERSE ----------------------------------------------------------------
# Filter 10 most liquid symbols by month
prices[, let(
  month = ceiling_date(ts_event, "month"),
  dollar_volume = volume * close
)]
universe = prices[, .(dollar_volume = sum(dollar_volume, na.rm = TRUE)),
                 by = .(month, symbol)]
setorder(universe, month, -dollar_volume)
universe = universe[, first(.SD, 10), by = month]
symbols_keep = universe[, unique(symbol)]

# Filter prices by symbols_keep
prices_dt = prices[symbol %in% symbols_keep]
setorder(prices_dt, symbol, ts_event)

# Check if symbols and instruments ids are the same
prices_dt[, .N, by = .(symbol, instrument_id)]

# Free memory
rm(prices)
gc()

# Dummy variable whwn symbol is in universe
prices_dt = merge(prices_dt, universe[, .(symbol, month, in_universe = 1)],
                  by = c("symbol", "month"), all.x = TRUE)


# FREQUENCY ---------------------------------------------------------------
# Upsample to 5 minute frequency
prices_dt = prices_dt[, .(
  open = first(open),
  high = max(high),
  low = min(low),
  close = last(close),
  volume = sum(volume),
  in_universe = first(in_universe)
  ), by = .(symbol, ts_event = round_date(ts_event, "5 mins"))]
gc()

# Plot number of stocks in the universe by month
unique(prices_dt[in_universe == TRUE]
       [, .(month = ceiling_date(ts_event, "month"), symbol)])[order(month)][
         , .N, by = month] |>
  ggplot(aes(x = month, y = as.integer(N))) +
  geom_line() +
  theme_minimal() +
  labs(title = "Number of stocks in the universe by month",
       x = "",
       y = "Number of stocks") +
  theme(axis.title.x = element_blank())
# Seems rights


# PREDICTORS --------------------------------------------------------------
# Parameters
rolling_window_size = 420

# Create Ohlcv object that we can use with finfeatures package
ohlcv_cols = colnames(prices_dt)[1:7]
ohlcv = Ohlcv$new(prices_dt[, ..ohlcv_cols],
                  id_col = "symbol",
                  date_col = "ts_event",
                  price = "close")

# Define at_
all(prices_dt[, .(symbol, ts_event)] == ohlcv$X[, .(symbol, date)])
at = prices_dt[, which(in_universe == TRUE)]

# Exuber
path_ = path_ext_set(glue("{PATH_PREDICTORS}/exuber"), "csv")
if (file_exists(path_)) {
  exuber_predictors = fread(path_)
} else {
  exuber_init = RollingExuber$new(
    windows = rolling_window_size,
    workers = 4L,
    at = at,
    lag = 0L,
    exuber_lag = 1L
  )
  exuber_predictors = exuber_init$get_rolling_features(ohlcv, TRUE)
  fwrite(exuber_predictors, path_)
}

# Theft
path_ = path_ext_set(glue("{PATH_PREDICTORS}/theft"), "csv")
if (file_exists(path_)) {
  theft_predictors = fread(path_)
} else {
  rolling_theft = RollingTheft$new(
    windows = rolling_window_size,
    workers = 1L,
    at = at,
    lag = 0L,
    features_set = c("catch22", "feasts", "tsfel", "tsfresh")
  )
  predictors_theft = suppressMessages(rolling_theft$get_rolling_features(ohlcv))
  fwrite(predictors_theft, path_)
}

# Backcusum
path_ = path_ext_set(glue("{PATH_PREDICTORS}/backcusum"), "csv")
if (file_exists(path_)) {
  backcusum_predictors = fread(path_)
} else {
  exuber_init = RollingExuber$new(
    windows = rolling_window_size,
    workers = 4L,
    at = at,
    lag = 0L,
    exuber_lag = 1L
  )
  exuber_predictors = exuber_init$get_rolling_features(ohlcv, TRUE)
  fwrite(exuber_predictors, path_)
}


# Ohlcv predictors - we will split computatation in 2 parts to save RAM
ohlcv_features = OhlcvFeaturesDaily$new(
  windows = c(6, 12, 24, 78, 78 * 5, 78 * 22),
  quantile_divergence_window = c(78, 78 * 5, 78 * 22)
)
predictors_ohlcv = ohlcv_features$get_ohlcv_features(ohlcv$X)



# LABELING ----------------------------------------------------------------
# Next bars returns
horizonts = c(1, 12)
cols_ = paste0("target_ret_", horizonts)
future_ret = function(c, n) shift(c, n) / c - 1
prices_dt[, (cols_) := lapply(horizonts, function(h) {
  shift(close, -h, type = "shift") / close - 1
}), by = symbol]

# TODO: Add tripple barrier
# Tripple bariaer labeling
# daily_vol = mlfinlab$utils$get_daily_vol(close = prices_dt$close, lookback = rolling_window_size)


# BAR FITLERING -----------------------------------------------------------
# Calculate returns
setorder(prices_dt, symbol, ts_event)
prices_dt[, returns := close / shift(close) - 1, by = symbol]

# Parameters
rolling_window_size = 420 # cca month: (12 * 7 * 5)

# 1) Rolling standard deviation of returns
prices_dt[, roll_sd_returns := roll_sd(returns, rolling_window_size), by = symbol]

# 2) Rolling standard deviation of volume
prices_dt[, roll_sd_volumes := roll_sd(volume, rolling_window_size), by = symbol]

# 3) Rolling percent rank
prices_dt[, roll_prank := roll_percent_rank(close, rolling_window_size), by = symbol]

# 4) Rolling RSI
prices_dt[, roll_rsi := rsi(close, rolling_window_size), by = symbol]
