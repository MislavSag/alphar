library(data.table)
library(arrow)
library(duckdb)
library(lubridate)
library(glue)
library(finfeatures)
library(reticulate)


# SET UP ------------------------------------------------------------------
# python environment
reticulate::use_virtualenv("C:/Users/Mislav/projects_py/pyquant", required = TRUE)
tsfel = reticulate::import("tsfel", convert = FALSE)
tsfresh = reticulate::import("tsfresh", convert = FALSE)
warnigns = reticulate::import("warnings", convert = FALSE)
warnigns$filterwarnings('ignore')

# paths
PATH_PREDICTORS = "F:/predictors/spyml"


# DATA --------------------------------------------------------------------
# Import SPY data using duckdb
get_symbol = function(symbol) {
  con <- dbConnect(duckdb::duckdb())
  query <- sprintf("
  SELECT *
  FROM 'F:/databento/minute.parquet'
  WHERE Symbol = '%s'
", symbol)
  data_ <- dbGetQuery(con, query)
  dbDisconnect(con)
  setDT(data_)
  return(na.omit(data_))
}
spy = get_symbol("SPY")
spy[, ts_event := with_tz(ts_event, tz = "America/New_York")]

# Filter trading minutes
spy = spy[as.ITime(ts_event) %between% c(as.ITime("09:30:00"), as.ITime("16:00:00"))]

# Keep columns we need
spy = spy[, .(date = ts_event, open, high, low, close, volume)]

# Upsample to 5 minute frequency
spy = spy[, .(
  open = first(open),
  high = max(high),
  low = min(low),
  close = last(close),
  volume = sum(volume)
), by = .(date = round_date(date, "5 mins"))]
gc()


# PREDICTORS --------------------------------------------------------------
# Create Ohlcv object
ohlcv = Ohlcv$new(spy[, .(symbol = "SPY", date, open, high, low, close, volume)])

# Exuber
path_ = glue("{PATH_PREDICTORS}/exuber.csv")
if (file.exists(path_)) {
  exuber = fread(path_)
} else {
  exuber_init = RollingExuber$new(
    windows = c(400, 400*2),
    workers = 4L,
    at = 1:nrow(ohlcv$X),
    lag = 0L,
    exuber_lag = 1L
  )
  exuber = exuber_init$get_rolling_features(ohlcv, TRUE)
  fwrite(exuber, path_)
}

# Backcusum
path_ = glue("{PATH_PREDICTORS}/backcusum.csv")
if (file.exists(path_)) {
  backcusum = fread(path_)
} else {
  backcusum_init = RollingBackcusum$new(
    windows = c(200), # slow with high windows
    workers = 6L,
    at = 1:nrow(ohlcv$X),
    lag = 0L,
    alternative = c("greater", "two.sided"),
    return_power = c(1, 2))
  backcusum = backcusum_init$get_rolling_features(ohlcv)

  fwrite(backcusum, path_)
}

# Theft py
path_ = glue("{PATH_PREDICTORS}/theft_py.csv")
if (file.exists(path_)) {
  theft_py = fread(path_)
} else {
  theft_init = RollingTheft$new(
    windows = 400, # one window since it is memory intensive for 2 windows
    workers = 1L,
    at = 1:nrow(ohlcv$X),
    lag = 0L,
    features_set = c("tsfel")) # "catch22", "feasts",
  theft_py = suppressMessages(theft_init$get_rolling_features(ohlcv))
  fwrite(theft_py, path_)
}

# Theft py 2
path_ = glue("{PATH_PREDICTORS}/theft_py2.csv")
if (file.exists(path_)) {
  theft_py2 = fread(path_)
} else {
  theft_init = RollingTheft$new(
    windows = 100,
    workers = 1L,
    at = 1:nrow(ohlcv$X),
    lag = 0L,
    features_set = c("tsfel", "tsfresh"))
  theft_py2 = suppressMessages(theft_init$get_rolling_features(ohlcv))
  fwrite(theft_py2, path_)
}

# Theft r
path_ = glue("{PATH_PREDICTORS}/theft_r.csv")
if (file.exists(path_)) {
  theft_r = fread(path_)
} else {
  theft_init = RollingBackcusum$new(
    windows = 400,
    workers = 1L,
    at = 1:nrow(ohlcv$X),
    lag = 0L,
    features_set = c("catch22", "feasts"))
  theft_r = theft_init$get_rolling_features(ohlcv)
  fwrite(theft_r, path_)
}

# Combine all rolling predictors
rolling_predictors <- Reduce(
  function(x, y) merge( x, y, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE),
  list(
    exuber,
    backcusum,
    theft_py,
    RollingTheftCatch22Features,
    RollingTheftTsfelFeatures,
    RollingTsfeaturesFeatures
  )
)

# Ohlcv features
ohlcv_init = OhlcvFeaturesDaily$new(
  windows = c(6, 12, 24, 48, 76, 76*2, 400*5, 400*22),
  quantile_divergence_window = c(60*4, 400, 400*2, 400*5, 400*22)
)
ohlcv_featues = ohlcv_init$get_ohlcv_features(copy(ohlcv$X))
at = 1:nrow(ohlcv$X)


# TARGETS -----------------------------------------------------------------
# Create target variable
horizonts = c(1, 12, 48, 76)
targets = paste0("target_", horizonts)
spy[, (targets) := lapply(horizonts,
                          function(x) shift(close, -x, type = "shift") / close - 1)]
