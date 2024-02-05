library(data.table)
library(fs)
library(arrow)
library(finfeatures)


# DATA --------------------------------------------------------------------
# Import QC daily data
prices = fread("F:/lean/data/stocks_daily.csv")
setnames(prices, gsub(" ", "_", c(tolower(colnames(prices)))))

# Remove duplicates
prices = unique(prices, by = c("symbol", "date"))

# adjust all columns
prices[, adj_rate := adj_close / close]
prices[, let(
  open = open*adj_rate,
  high = high*adj_rate,
  low = low*adj_rate
)]
setnames(prices, "close", "close_raw")
setnames(prices, "adj_close", "close")
prices[, let(adj_rate = NULL)]
setcolorder(prices, c("symbol", "date", "open", "high", "low", "close", "volume"))

# Remove observations where open, high, low, close columns are below 1e-008
prices = prices[open > 1e-008 & high > 1e-008 & low > 1e-008 & close > 1e-008]

# Remove missing values
prices = na.omit(prices)

# Keep only symbol with at least 2 years of data
# 200 by volume => cca 20 mil rows
# 100 by volume => cca 13 mil rows
symbol_keep = prices[, .N, symbol][N >= 2 * 252, symbol]
prices = prices[symbol %chin% symbol_keep]


# keep 100 most liquid at every date
prices[, dollar_volume := close * volume]
setorder(prices, date, -dollar_volume)
liquid_symbols = prices[, .(symbol = first(symbol, 100)), by = date]
liquid_symbols = liquid_symbols[, unique(symbol)]
sprintf("We keep %f percent of data",
        length(liquid_symbols) / prices[, length(unique(symbol))] * 100)
prices = prices[symbol %chin% liquid_symbols]
prices[, dollar_volume := NULL]

# Sort
setorder(prices, symbol, date)

# Add monthly column
prices[, month := yearmon(date)]

# Create a column last_month_day that will be equal to TRUE if date is last day of the month
prices[, last_month_day := last(date) == date, by = c("symbol", "month")]

# Check if last_month_day works as expected
symbol_ = "aapl"
prices[symbol == symbol_, .(symbol, date, close, month, last_month_day)][1:99]
tail(prices[symbol == symbol_, .(symbol, date, close, month, last_month_day)], 100)

# Plot one stock
data_plot = as.xts.data.table(prices[symbol == symbol_, .(date, close)])
plot(data_plot, main = symbol_)

# create monthly prices
prices_monthly = prices[last_month_day == TRUE]

# free memory
gc()


# PEDICTORS ---------------------------------------------------------------
# help functions for predictors calculation
PATH = "F:/data/equity/us/predictors_daily"
symbols = prices[, unique(symbol)]
get_rolling_predictor = function(predictors_folder) {
  dt_ = lapply(dir_ls(path(PATH, predictors_folder)),
               function(x) cbind(symbol = path_ext_remove(path_file(x)), read_parquet(x)))
  dt_ = rbindlist(dt_, fill = TRUE)
  dt_ = merge(dt_, prices_monthly[, .(symbol, date, last_month_day)],
                 by = c("symbol", "date"), all.x = TRUE, all.y = FALSE)
  dt_[, last_month_day := shift(last_month_day, 1, type = "lead"), by = symbol]
  dt_ = dt_[last_month_day == TRUE]
  return(dt_)
}

# get_rolling prdictors
exuber    = get_rolling_predictor("exuber")
binomialtrend = get_rolling_predictor("binomialtrend")
# backcusum = get_rolling_predictor("backcusum")
gc()

# get Ohlcv predictors
ohlcv = Ohlcv$new(prices)

# free memory
rm(list = c("prices", "data_plot", "liquid_symbols", "symbols", "symbol_keep"))
gc()

# Features from OHLLCV
OhlcvFeaturesInit = OhlcvFeaturesDaily$new(at = NULL,
                                           windows = c(5, 22, 66, 250, 500),
                                           quantile_divergence_window =  c(22, 66, 250))
OhlcvFeaturesSet = OhlcvFeaturesInit$get_ohlcv_features(copy(ohlcv$X))
OhlcvFeaturesSetSample = OhlcvFeaturesSet[at_ - lag_]
setorderv(OhlcvFeaturesSetSample, c("symbol", "date"))

# check if dates from Ohlcvfeatures and Rolling features are as expeted
isin_ = "HRHT00RA0005"
OhlcvFeaturesSetSample[symbol == isin_, .(symbol, date_ohlcv = date)]
rolling_predictors[symbol == isin_ht, .(symbol, date_rolling = date)]
# Seems good!

# merge all predictors
rolling_predictors[, date_rolling := date]
OhlcvFeaturesSetSample[, date_ohlcv := date]
features = rolling_predictors[OhlcvFeaturesSetSample, on=c("symbol", "date"), roll = -Inf]

# check again merging dates
features[symbol == isin_ht, .(symbol, date_rolling, date_ohlcv, date)]
features[, max(date)]

# check for duplicates
features[duplicated(features[, .(symbol, date)]), .(symbol, date)]
features[duplicated(features[, .(symbol, date_ohlcv)]), .(symbol, date_ohlcv)]
features[duplicated(features[, .(symbol, date_rolling)]), .(symbol, date_rolling)]
features[duplicated(features[, .(symbol, date_rolling)]) | duplicated(features[, .(symbol, date_rolling)], fromLast = TRUE),
         .(symbol, date, date_ohlcv, date_rolling)]
features = features[!duplicated(features[, .(symbol, date_rolling)])]

# merge predictors and monthly prices
any(duplicated(prices[, .(isin, date)]))
any(duplicated(features[, .(symbol, date_rolling)]))
features[, .(symbol, date_rolling, date_ohlcv, date)]
prices[last_month_day == TRUE, .(isin, date, month)]
dt = merge(features, prices[last_month_day == TRUE, .(isin, date, month)],
           by.x = c("symbol", "date_rolling"), by.y = c("isin", "date"),
           all.x = TRUE, all.y = FALSE)
dt[, .(symbol, date, date_rolling, date_ohlcv)]
dt[duplicated(dt[, .(symbol, date)]), .(symbol, date)]
dt[duplicated(dt[, .(symbol, date_ohlcv)]), .(symbol, date_ohlcv)]
dt[duplicated(dt[, .(symbol, date_rolling)]), .(symbol, date_rolling)]

# remove missing ohlcv
dt = dt[!is.na(date_ohlcv)]

