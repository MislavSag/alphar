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

# Remove duplicates - more compicated
dups = prices[, .(symbol , n = .N),
              by = .(date, open, high, low, close, volume, adj_close,
                     symbol_first = substr(symbol, 1, 1))]
dups = dups[n > 1]
dups[, symbol_short := gsub("\\.\\d$", "", symbol)]
symbols_remove = dups[, .(symbol, n = .N),
                      by = .(date, open, high, low, close, volume, adj_close,
                             symbol_short)]
symbols_remove[n >= 2, unique(symbol)]
symbols_remove = symbols_remove[n >= 2, unique(symbol)]
symbols_remove = symbols_remove[grepl("\\.", symbols_remove)]
prices = prices[symbol %nin% symbols_remove]

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
symbol_keep = prices[, .N, symbol][N >= 2 * 252, symbol]
prices = prices[symbol %chin% symbol_keep]

# keep 100 most liquid at every date
# 200 by volume => cca 20 mil rows
# 100 by volume => cca 13 mil rows
# 50 by volume => cca 8 mil rows
prices[, dollar_volume := close * volume]
setorder(prices, date, -dollar_volume)
liquid_symbols = prices[, .(symbol = first(symbol, 50)), by = date]
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
  # dt_[, last_month_day := shift(last_month_day, 1, type = "lead"), by = symbol]
  dt_ = dt_[last_month_day == TRUE]
  dt_[, let(last_month_day = NULL)]
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
                                           windows = c(5, 22, 66, 250),
                                           quantile_divergence_window =  c(22, 66, 250))
OhlcvFeaturesSet = OhlcvFeaturesInit$get_ohlcv_features(copy(ohlcv$X))

# filter rows from Ohlcv we need
predictors = Reduce(function(x, y) merge(x, y, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE),
                    list(OhlcvFeaturesSet[last_month_day == TRUE], exuber, binomialtrend))
setorderv(predictors, c("symbol", "date"))

# check if dates from Ohlcvfeatures and Rolling features are as expeted
predictors[1:5, c(1:15, 100:105)]
predictors[, .(symbol, date, month, returns_1, p_value_22)]
symbol_ = "aapl"
predictors[symbol == symbol_, .(symbol, date)]
# Not sure, but seems good

# check for duplicates
predictors[duplicated(predictors[, .(symbol, date)]), .(symbol, date)]
predictors[duplicated(predictors[, .(symbol, month)]), .(symbol, month)]

# define predictor columns
nonpredictor_keep = c("symbol", "date", "close", "close_raw", "month", "last_month_day", "returns")
nonpredictor_remove = c("open", "high", "low", "volume")
predictors_cols = setdiff(colnames(predictors), c(nonpredictor_keep, nonpredictor_remove))


# TARGET COLUMN -----------------------------------------------------------
# one month future return by symbol is target olumn. Used for gausscov
setorder(predictors, symbol, month)
predictors[, target := shift(close, -1, type = "shift") / close - 1, by = .(symbol)]

# check target
predictors[, .(symbol, date, month, close, target)]

# add target to columns we keep
nonpredictor_keep = c(nonpredictor_keep, "target")


# PREPROCESSING -----------------------------------------------------------
# convert columns to numeric. This is important only if we import existing features
chr_to_num_cols = setdiff(colnames(predictors[, .SD, .SDcols = is.character]), "symbol")
if (length(chr_to_num_cols) > 0) {
  predictors = predictors[, (chr_to_num_cols) := lapply(.SD, as.numeric), .SDcols = chr_to_num_cols]
}
log_to_num_cols = setdiff(colnames(predictors[, .SD, .SDcols = is.logical]), "last_month_day")
if (length(log_to_num_cols) > 0) {
  predictors = predictors[, (log_to_num_cols) := lapply(.SD, as.numeric), .SDcols = log_to_num_cols]
}

# remove duplicates
if (any(duplicated(predictors[, .(symbol, date)]))) {
  predictors = unique(predictors, by = c("symbol", "date"))
}

# remove columns with many NA
keep_cols = names(which(colMeans(!is.na(predictors)) > 0.5))
print(paste0("Removing columns with many NA values: ", setdiff(colnames(predictors), c(keep_cols, "right_time"))))
predictors = predictors[, .SD, .SDcols = keep_cols]

# remove Inf and Nan values if they exists
is.infinite.data.frame <- function(x) do.call(cbind, lapply(x, is.infinite))
keep_cols = names(which(colMeans(!is.infinite(as.data.frame(predictors))) > 0.98))
print(paste0("Removing columns with Inf values: ", setdiff(colnames(predictors), keep_cols)))
predictors = predictors[, .SD, .SDcols = keep_cols]

# remove inf values
n_0 = nrow(predictors)
predictors = predictors[is.finite(rowSums(predictors[, .SD, .SDcols = is.numeric], na.rm = TRUE))]
n_1 = nrow(predictors)
print(paste0("Removing ", n_0 - n_1, " rows because of Inf values"))

# Define predictors column again
predictors_cols = intersect(predictors_cols, colnames(predictors))

# Convert integer64 to numberic
cols = predictors[, colnames(.SD), .SDcols = bit64::is.integer64]
predictors[, (cols) := lapply(.SD, as.numeric), .SDcols = cols]

# save
fwrite(predictors, "F:/predictors/fi_clustering/predictors.csv")
