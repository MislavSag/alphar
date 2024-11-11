library(fastverse)
library(arrow)
library(dplyr)
library(roll)
library(PerformanceAnalytics)
library(janitor)
library(AzureStor)
library(qlcal)


# SETUP -------------------------------------------------------------------
# Paths
PATH     = "F:/data/equity/us/predictors_daily"
PAHDAILY = "F:/lean/data/stocks_daily.csv"
PATHSAVE = "F:/data/strategies/gausscov"

# Azure credentials
endpoint = "https://snpmarketdata.blob.core.windows.net/"
key = Sys.getenv("BLOB-KEY-SNP")
# key = "0M4WRlV0/1b6b3ZpFKJvevg4xbC/gaNBcdtVZW+zOZcRi0ZLfOm1v/j2FZ4v+o8lycJLu1wVE6HT+ASt0DdAPQ=="
BLOBENDPOINT = storage_endpoint(endpoint, key=key)
cont = storage_container(BLOBENDPOINT, "qc-live")

# Set calendar
qlcal::calendars
qlcal::setCalendar("UnitedStates/NYSE")


# IMPORT DAILY DATA -------------------------------------------------------
# import daily data
col = c("date", "open", "high", "low", "close", "volume", "close_adj", "symbol")
dt = fread(PAHDAILY, col.names = col)
dt = unique(dt, by = c("symbol", "date"))
unadjustd_cols = c("open", "high", "low")
dt[, (unadjustd_cols) := lapply(.SD, function(x) (close_adj / close) * x), .SDcols = unadjustd_cols]
dt = na.omit(dt)
setorder(dt, symbol, date)
setnames(dt, c("close", "close_adj"), c("close_raw", "close"))
dt = dt[open > 0.00003 & high > 0.00003 & low > 0.00003 & close > 0.00003]

# remove symbols with less than 1 year of data
dt_n = dt[, .N, by = symbol]
symbols_keep = dt_n[N > 21*12, symbol]
dim(dt)
dt = dt[symbol %chin% symbols_keep]
dim(dt)

# sort
setorder(dt, symbol, date)

# create standardized forward returns
dt[, returns := close / shift(close) - 1, by = symbol]
dt[, roll_std_q := roll_sd(returns, 22 * 3, min_obs = 22*3), by = symbol]

# Calculate standardized forward weekly returns
dt[, ret_forward := shift(close, -1, type = "shift") / close - 1, by = symbol]
# dt[, ret_forward := shift(close, -5, type = "shift") / close - 1, by = symbol]
# dt[, ret_forward_std := ret_forward / roll_std_q, by = symbol]
# dt[, .(symbol, date, close, ret_forward_std)]
# dt[, ret_forward := shift(close, -1, type = "shift") / close - 1, by = symbol]

# Select columns we need
# dt = dt[, .(symbol, date, close, volume, close_raw, ret_forward_std, ret_forward)]
dt = dt[, .(symbol, date, close, volume, close_raw, ret_forward)]

# Remove missing values
dt = na.omit(dt)

# Free memory
gc()

# Temporary - check some symbols
# s_ = "agmh"
# dt[symbol == s_ & date %between% c(as.Date("2024-01-01"), as.Date("2024-01-05"))]
# test = open_dataset("/home/sn/lean/data/stocks_hour.csv", format = "csv") |>
#   dplyr::filter(Symbol == s_) |>
#   dplyr::filter(Date > as.Date("2024-01-01"), Date < as.Date("2024-01-05")) |>
#   collect()
# setDT(test)
# test


# IMPORT RESULTS ----------------------------------------------------------
# Import results
files = list.files(PATHSAVE, full.names = TRUE, pattern = "csv")
res = lapply(files, function(x) fread(x))
res = rbindlist(res, idcol = "id")

# Metadata on predictors
PATH     = "F:/data/equity/us/predictors_daily"
predictors_dir = list.files(PATH, full.names = TRUE)
predictors_dir = predictors_dir[!grepl("pead.*|ohlcv.*|factor", predictors_dir)]

# Best predictors
res[, .N, by = predictor][order(N)]

# Merge predictors names and folders
best = res[, .N, by = predictor][order(N, decreasing = TRUE)][, predictor][c(4:6)]
# best = head(res[, .N, by = predictor][order(N, decreasing = TRUE)][, predictor], 3)
best = data.table(
  path = fcase(
    grepl("autoarima|nnetar", best), predictors_dir[grepl("forecasts", predictors_dir)],
    grepl("TSFEL|tsfresh", best), predictors_dir[grepl("theftpy", predictors_dir)]
  ),
  predictor = best
)

# Split symbols to avoid memory issues
# problematic_symbols = c("dgii", "dgin", "dht", "dis")
symbols_seq = dt[, unique(symbol)]
# symbols_seq = dt[, setdiff(unique(symbol), problematic_symbols)]
symbols_seq = split(symbols_seq, ceiling(seq_along(symbols_seq)/200))
length(symbols_seq)

# Utils to import predictors data
get_ohlcv_by_symbols = function(path, predictors) {
  res = lapply(symbols_seq, function(s) {
    tryCatch({
      open_dataset(path, format = "parquet") %>%
        dplyr::select(any_of(c("symbol", "date", predictors))) %>%
        dplyr::filter(symbol %in% s) %>%
        # dplyr::mutate(symbol = symbol) %>%
        collect()
    }, error = function(e) NULL)
  })
  res = rbindlist(res)
  setDT(res)
  setorder(res, symbol, date)
  return(res)
}

# Import predictors
predictors = list()
paths_ = best[, unique(path)]
for (i in seq_along(paths_)) {
  print(i)
  p = paths_[i]
  predictors_= best[path == p, predictor]
  predictors[[i]] = get_ohlcv_by_symbols(p, predictors_)
}
if (length(predictors) > 1) {
  predictors = Reduce(function(x, y) merge(x, y, all = TRUE), predictors)
} else {
  predictors = predictors[[1]]
}

# Checks
symbol_ = "aapl"
predictors[symbol == symbol_]
plot(as.xts.data.table(predictors[symbol == symbol_, 2:5]))
naniar::gg_miss_var(predictors[symbol == symbol_])
predictors[symbol == symbol_][is.na(nnetar_1_252_PointForecast)]

# Merge dt and predictors
X = merge(dt, predictors, by = c("symbol", "date"))
X = na.omit(X, cols = best[, predictor])
setorder(X, symbol, date)

# Checks
symbol_ = "aapl"
dt[symbol == symbol_]
predictors[symbol == symbol_]
X[symbol == symbol_]
plot(as.xts.data.table(X[symbol == symbol_, .SD, .SDcols = c(2, 7:9)]))
X[symbol == symbol_, .SD, .SDcols = c(2, 7:9)][is.na(.SD)]

# Are there constant columns by symbol
constant_cols = X[, lapply(.SD, function(x) sd(x, na.rm = TRUE) == 0), by = symbol]
constant_cols[, lapply(.SD, sum, na.rm = TRUE), .SDcols = -"symbol"]
X[, TSFEL_0_Histogram_5_66 := NULL]

# Define cols ids
cols_ids = c("symbol", "date", "close", "close_raw", "volume", "ret_forward")

# Create predictions
cols = gsub(" ", "", paste("coefs_", c("intercept", setdiff(colnames(X), cols_ids))))
X[, (c(cols, "predictions", "dollar_volume")) := NULL]
cols = gsub(" ", "", paste("coefs_", c("intercept", setdiff(colnames(X), cols_ids))))
setorder(X, symbol, date)
X[, (cols) := as.data.table(roll_lm(
  y = ret_forward,
  x = as.matrix(.SD[, .SD, .SDcols = -c("date", "close", "ret_forward",
                                        "close_raw", "volume", "ret_forward")]),
  # x = as.matrix(.SD[, .SD, .SDcols = -c("date", "close", "ret_forward_std")]),
  width = 252,   # 504
  min_obs = 252, # 504
  na_restore = TRUE
)$coefficients), by = symbol]
X[, all(is.na(coefs_autoarima_1_252_PointForecast))]
X[, predictions := shift(coefs_intercept) +
    # shift(coefs_autoarima_1_66_PointForecast)*autoarima_1_66_PointForecast +
    # shift(coefs_autoarima_mean_66_PointForecast)*autoarima_mean_66_PointForecast +
    # # shift(coefs_TSFEL_0_LPCC_1_66)*TSFEL_0_LPCC_1_66 +
    # # shift(coef_nnetar_1_66_PointForecast)*nnetar_1_66_PointForecast +
    # # shift(coefs_nnetar_mean_66_PointForecast)*nnetar_mean_66_PointForecast +
    # shift(coefs_nnetar_1_252_PointForecast)*nnetar_1_252_PointForecast,
    shift(coefs_autoarima_1_252_PointForecast) +
    shift(coefs_nnetar_mean_252_PointForecast) +
    shift(coefs_nnetar_mean_66_PointForecast),
  by = symbol]
X[, all(is.na(predictions))]

# # Add label for most liquid asssets
# X[, dollar_volume_month := frollsum(close_raw * volume, 22, na.rm = TRUE), by = symbol]
# calculate_liquid = function(prices, n) {
#   # dt = copy(prices)
#   # n = 500
#   dt = copy(prices)
#   setorder(dt, date, -dollar_volume_month)
#   filtered = na.omit(dt)[, .(symbol = first(symbol, n)), by = date]
#   col_ = paste0("liquid_", n)
#   filtered[, (col_) := TRUE]
#   dt = filtered[dt, on = c("date", "symbol")]
#   dt[is.na(x), x := FALSE, env = list(x = col_)] # fill NA with FALSE
#   return(dt)
# }
# X = calculate_liquid(X, 100)
# X = calculate_liquid(X, 200)
# X = calculate_liquid(X, 500)
# X = calculate_liquid(X, 1000)
#
# # Remove columns we don't need
# X[, dollar_volume_month := NULL]

# plot(dt[symbol == "amzn", close])
# plot(dt[symbol == "amzn", nnetar_1_252_PointForecast])
# plot(dt[symbol == "amzn", autoarima_mean_66_PointForecast])
# plot(dt[symbol == "amzn", autoarima_1_66_PointForecast])
# naniar::gg_miss_var(dt[symbol == "spy"])

# Individual backtests
# dt_by_symbol = X[symbol == "aapl"]
dt_by_symbol = X[symbol == sample(unique(symbol), 1)]
# dt_by_symbol = X[liquid_100 == TRUE][symbol == sample(unique(symbol), 1)]
setorder(dt_by_symbol, date)
dt_by_symbol[, strategy := fifelse(predictions > 0, ret_forward, 0)]
dt_by_symbol_xts = as.xts.data.table(dt_by_symbol[, .(date, strategy, benchmark = ret_forward)])
dt_by_symbol[is.na(predictions)]
if (!(dt_by_symbol[, all(is.na(predictions))])) {
  dt_by_symbol_xts = na.omit(dt_by_symbol_xts)
  print(
    cbind(
      annualized_return = Return.annualized(dt_by_symbol_xts),
      sharpe_ratio = suppressMessages(SharpeRatio(dt_by_symbol_xts)[1, 1]),
      max_dd = maxDrawdown(dt_by_symbol_xts)
    )
  )
  charts.PerformanceSummary(dt_by_symbol_xts, main = dt_by_symbol[, first(symbol)])
} else {
  print("No predictions for this symbol")
}

# Save data
symbol = "fxi"
qc_data = na.omit(as.data.table(dt_by_symbol))
qc_data = qc_data[, .(date, predictions)]
# qc_data[, date := vapply(date, function(x) advanceDate(x, 1), FUN.VALUE = double(1))]
qc_data[, date := paste0(as.Date(date), " 16:00:00")]
qc_data[, .(date)]
storage_write_csv(qc_data, cont, "gausscov_individual.csv")

# Compare QC and R
dt_by_symbol[date %between% c(as.Date("2023-01-01"), as.Date("2023-01-17"))]

#  Buy all where prediction > 0
dts = na.omit(X)
# dts = na.omit(X[liquid_1000 == TRUE])
dts[, weight_all := 1/nrow(.SD[predictions > 0]), by = date]
dt_portfolio = dts[, .(portfolio_return = sum(weight_all * ret_forward)), by = date]
dt_xts = as.xts.data.table(dt_portfolio[, .(date, portfolio_return)])
cbind(
  annualized_return = Return.annualized(dt_xts),
  sharpe_ratio = suppressMessages(SharpeRatio(dt_xts)[1, 1]),
  max_dd = maxDrawdown(dt_xts)
)
charts.PerformanceSummary(dt_xts)
charts.PerformanceSummary(dt_xts["2020-01-01/"])

# Sort
setorder(X, date, -predictions)
dts = na.omit(X)
dts = dts[close_raw > 20]
n = as.integer(dts[, length(unique(symbol))]*0.01)
dt_sort = na.omit(X)[, head(.SD[, .(symbol, predictions, ret_forward)], n), by = date]
dt_sort = na.omit(dt_sort)
# dt_sort[, date := vapply(date, function(x) advanceDate(x, 1), FUN.VALUE = double(1))]
# dt_sort[, date := as.Date(date)]
dt_sort[, weight := 1/nrow(.SD), by = date]
setorder(dt_sort, date, symbol)
# Calcualte portfolio returns
dt_sort_portfolio = dt_sort[, .(portfolio_return = sum(weight * ret_forward)), by = date]
dt_xts = as.xts.data.table(dt_sort_portfolio[, .(date, portfolio_return)])
cbind(
  annualized_return = Return.annualized(dt_xts),
  sharpe_ratio = suppressMessages(SharpeRatio(dt_xts)[1, 1]),
  max_dd = maxDrawdown(dt_xts)
)
charts.PerformanceSummary(dt_xts)
charts.PerformanceSummary(dt_xts["2020-01-01/"])

# Save for backtesting in QC
qc_data = dt_sort[, .(
  symbol = paste(symbol, collapse = "|"),
  predictions = paste(predictions, collapse = "|"),
  weight = paste(weight, collapse = "|")
), by = date]
qc_data[, symbol := toupper(symbol)]
qc_data[, next_day := lapply(date, function(x) advanceDate(x, 1))]
qc_data[, .(date, next_day)]
qc_data[, date := vapply(date, function(x) advanceDate(x, 1), FUN.VALUE = double(1))]
qc_data[, date := as.character(as.Date(date))]
# qc_data[, date := paste0(date, " 15:59:00")]
qc_data[, date := paste0(date, " 09:31:00")]
qc_data[, .(date)]
storage_write_csv(qc_data[, .SD, .SDcols = -"next_day"], cont, "gausscov_back.csv")

# Check differences between QC and R
s_ = "agmh"
dt_sort[symbol == s_ & date %between% c(as.Date("2024-01-01"), as.Date("2024-01-05"))]

# Predict newest data
live_predictions = na.omit(dt[date == max_date], cols = "predictions")
setorder(live_predictions, -predictions)

# Save predictions for potential future use
time_ = strftime(Sys.time(), format = "%Y%m%d")
file_name = paste0("gausscov_predictions_", time_, ".csv")
fwrite(live_predictions, file.path(PATHSAVE, "predictions", file_name))

# Save the results to the Azure
qc_data = live_predictions[predictions > 0]
qc_data = qc_data[, .(symbol, predictions)]
qc_data[, symbol := toupper(symbol)]
storage_write_csv(qc_data, cont, "gausscov_live.csv")
