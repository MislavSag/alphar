library(data.table)
library(fs)
library(roll)
library(forecast)
library(QuantTools)
library(fs)
library(duckdb)
library(PerformanceAnalytics)
library(gausscov)
library(AzureStor)
library(readr)
library(runner)
library(future.apply)
# anomaly detection
# library(AnomalyDetection)


# UTILS -------------------------------------------------------------------
# rolling zscore function
roll_zscore <- function(x, ...) {
  args <- list(...)

  # either use provided `fill` or `NA`
  args$fill <- if (is.null(args$fill)) NA else args$fill
  args <- append(args, list(x = x))

  avg <- do.call(RcppRoll::roll_mean, args)
  std <- do.call(RcppRoll::roll_sd, args)

  (x - avg) / std
}

# creds
blob_key = "0M4WRlV0/1b6b3ZpFKJvevg4xbC/gaNBcdtVZW+zOZcRi0ZLfOm1v/j2FZ4v+o8lycJLu1wVE6HT+ASt0DdAPQ=="
endpoint = "https://snpmarketdata.blob.core.windows.net/"
BLOBENDPOINT = storage_endpoint(endpoint, key=blob_key)


# HELP DATA ---------------------------------------------------------------
# get securities data from QC
get_sec = function(symbol) {
  con <- dbConnect(duckdb::duckdb())
  query <- sprintf("
    SELECT *
    FROM 'F:/lean_root/data/all_stocks_daily.csv'
    WHERE Symbol = '%s'
", symbol)
  data_ <- dbGetQuery(con, query)
  dbDisconnect(con)
  data_ = as.data.table(data_)
  data_ = data_[, .(date = Date, close = `Adj Close`)]
  data_[, returns := close / shift(close) - 1]
  data_ = na.omit(data_)
  dbDisconnect(con)
  return(data_)
}
spy = get_sec("spy")
tlt = get_sec("tlt")


# FRED --------------------------------------------------------------------
# import FRED meta
fred_meta = fread("F:/macro/fred_series_meta.csv")
fred_meta = unique(fred_meta, by = c("id"))
fred_meta[, .N, by = id][N > 1] # no duplicated values

# import FRED series data
fred_files = dir_ls("F:/macro/fred")
plan("multisession", workers = 4)
fred_l = future_lapply(fred_files, fread)
length(fred_l)
fred_l = future_lapply(fred_l, function(dt_) dt_[, value := as.numeric(value)])
fred_l = fred_l[!sapply(fred_l, is.null)]
fred_l = fred_l[!sapply(fred_l, function(x) nrow(x) == 0)]

# show dates problem with gpd
# gsp_test = future_vapply(fred_l, function(x) {
#   x[, any(series_id == "GDP")]
# }, FUN.VALUE = logical(1L))
# gsp_test = fred_l[[which(gsp_test)]]
# gsp_test

# solve date problem
fred_l = future_lapply(fred_l, function(dt) {
  if (dt[, all(vintage == 1)]) {
    dt[, date_real := realtime_start]
  } else {
    dt[, date_real := date]
  }
})

# merge all series
fred_dt = rbindlist(fred_l, fill = TRUE)
setorder(fred_dt, series_id, date)

# merge meta data
fred_dt = fred_meta[, .(series_id = id, frequency_short, popularity, observation_start, units, id_category)][
  fred_dt, on = "series_id"]
# 44.649.740

# remove unnecessary columns
fred_dt = fred_dt[, .(series_id, date = date_real, frequency_short, popularity,
                      units, id_category, value)]

# test
fred_dt[series_id == "GDP"]

# save fred so we dont need to wait another time!
# TODO MOVE THIS PART TO QUANTDATA

# value integer to numeric
# fred_dt[, value = as.nu]

#
# stl()

####################### TRY PERCENTE ###################
# # percents
# fred_dt[, unique(units)]
# fred_dt_pct = fred_dt[grepl("percent", units, ignore.case = TRUE)]
# nrow(fred_dt_pct) / nrow(fred_dt)
# fred_dt_pct[, percent := value / 100]
# plot(as.xts.data.table(fred_dt_pct[series_id == fred_dt_pct[, sample(series_id, 1)], .(date, percent)]))
#
# # calculate expanding rolling sd
# fred_dt_pct[, unique(frequency_short)]
# fred_dt_pct[frequency_short == "D", zscore_roll := roll_zscore(percent, n = 252*5, na.rm = TRUE, partial = TRUE, align = "right"), by = series_id]
# fred_dt_pct[frequency_short == "W", zscore_roll := roll_zscore(percent, n = 4*12*5, na.rm = TRUE, partial = TRUE, align = "right"), by = series_id]
# fred_dt_pct[frequency_short == "BW", zscore_roll := roll_zscore(percent, n = 2*12*5, na.rm = TRUE, partial = TRUE, align = "right"), by = series_id]
# fred_dt_pct[frequency_short == "M", zscore_roll := roll_zscore(percent, n = 12*5, na.rm = TRUE, partial = TRUE, align = "right"), by = series_id]
# fred_dt_pct[frequency_short == "Q", zscore_roll := roll_zscore(percent, n = 4*5, na.rm = TRUE, partial = TRUE, align = "right"), by = series_id]
#
# # extreme values
# fred_dt_pct_ext = na.omit(fred_dt_pct, cols = "zscore_roll")
# fred_dt_pct_ext = fred_dt_pct_ext[abs(zscore_roll) > 3 & date > as.Date(Sys.Date()-132)]
# paste0("https://fred.stlouisfed.org/series/", fred_dt_pct_ext[, unique(series_id)])
####################### TRY PERCENTE ###################

####################### TRY AnomalyDetection ###################
# # remove na for values
# fred_dt = na.omit(fred_dt, cols = "value")
#
# # remove series with low observations
# n_m = fred_dt[frequency_short == "M", .N, by = "series_id"]
# series_remove = n_m[N < 25, series_id]
# fred_dt = fred_dt[!(series_id %in% series_remove)]
#
# # use AnomalyDetection
# anoms_m = fred_dt[frequency_short == "M"][
#   , AnomalyDetectionVec(as.data.frame(.SD[, .(value)]), max_anoms=0.02, direction='both', period = 12, only_last = TRUE)$anoms,
#   by = series_id]
# anoms_m[, unique(series_id)]
####################### TRY AnomalyDetection ###################

####################### SMA ###################
# # indecies
# fred_dt[, unique(units)]
# fred_dt_index = fred_dt[grepl("index", units, ignore.case = TRUE)]
# fred_dt_index = na.omit(fred_dt_index, cols = "value")
# fred_dt_index[frequency_short == "D", sma := sma(value, n = 252), by = series_id]
# fred_dt_index[frequency_short == "W", sma := sma(value, n = 4*12), by = series_id]
# fred_dt_index[frequency_short == "BW", sma := sma(value, n = 2*12), by = series_id]
# fred_dt_index[frequency_short == "M", sma := sma(value, n = 12), by = series_id]
# fred_dt_index[frequency_short == "Q", sma := sma(value, n = 4), by = series_id]
# fred_dt_index[frequency_short == "A", sma := sma(value, n = 4), by = series_id]
#
# fred_dt_index[abs(value - sma) > 1, by = series_id]
####################### SMA ###################

# diff if necessary
fred_dt = fred_dt[, ndif := ndiffs(value), by = series_id]
fred_dt[, unique(ndif), by = series_id][, .N, by = V1]
fred_dt[ndif > 0, value_diff := c(rep(NA, unique(ndif)), diff(value, unique(ndif))), by = series_id]

# calculate expanding rolling sd
fred_dt[, unique(frequency_short)]
fred_dt[frequency_short == "D", zscore_roll := roll_zscore(value_diff, n = 252*5, na.rm = TRUE, partial = TRUE, align = "right"), by = series_id]
fred_dt[frequency_short == "W", zscore_roll := roll_zscore(value_diff, n = 4*12*5, na.rm = TRUE, partial = TRUE, align = "right"), by = series_id]
fred_dt[frequency_short == "BW", zscore_roll := roll_zscore(value_diff, n = 2*12*5, na.rm = TRUE, partial = TRUE, align = "right"), by = series_id]
fred_dt[frequency_short == "M", zscore_roll := roll_zscore(value_diff, n = 12*5, na.rm = TRUE, partial = TRUE, align = "right"), by = series_id]
fred_dt[frequency_short == "Q", zscore_roll := roll_zscore(value_diff, n = 4*5, na.rm = TRUE, partial = TRUE, align = "right"), by = series_id]

# extreme values
fred_dt[, zscore_3 := as.integer(abs(zscore_roll) > 3)]
fred_dt[, zscore_4 := as.integer(abs(zscore_roll) > 4)]
fred_dt[, zscore_5 := as.integer(abs(zscore_roll) > 5)]

# inspect
fred_dt[popularity > 80 & zscore_4 == TRUE]
fred_dt[popularity > 50 & zscore_4 == TRUE & date > as.Date("2023-08-01")]
plot(as.xts.data.table(fred_dt[series_id == "DTBSPCKNONWW", .(date, value)]))
paste0("https://fred.stlouisfed.org/series/", fred_dt_extreme[, unique(series_id)])
plot(as.xts.data.table(fred_dt_extreme[, .N, by = date][order(date)]))
plot(as.xts.data.table(fred_dt_extreme[, .N, by = date][order(date)][date > as.Date("2000-01-01")]))
plot(as.xts.data.table(fred_dt_extreme[, .N, by = date][order(date)][date > as.Date("2020-01-01")]))
plot(as.xts.data.table(fred_dt_extreme[, .N, by = date][order(date)][date > as.Date("2021-01-01")]))
plot(as.xts.data.table(fred_dt_extreme[, .N, by = date][order(date)][date > as.Date("2021-01-01")]))


# PREPARE DATA FOR MODELING -----------------------------------------------
# optional - keep only series with break
# fred_dt[, length(unique(series_id))]
# series_break = fred_dt[, .(any_break = any(zscore_3 == 1, na.rm = TRUE)), by = series_id]
# nrow(series_break[any_break == TRUE]) / nrow(series_break) * 100
# fred_dt_sample = fred_dt[series_id %in% series_break[any_break == TRUE, series_id]]
# nrow(fred_dt) # 6.931.388
# nrow(fred_dt_sample) # 4.075.000
# nrow(fred_dt_sample) / nrow(fred_dt) * 100
# fred_dt_sample = na.omit(fred_dt_sample)

# alternative to above - keep all series
fred_dt_sample = fred_dt[popularity > 30]

# check for duplicates
date_series = fred_dt_sample[, .N, by = .(date, series_id)]
fred_dt_sample = unique(fred_dt_sample,
                        fromLast = TRUE,
                        by = c("series_id", "date"))
fred_dt_sample[series_id == "GDP"]

# reshape - variables to columns
fred_dt_sample = dcast(fred_dt_sample[, .(date, series_id, zscore_3)],
                       date ~ series_id,
                       value.var = "zscore_3")
setorder(fred_dt_sample, date)
fred_dt_sample[1:10, 1:10]

# locf vars
predictors = colnames(fred_dt_sample)[2:ncol(fred_dt_sample)]
fred_dt_sample[, (predictors) := lapply(.SD, nafill, type = "locf"), .SDcols = predictors]

# lag all variables - IMPORTANT !
fred_dt_sample[, (predictors) := lapply(.SD, shift), .SDcols = predictors]
# fred_dt_sample = na.omit(fred_dt_sample)

# add target variable
DT = fred_dt_sample[spy[, .(date, close)], on = "date"]
DT = DT[date > as.Date("2000-01-01")]
cols = c("date", "close", predictors)
DT = DT[, ..cols]
DT[, `:=`(
  ret_d = shift(close, n = -1, type = "shift") / close -1,
  ret_w = shift(close, n = -5, type = "shift") / close -1,
  ret_bw = shift(close, n = -10, type = "shift") / close -1,
  ret_m = shift(close, n = -22, type = "shift") / close -1,
  ret_q = shift(close, n = -66, type = "shift") / close -1
)]

# agggregate extremes
total_extremes = rowSums(DT[, 3:(ncol(DT)-5)], na.rm = TRUE)
total_non_nas = rowSums(!is.na(DT[, 3:(ncol(DT)-5)]))
total_extremes_sd = apply(DT[, 3:(ncol(DT)-5)], MARGIN = 1, FUN = function(x) sd(x, na.rm = TRUE))
total_extremes_dt = as.data.table(
  cbind.data.frame(
    date = DT$date,
    ext = total_extremes,
    ext_ratio = total_extremes / total_non_nas,
    ext_sd = total_extremes_sd
  )
)
plot(as.xts.data.table(total_extremes_dt[, .(date, ext)]))
plot(as.xts.data.table(total_extremes_dt[, .(date, ext_ratio)]), main = "Ext Ratio")
plot(as.xts.data.table(total_extremes_dt[, .(date, ext_ratio - shift(ext_ratio))]), main = "Ext Ratio Diff")
plot(as.xts.data.table(total_extremes_dt[, .(date, ext_sd)]), main = "Ext Sd")
plot(as.xts.data.table(total_extremes_dt[, .(date, ext_sd - shift(ext_sd))]), main = "Ext Sd Diff")

# library(kcpRS)

# simple fast backtest
backtest_data = as.data.table(cbind.data.frame(total_extremes_dt, close = DT$close))
backtest_data[, returns := close / shift(close, 1) - 1]
backtest_data = na.omit(backtest_data)
backtest_data[, ind := ext_ratio - shift(ext_ratio)]
backtest_data = na.omit(backtest_data)
backtest_data[, ind := ema(ind, 10)]
# plot(as.xts.data.table(backtest_data[, .(date, ind)]))
backtest_data[, sign := 1]
backtest_data[shift(ind) > 0, sign := 0]
backtest_data[is.na(sign), sign := 1]
backtest_data[, strategy := returns * shift(sign)]
backtest_data = na.omit(backtest_data)
backtest_xts = as.xts.data.table(backtest_data[, .(date, strategy, benchmark = returns)])
charts.PerformanceSummary(backtest_xts)

# save data to QC to backtest there
qc_data = backtest_data[, .(date, ind)]
qc_data[, date := as.character(date)]
cont = storage_container(BLOBENDPOINT, "qc-backtest")
storage_write_csv(qc_data, cont, "fredagg.csv", col_names = FALSE)


# MODEL AGGREGATE ---------------------------------------------------------
# prepare data for modeling
model_data = as.data.table(cbind.data.frame(total_extremes_dt, c = DT$close))
model_data = model_data[, .(date, ext_ratio, returns = c / shift(c) - 1)]
model_data = na.omit(model_data)
ndiffs(model_data[, ext_ratio])
model_data[, ext_ratio := c(NA, diff(ext_ratio))]
model_data = na.omit(model_data)

# rolling var
library(vars)
var_roll = runner(
  x = model_data,
  f = function(dt_) {
    VAR(as.xts.data.table(dt_), lag.max = 10)
  },
  at = 1000:nrow(model_data),
  na_pad = TRUE,
  simplify = FALSE
)

# extract predictions
predictions_l = lapply(var_roll, function(x) {
  # x = var_roll[[1001]]
  y = predict(x)
  y$fcst$returns[1]
})
predictions = unlist(predictions_l)
predictions = cbind.data.frame(model_data[1000:nrow(model_data)], resp = predictions)
predictions = setDT(predictions)
predictions[, resp := shift(resp)]
predictions = na.omit(predictions)
predictions[, sum(sign(returns) == sign(resp)) / length(returns)]

# backtest
backtest_data = predictions[, .(date, resp, returns)]
backtest_data[, sign := ifelse(resp < -0.0001, 0, 1)]
backtest_data[is.na(sign), sign := 1]
backtest_data[, strategy := returns * shift(sign)]
backtest_data = na.omit(backtest_data)
backtest_xts = as.xts.data.table(backtest_data[, .(date, strategy, benchmark = returns)])
charts.PerformanceSummary(backtest_xts)


# IMPORTANT PREDICTORS ----------------------------------------------------
# prepare dataset
dt = fred_dt[popularity > 50]
date_series = dt[, .N, by = .(date, series_id)]
dt = unique(dt,
            fromLast = TRUE,
            by = c("series_id", "date"))
dt = dcast(dt[, .(date, series_id, value_diff)],
           date ~ series_id,
           value.var = "value_diff")
setorder(dt, date)
predictors = colnames(dt)[2:ncol(dt)]
dt[, (predictors) := lapply(.SD, nafill, type = "locf"), .SDcols = predictors]
dt = dt[spy[, .(date, close)], on = "date"]
dt[, `:=`(
  ret_d = shift(close, n = -1, type = "shift") / close -1,
  ret_w = shift(close, n = -5, type = "shift") / close -1,
  ret_bw = shift(close, n = -10, type = "shift") / close -1,
  ret_m = shift(close, n = -22, type = "shift") / close -1,
  ret_q = shift(close, n = -66, type = "shift") / close -1
)]

# prepare data for prediciton
train_X = dt[date %between% c("2000-01-01", "2023-01-01"), ..predictors]
train_y = dt[date %between% c("2000-01-01", "2023-01-01"), ret_w]
test_X  = dt[date %between% c("2022-01-01", "2023-10-01"), ..predictors]
test_y  = dt[date %between% c("2022-01-01", "2023-10-01"), ret_w]

# remove columns with many NA values
nas_cols_train = colSums(is.na(train_X))
nas_cols_train = nas_cols_train / nrow(train_X)
non_na = names(nas_cols_train)[nas_cols_train < 0.2]
train_X = train_X[, ..non_na]
test_X = test_X[, ..non_na]
dim(train_X)
dim(test_X)
train_X[, 1:5]
train_X = na.omit(train_X)
test_X = na.omit(test_X)
dim(train_X)
dim(test_X)

# featues importance
f1st_res = f1st(y = train_y, x = as.matrix(train_X))
res_1 = f1st_res[[1]]
res_1 = res_1[res_1[, 1] != 0, , drop = FALSE]
f1st_res_imp = colnames(train_X)[res_1[, 1]]
f1st_res_imp
# f3st_res = f3st(y = resp, x = bin, m = 2)
# res_index <- unique(as.integer(f3st_res[[1]][1, ]))[-1]
# res_index  <- res_index [res_index != 0]
# colnames(bin)[res_index]

# rolling VAR

