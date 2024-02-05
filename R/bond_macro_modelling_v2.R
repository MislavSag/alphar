library(data.table)
library(AzureStor)
library(httr)
library(roll)
library(ggplot2)
library(duckdb)
library(lubridate)
library(gausscov)
library(fs)
library(future.apply)
library(forecast)
library(matrixStats)



# DATA --------------------------------------------------------------------
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
  return(data_)
}
spy_dt = get_sec("spy")
tlt_dt = get_sec("tlt")

######## THIS TO QUANTDATA ###########
# # import FRED meta
# fred_meta = fread("F:/macro/fred_series_meta.csv")
# fred_meta = unique(fred_meta, by = "id")
#
# # import FRED series data
# fred_files = dir_ls("F:/macro/fred")
# # plan("multisession", workers = 6)
# fred_l = lapply(fred_files, function(x) tryCatch(fread(x), error = function(e) NULL))
# length(fred_l)
# fred_l = fred_l[!sapply(fred_l, is.null)]
# fred_l = fred_l[!sapply(fred_l, function(x) nrow(x) == 0)]
# fred_l = future_lapply(fred_l, function(dt_) dt_[, value := as.numeric(value)])
#
# # solve date problem
# fred_l = future_lapply(fred_l, function(dt) {
#   if (dt[, all(vintage == 1)]) {
#     dt[, date_real := realtime_start]
#   } else {
#     dt[, date_real := date]
#   }
# })
#
# # merge all series
# fred_dt = rbindlist(fred_l, fill = TRUE)
# setorder(fred_dt, series_id, date)
#
# # merge meta data
# fred_dt = fred_meta[, .(series_id = id, frequency_short, popularity, observation_start, units, id_category)][
#   fred_dt, on = "series_id"]
#
# # save fred data
# fwrite(fred_dt, "F:/macro/fred.csv")
######## THIS TO QUANTDATA ###########

# import fred series
fred_dt = fread("F:/macro/fred.csv")

# remove unnecessary columns
fred_dt = fred_dt[, .(series_id, date = date_real, frequency_short, popularity,
                      units, id_category, value)]

# filter daily and weekly data
fred_dt_w = fred_dt[frequency_short %in% c("D", "W", "BW", "M")]

# diff if necessary
fred_dt_w = fred_dt_w[, ndif := ndiffs(value), by = series_id]
fred_dt_w[, unique(ndif), by = series_id][, .N, by = V1]
fred_dt_w[ndif > 0, value_diff := c(rep(NA, unique(ndif)), diff(value, unique(ndif))), by = series_id]

# check for duplicates
date_series = fred_dt_w[, .N, by = .(date, series_id)]
fred_dt_w = unique(fred_dt_w,
                   fromLast = TRUE,
                   by = c("series_id", "date"))

# reshape - variables to columns
fred_dt_w = dcast(fred_dt_w[, .(date, series_id, value_diff)],
                  date ~ series_id,
                  value.var = "value_diff")
setorder(fred_dt_w, date)
fred_dt_w[1:10, 1:10]

# locf vars
predictors = colnames(fred_dt_w)[2:ncol(fred_dt_w)]
fred_dt_w[, (predictors) := lapply(.SD, nafill, type = "locf"), .SDcols = predictors]

# downsample TLT to weekly
tlt = copy(tlt_dt)
tlt[, week := floor_date(date, "month")]
tlt[, close := tail(close, 1), by = week]
tlt = tlt[, tail(.SD, 1), by = week]

# add target variable
DT = fred_dt_w[tlt[, .(date, close)], on = "date", roll = Inf]
DT = DT[date > as.Date("2002-01-01")]
cols = c("date", "close", predictors)
DT = DT[, ..cols]
DT[, `:=`(
  ret_1 = shift(close, n = -1, type = "shift") / close -1,
  ret_2 = shift(close, n = -2, type = "shift") / close -1,
  ret_3 = shift(close, n = -3, type = "shift") / close -1,
  ret_4 = shift(close, n = -4, type = "shift") / close -1,
  ret_12 = shift(close, n = -12, type = "shift") / close -1
)]
# DT[, `:=`(
#   ret_d = shift(close, n = -1, type = "shift") / close -1,
#   ret_w = shift(close, n = -5, type = "shift") / close -1,
#   ret_bw = shift(close, n = -10, type = "shift") / close -1,
#   ret_m = shift(close, n = -22, type = "shift") / close -1,
#   ret_q = shift(close, n = -66, type = "shift") / close -1
# )]

# lag all variables - IMPORTANT !
DT[, (predictors) := lapply(.SD, shift), .SDcols = predictors]


# MODEL -------------------------------------------------------------------
# define predictors
non_pred_cols = c("date", "date_merge_dt", "week", "close", "volume",
                  "date_merge_tlt", "ret_1", "ret_2", "ret_3", "ret_4", "ret_12")
predictors = setdiff(colnames(DT), non_pred_cols)

DT = na.omit(DT, cols = "ret_1")

X = DT[, ..predictors]
y = DT[, ret_1]
dim(X)
length(y)

# remove columns with many NA values
nas_cols_train = colSums(is.na(X))
nas_cols_train = nas_cols_train / nrow(X)
non_na = names(nas_cols_train)[nas_cols_train < 0.1]
X = X[, ..non_na]
dim(X)
X = as.matrix(X)

isna_ind = rowAnyNAs(X)
X = X[!isna_ind, ]
y = y[!isna_ind]
dim(X)
length(y)

rowany

# # remove INF values if exists
# X_cols = c(predictors, "y_week")
# any(vapply(dt_model[, ..X_cols], function(x) any(is.infinite(x)), FUN.VALUE = logical(1L)))

# prepare X and y
# char_cols = X[, colnames(.SD), .SDcols = is.factor]
# X[, (char_cols) := lapply(.SD, as.integer), .SDcols = char_cols]
# X[, ..char_cols]
# X = na.omit(X)
# formula <- as.formula(paste(" ~ (", paste(colnames(X), collapse = " + "), ")^2"))
# X = model.matrix(formula, X)
# X = as.matrix(X)

# insample feature importance using gausscov
f1st_res = f1st(y = y, x = X, p0=0.01)
f1st_res = f1st_res[[1]]
f1st_res_index = f1st_res[f1st_res[, 1] != 0, , drop = FALSE]
colnames(X)[f1st_res_index[, 1]]
lm(y ~ X[, colnames(X)[f1st_res_index[, 1]]])
f3st_res = f3st(y = y, x = X, m = 3, p = 0.1)
res_index <- unique(as.integer(f3st_res[[1]][1, ]))[-1]
res_index  <- res_index [res_index  != 0]
colnames(X)[res_index]

# visualize most important variables
important_vars = c("GS10",
                   "change_in_nonrept_long_all:ret_3")
X_imp = cbind(date = dataset[, date],
              close = dataset[, close],
              X[, important_vars], y)
X_imp = as.data.table(X_imp)
X_imp[, date := as.POSIXct(date)]
X_imp[, signal := ifelse(shift(`change_in_noncomm_spead_all:ret_6`) < 300, 0, 1)]
X_imp[, signal2 := ifelse(shift(`change_in_nonrept_long_all:ret_3`) < 100, 0, 1)]
ggplot(X_imp, aes(x = date)) +
  geom_line(aes(y = `change_in_noncomm_spead_all:ret_6`))
ggplot(X_imp, aes(x = date)) +
  geom_line(aes(y = `change_in_nonrept_long_all:ret_3`))
ggplot(X_imp[1:200], aes(x = date)) +
  geom_line(aes(y = `change_in_noncomm_spead_all:ret_6`))
ggplot(X_imp[800:1000], aes(x = date)) +
  geom_line(aes(y = `change_in_noncomm_spead_all:ret_6`))
ggplot(X_imp[1000:nrow(X_imp)], aes(x = date)) +
  geom_line(aes(y = `change_in_noncomm_spead_all:ret_6`))
ggplot(na.omit(X_imp), aes(x = date, color = as.factor(signal))) +
  geom_line(aes(y = close))
ggplot(na.omit(X_imp[1000:nrow(X_imp)]), aes(x = date, y = close, color = signal)) +
  geom_line(size = 1)
ggplot(na.omit(X_imp[800:1000]), aes(x = date, y = close, color = signal)) +
  geom_line(size = 1)
ggplot(na.omit(X_imp[1000:nrow(X_imp)]), aes(x = date, y = close, color = signal2)) +
  geom_line(size = 1)
ggplot(na.omit(X_imp[800:1000]), aes(x = date, y = close, color = signal2)) +
  geom_line(size = 1)
