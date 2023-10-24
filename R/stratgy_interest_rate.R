library(data.table)
library(AzureStor)
library(httr)
library(roll)
library(ggplot2)
library(duckdb)
library(lubridate)
library(gausscov)



# SET UP ------------------------------------------------------------------
# globals
PATHOHLCV = "F:/lean_root/data/equity/usa/daily"
NASPATH       = "C:/Users/Mislav/SynologyDrive/trading_data"

# # help vars
cols_ohlc = c("open", "high", "low", "close")
cols_ohlcv = c("open", "high", "low", "close", "volume")

# creds for Azure Blob Storage
ENDPOINT = storage_endpoint(Sys.getenv("BLOB-ENDPOINT-SNP"),
                            Sys.getenv("BLOB-KEY-SNP"))
cont = storage_container(ENDPOINT, "indexes")



# DATA --------------------------------------------------------------------
# import macro data
macro_indicators = fread(file.path(NASPATH, "macro_predictors.csv"))
macro_indicators[, date := as.Date(date)]

# help function to import symbols
get_symbol = function(symbol) {
  dt = fread(cmd = paste0("unzip -p ", PATHOHLCV, "/", symbol, ".zip"),
             col.names = c("date", cols_ohlcv))
  dt[, date := as.Date(substr(date, 1, 8), "%Y%m%d")]
  dt[, (cols_ohlc) := lapply(.SD, function(x) x / 10000), .SDcols = cols_ohlc]
  dt = dt[, .(date, close)]
  setnames(dt, "close", paste0("close_", symbol))
  return(dt)
}

# import ohlcv data for bond, equity and commodities
treasury = get_symbol("shv")
bondshort = get_symbol("vgsh") # vgsh, scho, spts
bond = get_symbol("tlt")
equity = get_symbol("spy")
commodity = get_symbol("dbc") # # DBC (GSG, USCI)
tips = get_symbol("tip") # # DBC (GSG, USCI)
gold = get_symbol("gld")

# merge  all data for daily frequency
dt = Reduce(function(x, y) merge(x, y, by = "date", all = TRUE),
            list(macro_indicators, equity, bond, bondshort, treasury, gold))

# fill missing values
cols = dt[, colnames(.SD), .SDcols = is.numeric]
dt = dt[, (cols) := lapply(.SD, nafill, type = "locf"), .SDcols = cols]




# DOWNSAMPLE --------------------------------------------------------------
# downsample
dataset = copy(dt)
dataset[, week := floor_date(date, "month")]
dataset = dataset[, tail(.SD, 1), by = week]




# MODEL -------------------------------------------------------------------
# define predictors
non_pred_cols = c("date", "date_merge_dt", "week", "close", "volume",
                  "date_merge_tlt", "close_spy", "close_tlt", "close_vgsh",
                  "close_shv", "close_gld", "y_week")
predictors = setdiff(colnames(dataset), non_pred_cols)

# create target variable
dataset[, y_week := shift(GS10_m, -1, type = "shift")]

# remove INF values if exists
X_cols = c(predictors, "y_week")
any(vapply(dataset[, ..X_cols], function(x) any(is.infinite(x)), FUN.VALUE = logical(1L)))
dim(dataset)
numeric_cols <- names(dt)[sapply(dt, is.numeric | is.integer)]
dataset[, (numeric_cols) := lapply(.SD, function(x) replace(x, is.infinite(x), NA)), .SDcols = numeric_cols]
dim(dataset)

# remove NA from predictors or target
dim(dataset)
dataset = na.omit(dataset, cols = c(predictors, "y_week"))
dim(dataset)

# prepare X and y
X = dataset[, ..predictors]
X[, y_week:= NULL]
char_cols = X[, colnames(.SD), .SDcols = is.factor]
X[, (char_cols) := lapply(.SD, as.integer), .SDcols = char_cols]
X[, ..char_cols]
X = na.omit(X)
# formula <- as.formula(paste(" ~ (", paste(colnames(X), collapse = " + "), ")^2"))
# X = model.matrix(formula, X)
X = as.matrix(X)
y = as.matrix(dataset[, .(y_week)])

# insample feature importance using gausscov
f1st_res = f1st(y = y, x = X, p0=0.1)
f1st_res = f1st_res[[1]]
f1st_res_index = f1st_res[f1st_res[, 1] != 0, , drop = FALSE]
colnames(X)[f1st_res_index[, 1]]
lm(y ~ X[, colnames(X)[f1st_res_index[, 1]]])
f3st_res = f3st(y = y, x = X, m = 3, p = 0.2)
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

