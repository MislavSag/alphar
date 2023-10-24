library(httr)
library(checkmate)
library(data.table)
library(ggplot2)
library(DBI)
library(mlr3verse)
library(gausscov)
library(PerformanceAnalytics)



# FMP cloud API
FMPAPIKEY = "15cd5d0adf4bc6805a724b4417bbaafc"

# get COT symobls
url = paste0("https://financialmodelingprep.com/api/v4/commitment_of_traders_report/list?apikey=", FMPAPIKEY)
p = GET(url)
res = content(p)
cot_symbols = rbindlist(res)

# help function to downlaod data
get_cot_data = function(
    url = paste0("https://financialmodelingprep.com/api/v4/commitment_of_traders_report/ES?apikey=", FMPAPIKEY)) {
  p = GET(url)
  res = content(p)
  cot_by_symbol = rbindlist(res)
  cot_by_symbol[, date := as.Date(date)]
  return(cot_by_symbol)
}

# get COT data for every symbol
cot_by_symbol = get_cot_data()

# get COT analysis for every symbol
cota_by_symbol = get_cot_data(paste0("https://financialmodelingprep.com/api/v4/commitment_of_traders_report_analysis/ES?apikey=", FMPAPIKEY))
cota_by_symbol[, sector := NULL]

# merge data and analysis
cot = merge(cot_by_symbol, cota_by_symbol[], by = c("symbol", "date"), all.x = TRUE, all.y = FALSE)
setorder(cot, date)
cot[, day_week := weekdays(date)]

# check if all report dates are on tuesday
cot[, all(day_week == "Tuesday")]
nrow(cot[day_week == "Tuesday"])
nrow(cot[day_week != "Tuesday"])
cot[day_week != "Tuesday", .(date, day_week)]

# release date on friday
cot[day_week == "Monday", release_date := date + 4]
cot[day_week == "Tuesday", release_date := date + 3]
cot[day_week == "Wednesday", release_date := date + 2]
cot[day_week == "Friday", release_date := date]

# remove date column
setnames(cot, c("date", "release_date"), c("date_report", "date"))

# visualizations
ggplot(cot, aes(x = date, y = conc_gross_le_4_tdr_long_ol)) +
  geom_line()
ggplot(cot, aes(x = date, y = comm_positions_short_all)) +
  geom_line()
ggplot(cot, aes(x = date, y = netPostion)) +
  geom_line()

# import SPY data
con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")
query <- sprintf("SELECT * FROM read_csv_auto('%s') WHERE symbol = 'spy'", "F:/lean_root/data/all_stocks_daily.csv")
spy <- dbGetQuery(con, query)
dbDisconnect(con)
spy = as.data.table(spy)

# clean SPY data
spy = spy[, .(date = Date, close = `Adj Close`, volume = Volume)]
spy[, returns := close / shift(close) - 1]
spy = na.omit(spy)

# create target variable
spy[, y_week := shift(close, -5, type = "shift") / close - 1]

# merge spy and cot data
dataset = merge(spy, cot, by = "date", all = TRUE)

# visualize target variable
ggplot(dataset, aes(x = date, returns)) +
  geom_line()
ggplot(dataset, aes(returns)) +
  geom_histogram()

# remove missing values
dataset = na.omit(dataset)

# define predictors vector
non_predictors = c("date", "close", "volume", "y_week", "symbol", "date_report",
                   "short_name", "sector",  "market_and_exchange_names",
                   "contract_units", "name", "exchange", "day_week", "returns",
                   "as_of_date_in_form_yymmdd", "cftc_contract_market_code",
                   "cftc_market_code", "cftc_commodity_code", "cftc_region_code")
predictors = setdiff(colnames(dataset), non_predictors)

# add return predictors
ret_cols = paste0("ret_", 1:10)
dataset[, (ret_cols) := lapply(1:10, function(x) close / shift(close, x) - 1)]
predictors = c(predictors, ret_cols)

# character predictors to factors
char_predictors = dataset[, ..predictors][, colnames(.SD), .SDcols = is.character]
dataset[, (char_predictors) := lapply(.SD, as.factor), .SDcols = char_predictors]

# https://www.r-bloggers.com/2020/02/a-guide-to-encoding-categorical-features-using-r/
encode_ordinal <- function(x, order = unique(x)) {
  x <- as.numeric(factor(x, levels = order, exclude = NULL))
  x
}

# convert date to posixct because mlr3 doesnt support date
dataset[, date := as.POSIXct(date)]

# remove NA from predictors or target
dataset = na.omit(dataset, cols = c(predictors, "y_week"))

# remove INF values if exists
X_cols = c(predictors, "y_week")
any(vapply(dataset[, ..X_cols], function(x) any(is.infinite(x)), FUN.VALUE = logical(1L)))

# create task
id_cols = "date"

# prepare X and y
X = dataset[, ..predictors]
char_cols = X[, colnames(.SD), .SDcols = is.factor]
X[, (char_cols) := lapply(.SD, as.integer), .SDcols = char_cols]
X[, ..char_cols]
# predictors_diff = paste0(setdiff(predictors, char_cols), "_diff")
# X[, (predictors_diff) := lapply(.SD, function(x) x - shift(x)),
  # .SDcols = setdiff(predictors, char_cols)]
X = na.omit(X)
formula <- as.formula(paste(" ~ (", paste(colnames(X), collapse = " + "), ")^2"))
X = model.matrix(formula, X)
X = as.matrix(X)
y = as.matrix(dataset[, .(y_week)])

# insample feature importance using gausscov
f1st_res = f1st(y = y, x = X, p0=0.001)
f1st_res = f1st_res[[1]]
f1st_res_index = f1st_res[f1st_res[, 1] != 0, , drop = FALSE]
colnames(X)[f1st_res_index[, 1]]
f3st_res = f3st(y = y, x = X, m = 3, p = 0.001)
res_index <- unique(as.integer(f3st_res[[1]][1, ]))[-1]
res_index  <- res_index [res_index  != 0]
colnames(X)[res_index]

# visualize most important variables
important_vars = c("change_in_noncomm_spead_all:ret_6",
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

# backtest simple strategy
Backtest <- function(returns, indicator, threshold, return_cumulative = TRUE) {
  sides <- vector("integer", length(indicator))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(indicator[i-1])) {
      sides[i] <- NA
    } else if (indicator[i-1] > threshold) {
      sides[i] <- 0
    } else {
      sides[i] <- 1
    }
  }
  sides <- ifelse(is.na(sides), 1, sides)
  returns_strategy <- returns * sides
  if (return_cumulative) {
    return(PerformanceAnalytics::Return.cumulative(returns_strategy))
  } else {
    return(returns_strategy)
  }
}
backtest_data = copy(X_imp)
backtest_data[, returns := close / shift(close, 1) - 1]
backtest_data = na.omit(backtest_data[, .(date, benchmark = returns, strategy = signal2 * returns)])
charts.PerformanceSummary(as.xts.data.table(backtest_data))

# task with future week returns as target
target_ = "y_week"
cols_ = c(id_cols, target_, predictors)
task_ret_week <- as_task_regr(dataset[, ..cols_],
                              id = "task_week",
                              target = target_)

