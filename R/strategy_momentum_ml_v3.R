library(data.table)
library(tiledb)
library(rvest)
library(lubridate)
library(finfeatures)
library(mlr3)
library(mlr3pipelines)
library(mlr3viz)
library(mlr3tuning)
library(mlr3mbo)
library(mlr3misc)
library(mlr3hyperband)
library(mlr3extralearners)
library(bbotk)
library(future.apply)
library(QuantTools)
library(PerformanceAnalytics)



# SET UP ------------------------------------------------------------------
# globals
DATAPATH     = "F:/lean_root/data/all_stocks_daily.csv"
DATAPATHHOUR = "F:/lean_root/data/all_stocks_hour.csv"
# URIEXUBER    = "F:/equity-usa-hour-exuber"
NASPATH      = "C:/Users/Mislav/SynologyDrive/trading_data"


# UTILS -------------------------------------------------------------------
# utils https://stackoverflow.com/questions/1995933/number-of-months-between-two-dates
monnb <- function(d) {
  lt <- as.POSIXlt(as.Date(d, origin="1900-01-01"))
  lt$year*12 + lt$mon }
mondf <- function(d1, d2) { monnb(d2) - monnb(d1) }


# UNIVERSE ----------------------------------------------------------------
# SPY constitues
spy_const = fread(file.path(NASPATH, "spy.csv"))
symbols_spy = unique(spy_const)

# SP 500
sp500_changes = read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies") %>%
  html_elements("table") %>%
  .[[2]] %>%
  html_table()
sp500_changes = sp500_changes[-1, c(1, 2, 4)]
sp500_changes = as.data.table(sp500_changes)
sp500_changes[, Date := as.Date(Date, format = "%b %d, %Y")]
sp500_changes = sp500_changes[Date < as.Date("2009-06-28")]
sp500_changes_symbols = unlist(sp500_changes[, 2:3], use.names = FALSE)
sp500_changes_symbols = unique(sp500_changes_symbols)
sp500_changes_symbols = sp500_changes_symbols[sp500_changes_symbols != ""]

# SPY constitues + SP 500
symbols_sp500 = unique(c(symbols_spy, sp500_changes_symbols))
symbols_sp500 = tolower(symbols_sp500)
symbols_sp500 = c("spy", symbols_sp500)



# IMPORT DATA -------------------------------------------------------------
# import QC daily data
col = c("date", "open", "high", "low", "close", "volume", "close_adj", "symbol")
dt = fread(DATAPATH, col.names = col)
dt <- unique(dt, by = c("symbol", "date"))
unadjustd_cols = c("open", "high", "low")
adjusted_cols = paste0(unadjustd_cols, "_adj")
dt[, (adjusted_cols) := lapply(.SD, function(x) (close_adj / close) * x), .SDcols = unadjustd_cols]
dt = na.omit(dt)
setorder(dt, symbol, date)

# import QC hourly data
# col = c("date", "open", "high", "low", "close", "volume", "close_adj", "symbol")
# dth = fread(DATAPATHHOUR, col.names = col)
# dth <- unique(dth, by = c("symbol", "date"))
# unadjustd_cols = c("open", "high", "low")
# adjusted_cols = paste0(unadjustd_cols, "_adj")
# dth[, (adjusted_cols) := lapply(.SD, function(x) (close_adj / close) * x), .SDcols = unadjustd_cols]
# dth = na.omit(dth)
# setorder(dth, symbol, date)

# free resources
gc()


# PREDICTORS --------------------------------------------------------------
# create help colummns
dt[, year_month_id := ceiling_date(date, unit = "month") - days(1)]

# remove negative prices
dt = dt[close > 0 & close_adj > 0]

# add variables
dt[, dollar_volume := volume * close]

# PRA window sizes
windows_ = c(5, 22, 22 * 3, 22 * 6, 252, 252 * 2, 252 * 4)

# calculate PRA predictors
pra_predictors = paste0("pra_", windows_)
dt[, (pra_predictors) := lapply(windows_,
                                function(w) roll_percent_rank(close_adj, w)),
   by = symbol]

# calculate momentum indicators
setorder(dt, symbol, date)
moms = seq(21, 21 * 12, 21)
mom_cols = paste0("mom", 1:12)
dt[, (mom_cols) := lapply(moms, function(x) close_adj  / shift(close_adj , x) - 1), by = symbol]
moms = seq(21 * 2, 21 * 12, 21)
mom_cols_lag = paste0("mom_lag", 2:12)
dt[, (mom_cols_lag) := lapply(moms, function(x) shift(close_adj, 21) / shift(close_adj , x) - 1), by = symbol]

# create target var
moms_target = c(5, 10, 21, 42, 63)
moms_target_cols = paste0("mom_target_", c("w", "2w", "m", "2m", "3m"))
dt[, (moms_target_cols) := lapply(moms_target, function(x) (shift(close_adj , -x, "shift") / close_adj ) - 1),
   by = symbol]

# order data
setorder(dt, symbol, date)

# downsample
dtm = dt[, .SD[.N], by = c("symbol", "year_month_id")]
dtm_2 = dt[, .(
  dollar_volume_sum = sum(dollar_volume, na.rm = TRUE),
  dollar_volume_mean = mean(dollar_volume, na.rm = TRUE),
  volume_sum = sum(volume, na.rm = TRUE),
  volume_mean = mean(volume, na.rm = TRUE)
), by = c("symbol", "year_month_id")]
dtm = dtm_2[dtm, on = c("symbol", "year_month_id")]
dtm[symbol == "aapl"]

# plots
plot(dtm[symbol == "aapl", .(date, dollar_volume)],
     type = "l", main = "Volume and dolalr volme")
plot(dtm[symbol == "aapl", .(date, close_adj)],
     type = "l", main = "Volume and dolalr volme")



# PRA INDICATORS ----------------------------------------------------------
# pra indicators
cols = c("symbol", "date", pra_predictors)
pra = dt[, ..cols]

# crate dummy variables
cols <- paste0("pra_", windows_)
cols_above_999 <- paste0("pr_above_dummy_", windows_)
pra[, (cols_above_999) := lapply(.SD, function(x) ifelse(x > 0.999, 1, 0)), .SDcols = cols]
cols_below_001 <- paste0("pr_below_dummy_", windows_)
pra[, (cols_below_001) := lapply(.SD, function(x) ifelse(x < 0.001, 1, 0)), .SDcols = cols]
cols_net_1 <- paste0("pr_below_dummy_net_", windows_)
pra[, (cols_net_1) := pra[, ..cols_above_999] - pra[, ..cols_below_001]]

cols_above_99 <- paste0("pr_above_dummy_99_", windows_)
pra[, (cols_above_99) := lapply(.SD, function(x) ifelse(x > 0.99, 1, 0)), .SDcols = cols]
cols_below_01 <- paste0("pr_below_dummy_01_", windows_)
pra[, (cols_below_01) := lapply(.SD, function(x) ifelse(x < 0.01, 1, 0)), .SDcols = cols]
cols_net_2 <- paste0("pr_below_dummy_net_0199", windows_)
pra[, (cols_net_2) := pra[, ..cols_above_99] - pra[, ..cols_below_01]]

cols_above_97 <- paste0("pr_above_dummy_97_", windows_)
pra[, (cols_above_97) := lapply(.SD, function(x) ifelse(x > 0.97, 1, 0)), .SDcols = cols]
cols_below_03 <- paste0("pr_below_dummy_03_", windows_)
pra[, (cols_below_03) := lapply(.SD, function(x) ifelse(x < 0.03, 1, 0)), .SDcols = cols]
cols_net_3 <- paste0("pr_below_dummy_net_0397", windows_)
pra[, (cols_net_3) := pra[, ..cols_above_97] - pra[, ..cols_below_03]]

cols_above_95 <- paste0("pr_above_dummy_95_", windows_)
pra[, (cols_above_95) := lapply(.SD, function(x) ifelse(x > 0.95, 1, 0)), .SDcols = cols]
cols_below_05 <- paste0("pr_below_dummy_05_", windows_)
pra[, (cols_below_05) := lapply(.SD, function(x) ifelse(x < 0.05, 1, 0)), .SDcols = cols]
cols_net_4 <- paste0("pr_below_dummy_net_0595", windows_)
pra[, (cols_net_4) := pra[, ..cols_above_95] - pra[, ..cols_below_05]]

# PRA indicators
indicators <- pra[symbol != "SPY", lapply(.SD, sum, na.rm = TRUE),
                  .SDcols = c(colnames(pra)[grep("pr_\\d+", colnames(pra))],
                              cols_above_999, cols_above_99, cols_below_001, cols_below_01,
                              cols_above_97, cols_below_03, cols_above_95, cols_below_05,
                              cols_net_1, cols_net_2, cols_net_3, cols_net_4),
                  by = .(date)]
indicators <- unique(indicators, by = c("date"))
setorder(indicators, "date")

# free memory
rm(pra)
gc()

# create indicator signals
cols_ = colnames(indicators)[grep("below_dummy_\\d+", colnames(indicators))]
cols_ = c("date", cols_)
indicators_below = indicators[, ..cols_]
q = c(0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99)
indicators_signals = indicators_below[, lapply(.SD, function(x) sapply(quantile(x, probs = q), function(y) x < y)),
                                      .SDcols = setdiff(cols_, "date")]
indicators_signals = cbind(date = indicators[, date], indicators_signals)
setnames(indicators_signals, gsub("\\.", "_", colnames(indicators_signals)))


# BACKTEST OPTIMIZATION ---------------------------------------------------
# parameters
num_coarse = c(50, 100, 200, 300, 500, 1000, 2000) # number of stocks we leave after coarse universe
filter_var = "dollar_volume_mean"                  # variable we use for filtering in coarse universe
min_price = c(1, 2, 5, 10)                         # minimal unadjusted price of the stock at month
num_long = c(10, 30, 50)                         # number of stocks in portfolio
mom_vars = colnames(dt)[grep("mom\\d+$|lag", colnames(dt))] # momenutm variables we apply ranking on
target_vars = "mom_target_m" # colnames(dt)[grep("target", colnames(dt))] # target variables
risk_variables = colnames(indicators_signals)[-1]
params = expand.grid(num_coarse, filter_var, min_price, num_long, mom_vars,
                     target_vars, risk_variables,
                     stringsAsFactors = FALSE)
colnames(params) = c("num_coarse", "filter_var", "min_price", "num_long",
                     "mom_vars", "target_variables", "risk_vars")

# momentum backtest function
strategy_momentum = function(x,
                             filter_var = "dollar_volume_mean",
                             num_coarse = 200,
                             min_price = 1,
                             num_long = 50,
                             mom_var = "mom11",
                             target_variables = "mom_target_m",
                             risk_var = "pr_below_dummy_66",
                             return_cumulative = TRUE) {

  # debug
  # num_coarse = 50
  # min_price = 2
  # num_long = 10
  # mom_var = "mom10"
  # target_variables = "mom_target_m"
  # risk_var = "pr_below_dummy_01_22_5_"
  # x = copy(dtm)

  # remove missing values
  cols = c("symbol", "date", "year_month_id", "close", filter_var, mom_var, target_variables)
  x = na.omit(x[, ..cols])

  # remove assets with price < x$
  x = x[, .SD[close > min_price], by = year_month_id]

  # filter 500 with highest volume
  setorderv(x, c("year_month_id", filter_var), order = 1L)
  x = x[, tail(.SD, num_coarse), by = year_month_id]

  # choose n with highest growth
  setorderv(x, c("year_month_id", mom_var), order = c(1, -1))
  y = x[, head(.SD, num_long), by = year_month_id]
  # y[year_month_id == as.Date("2023-04-30")]

  # get daily data for final universe
  ret_results = list()
  yms = y[, unique(year_month_id)]
  for (i in 1:length(yms)) {
    ym = yms[i]
    print(ym)
    y_sample = y[year_month_id == ym[1]]
    dt_sample = dt[symbol %chin% y_sample$symbol]
    dt_sample = dt_sample[year_month_id == ym[1]]
    cols_ = c("symbol", "date", "year_month_id", "close_adj")
    dt_sample = dt_sample[, ..cols_]
    dt_sample[, returns := close_adj / shift(close_adj) - 1]
    dt_sample = dcast(dt_sample, formula = date ~ symbol, value.var = "returns")
    dt_sample = indicators_signals[, .(date, risk_var_ = get(risk_var))][dt_sample, on = "date"]
    dt_sample = dt_sample[shift(risk_var_) == TRUE]
    if (nrow(dt_sample) == 0) {
      ret_results[[i]] = NULL
    } else if (nrow(dt_sample) == 1) {
      ret_results[[i]] = sum(dt_sample[, 3:ncol(dt_sample)] * 0.1)
    } else {
      ret_results[[i]] = Return.portfolio(as.xts.data.table(dt_sample[, .SD, .SDcols = -c("risk_var_")]))
    }
  }



  # # merge y and pra
  # y_wide = dcast(y, formula = date ~ symbol, value.var = "")
  # y[indicators, on = "date", roll = -Inf]
  #
  # # add daily data to control for risk
  # y_wide = dcast(y, formula = date ~ symbol, value.var = "")
  # dt_sample = y_wide[indicators, on = c("date")]

  # get returns by month
  results = y[, .(returns = sum(get(target_variables) * 1/nrow(.SD))), by = year_month_id]

  # CAR
  if (return_cumulative) {
    car = Return.cumulative(as.xts.data.table(results))
    return(car)
  } else {
    return(results)
  }
}
strategy_momentum(dtm, "dollar_volume_mean", 200, 1, 50, "mom11", "mom_target_m") # example

# backtest across all parameters
results_l = vapply(1:nrow(params), function(i) {
  strategy_momentum(dtm,
                    filter_var = params$filter_var[i],
                    num_coarse = params$num_coarse[i],
                    min_price = params$min_price[i],
                    num_long = params$num_long[i],
                    mom_var = params$mom_vars[i],
                    target_variables = params$target_variables[i])
}, FUN.VALUE = numeric(1L))
results = as.data.table(cbind(params, results_l))

# analyse results
tail(results[order(results_l), ], 10)
results[mom_vars == "mom10" & min_price == 2 & num_long == 10 & num_coarse == 50]
ggplot(results, aes(num_coarse, min_price, fill = results_l)) +
  geom_tile()
ggplot(results, aes(num_coarse, num_long, fill = results_l)) +
  geom_tile()
ggplot(results, aes(min_price, num_long, fill = results_l)) +
  geom_tile()

# inspect atom
strategy_momentum(dtm, "dollar_volume_mean", 50, 2, 10, "mom10", "mom_target_m", TRUE)
x = strategy_momentum(dtm, "dollar_volume_mean", 50, 2, 10, "mom10", "mom_target_m", FALSE)
charts.PerformanceSummary(x)
x[order(returns)]

# compare with QC
dtm[year_month_id == "2023-05-31"] # same
symbol_ = "isee"
dt[symbol == symbol_]
plot(as.xts.data.table(dt[symbol == symbol_, .(date, close_adj)]), type = "l")
dtm[symbol == symbol_]
dtm[symbol == symbol_ & date == "2023-04-28"]
dtms[symbol == symbol_]
dtms[symbol == symbol_ & date == "2010-01-31"]
# Symbol F close_t-n 1.550195415 close_t-1 6.373025595 time 2010-01-01 00:00:00
# Symbol LVS close_t-n 1.72733057 close_t-1 10.91752951 time 2010-01-29 15:00:00   5.320463
# Symbol LVS close_t-n 1.6673075 close_t-1 10.31062958 time 2010-02-01  12:00:00   5.184
6.373025595 / 1.550195415 - 1
dt[symbol == symbol_ & date %between% c("2023-02-01", "2023-04-02")]
dt[symbol == symbol_ & date %between% c("2009-01-26", "2009-02-02")]
dt[symbol == symbol_ & date %between% c(as.Date("2009-12-28") - 270, as.Date("2009-12-28") - 230)]
dt[symbol == symbol_ & round(close_adj, 3) == 1.727]
dt[symbol == symbol_ & round(close_adj, 3) == 11.058]


# vis results
charts.PerformanceSummary(portfolio_ret)

# Portfolio results
SharpeRatio(portfolio_ret)
SharpeRatio.annualized(portfolio_ret)


# inspect
test = as.data.table(portfolio_ret)
test[portfolio.returns > 0.5]
test[portfolio.returns <- -0.5]





# UNIVERSE ----------------------------------------------------------------
# parameters
num_coarse = 500                  # number of stocks we leave after coarse universe
filter_var = "dollar_volume_mean" # variable we use for filtering in coarse universe
min_price = 2                     # minimal unadjusted price of the stock at month
num_long = 50                     # number of stocks in portfolio
mom_var = "mom12"                 # momenutm variables we apply ranking on
target_var = "mom_target_m"       # colnames(dt)[grep("target", colnames(dt))] # target variables
risk_variable = c("pra", "minmax")

# remove missing values
cols = c("symbol", "year_month_id", "date", "close", filter_var, mom_var, target_var)
universe = na.omit(dtm[, ..cols])

# remove assets with price < x$
universe = universe[, .SD[close > min_price], by = year_month_id]

# filter 500 with highest volume
setorderv(universe, c("year_month_id", filter_var), order = 1L)
universe = universe[, tail(.SD, num_coarse), by = year_month_id]

# choose n with highest growth
setorderv(universe, c("year_month_id", mom_var), order = c(1, -1))
universe = universe[, head(.SD, num_long), by = year_month_id]



# PREDICTORS --------------------------------------------------------------
# create OHLCV object for all symbols in universe
symbols_in_universe = universe[, unique(symbol)]
ohlcv = dt[symbol %chin% symbols_in_universe]
universe[, index := TRUE]
ohlcv = merge(ohlcv, universe[, .(symbol, date, index)],
              by = c("symbol", "date"), all.x = TRUE, all.y = FALSE)
ohlcv[index == TRUE]
at_ = ohlcv[, which(index == TRUE)]
lag_ = 1L
ohlcv = Ohlcv$new(ohlcv[, .(symbol, date, open, high, low, close, volume)],
                  id_col = "symbol",
                  date_col = "date",
                  price = "close",
                  ohlcv = c("open", "high", "low", "close", "volume"))

# BackCUSUM features
print("Calculate BackCUSUM features.")
# at_ = get_at_(RollingBackCusumFeatures)
RollingBackcusumInit = RollingBackcusum$new(windows = c(22 * 3, 22 * 6),
                                            workers = 4L,
                                            at = at_,
                                            lag = lag_,
                                            alternative = c("greater", "two.sided"),
                                            return_power = c(1, 2))
RollingBackCusumFeatures_new = RollingBackcusumInit$get_rolling_features(ohlcv)
gc()

# tsfeatures features
print("Calculate tsfeatures features.")
# at_ = get_at_(RollingTsfeaturesFeatures)
RollingTsfeaturesInit = RollingTsfeatures$new(windows = c(22 * 3, 22 * 6),
                                              workers = 6L,
                                              at = at_,
                                              lag = lag_,
                                              scale = TRUE)
RollingTsfeaturesFeaturesNew = RollingTsfeaturesInit$get_rolling_features(ohlcv)
gc()

# theft catch22 features
print("Calculate Catch22 and feasts features.")
# at_ = get_at_(RollingTheftCatch22Features)
RollingTheftInit = RollingTheft$new(windows = c(5, 22, 22 * 3, 22 * 12),
                                    workers = 4L,
                                    at = at_,
                                    lag = lag_,
                                    features_set = c("catch22", "feasts"))
RollingTheftCatch22FeaturesNew = RollingTheftInit$get_rolling_features(ohlcv)
gc()

# ohlcv features
# Features from OHLLCV
print("Calculate Ohlcv features.")
OhlcvFeaturesInit = OhlcvFeatures$new(at = NULL,
                                      windows = c(5, 10, 22, 22 * 3, 22 * 6, 22 * 12),
                                      quantile_divergence_window =  c(22, 22*3, 22*6, 22*12, 22*12*2))
OhlcvFeaturesSet = OhlcvFeaturesInit$get_ohlcv_features(ohlcv)
OhlcvFeaturesSetSample <- OhlcvFeaturesSet[at_ - lag_]
nrow(OhlcvFeaturesSetSample) == nrow(universe)
setorderv(OhlcvFeaturesSetSample, c("symbol", "date"))
# DEBUG
head(OhlcvFeaturesSetSample[symbol == "aapl", .(symbol, date)])

# free memory
rm(OhlcvFeaturesSet)
gc()

# merge all features test
rolling_predictors <- Reduce(
  function(x, y) merge( x, y, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE),
  list(
    RollingBackCusumFeatures_new,
    RollingTheftCatch22FeaturesNew,
    RollingTsfeaturesFeaturesNew
  )
)

# merge ohlcv predictors
rolling_predictors[, date_rolling := date]
OhlcvFeaturesSetSample[, date_ohlcv := date]
predictors <- rolling_predictors[OhlcvFeaturesSetSample, on = c("symbol", "date"), roll = Inf]

# check for duplicates
predictors[duplicated(predictors[, .(symbol, date)]), .(symbol, date)]
predictors[duplicated(predictors[, .(symbol, date_ohlcv)]), .(symbol, date_ohlcv)]
predictors[duplicated(predictors[, .(symbol, date_rolling)]), .(symbol, date_rolling)]
predictors[duplicated(predictors[, .(symbol, date_rolling)]) | duplicated(predictors[, .(symbol, date_rolling)], fromLast = TRUE),
         .(symbol, date, date_ohlcv, date_rolling)]
predictors = predictors[!duplicated(predictors[, .(symbol, date_rolling)])]

# merge features and universe
any(duplicated(universe[, .(symbol, date)]))
any(duplicated(predictors[, .(symbol, date_rolling)]))
predictors[symbol == "aa", .(symbol, date)]
universe[symbol == "aa", .(symbol, date)]
predictors = predictors[universe, on = c("symbol", "date"), roll = Inf]
predictors[, .(symbol, date, date_rolling, date_ohlcv, year_month_id)]
predictors[duplicated(predictors[, .(symbol, date)]), .(symbol, date)]
predictors[duplicated(predictors[, .(symbol, date_ohlcv)]), .(symbol, date_ohlcv)]
predictors[duplicated(predictors[, .(symbol, date_rolling)]), .(symbol, date_rolling)]

#TODO: ADD FUNDAMENTALS
#TODO ADD MACRODATA

# convert char features to numeric features
char_cols <- predictors[, colnames(.SD), .SDcols = is.character]
char_cols <- setdiff(char_cols, c("symbol"))
predictors[, (char_cols) := lapply(.SD, as.numeric), .SDcols = char_cols]



# PREDICTOR SPACE ---------------------------------------------------------
# features space from features raw
cols_remove <- c("index", "close.y", "dollar_volume_mean")
cols_non_predictors <- c("symbol", "date", "date_rolling", "date_ohlcv",
                         "open", "high", "low", "volume", "returns",
                         "mom_target_m", "year_month_id", "i.close")
cols_predictors <- setdiff(colnames(predictors), c(cols_remove, cols_non_predictors))
head(cols_predictors, 10)
tail(cols_predictors, 500)
cols <- c(cols_non_predictors, cols_predictors)
predictors <- predictors[, .SD, .SDcols = cols]

# checks
predictors[, .(symbol, date, date_rolling, year_month_id)]
predictors[, .(symbol, date, date_rolling, kurtosis_10)]



# CLEAN DATA --------------------------------------------------------------
# convert columns to numeric. This is important only if we import existing features
dataset <- copy(predictors)
chr_to_num_cols <- setdiff(colnames(dataset[, .SD, .SDcols = is.character]), c("symbol", "time", "right_time"))
dataset <- dataset[, (chr_to_num_cols) := lapply(.SD, as.numeric), .SDcols = chr_to_num_cols]
log_to_num_cols <- colnames(dataset[, .SD, .SDcols = is.logical])
dataset <- dataset[, (log_to_num_cols) := lapply(.SD, as.numeric), .SDcols = log_to_num_cols]

# remove duplicates
any(duplicated(dataset[, .(symbol, date)]))
dataset <- unique(dataset, by = c("symbol", "date"))

# remove columns with many NA
keep_cols <- names(which(colMeans(!is.na(dataset)) > 0.5))
print(paste0("Removing columns with many NA values: ", setdiff(colnames(dataset), c(keep_cols, "right_time"))))
dataset <- dataset[, .SD, .SDcols = keep_cols]

# remove Inf and Nan values if they exists
is.infinite.data.frame <- function(x) do.call(cbind, lapply(x, is.infinite))
keep_cols <- names(which(colMeans(!is.infinite(as.data.frame(dataset))) > 0.98))
print(paste0("Removing columns with Inf values: ", setdiff(colnames(dataset), keep_cols)))
dataset <- dataset[, .SD, .SDcols = keep_cols]

# remove inf values
n_0 <- nrow(dataset)
clf_data <- dataset[is.finite(rowSums(dataset[, .SD, .SDcols = is.numeric], na.rm = TRUE))]
n_1 <- nrow(dataset)
print(paste0("Removing ", n_0 - n_1, " rows because of Inf values"))



# PREPARE DATA FOR THE TASK -----------------------------------------------
# change type of year_month_id
dataset[, year_month_id := as.integer(year_month_id)]

# define predictors
dataset[, .(date, year_month_id)]
cols_non_features <- c(
  "symbol", "date", "date_rolling", "date_ohlcv", "open", "high", "low",
  "volume", "returns", "year_month_id", "i.close"
)
targets <- c(colnames(dataset)[grep("mom_targ", colnames(dataset))])
cols_features <- setdiff(colnames(dataset), c(cols_non_features, targets))

# remove constant columns in set and remove same columns in test set
features_ <- dataset[, ..cols_features]
remove_cols <- colnames(features_)[apply(features_, 2, var, na.rm=TRUE) == 0]
print(paste0("Removing feature with 0 standard deviation: ", remove_cols))
cols_features <- setdiff(cols_features, remove_cols)

# convert variables with low number of unique values to factors
int_numbers = na.omit(dataset[, ..cols_features])[, lapply(.SD, function(x) all(floor(x) == x))]
int_cols = colnames(dataset[, ..cols_features])[as.matrix(int_numbers)[1,]]
factor_cols = dataset[, ..int_cols][, lapply(.SD, function(x) length(unique(x)))]
factor_cols = as.matrix(factor_cols)[1, ]
factor_cols = factor_cols[factor_cols <= 100]
dataset = dataset[, (names(factor_cols)) := lapply(.SD, as.factor), .SD = names(factor_cols)]

# remove observations with missing target
# if we want to keep as much data as possible an use only one predicitn horizont
# we can skeep this step
dataset = na.omit(dataset, cols = targets)

# change IDate to date, because of error
# Assertion on 'feature types' failed: Must be a subset of
# {'logical','integer','numeric','character','factor','ordered','POSIXct'},
# but has additional elements {'IDate'}.
dataset[, date := as.POSIXct(date, tz = "UTC")]
dataset[, .(symbol,date, date_rolling, year_month_id)]

# sort
setorder(dataset, date)

# inspect
dataset[, .(symbol, date)]



# TASKS -------------------------------------------------------------------
# id columns we always keep
id_cols = c("symbol", "date", "year_month_id")

# task with future week returns as target
target_ = colnames(dataset)[grep("mom_target", colnames(dataset))]
cols_ = c(id_cols, target_, cols_features)
task_mom <- as_task_regr(dataset[, ..cols_],
                         id = "task_mom",
                         target = target_)

# set roles for symbol, date and yearmonth_id
task_mom$col_roles$feature = setdiff(task_mom$col_roles$feature, id_cols)



# CROSS VALIDATIONS -------------------------------------------------------
# create train, tune and test set
nested_cv_split = function(task,
                           train_length = 12,
                           tune_length = 2,
                           test_length = 1) {

  # create cusom CV's for inner and outer sampling
  custom_inner = rsmp("custom")
  custom_outer = rsmp("custom")

  # get year month id data
  # task = task_ret_week$clone()
  task_ = task$clone()
  yearmonthid_ = task_$backend$data(cols = c("year_month_id", "..row_id"),
                                    rows = 1:task_$nrow)
  stopifnot(all(task_$row_ids == yearmonthid_$`..row_id`))
  groups_v = yearmonthid_[, unlist(unique(year_month_id))]

  # util vars
  start_folds = 1:(length(groups_v)-train_length-tune_length-test_length)
  get_row_ids = function(mid) unlist(yearmonthid_[year_month_id %in% mid, 2], use.names = FALSE)

  # create train data
  train_groups <- lapply(start_folds,
                         function(x) groups_v[x:(x+train_length-1)])
  train_sets <- lapply(train_groups, get_row_ids)

  # create tune set
  tune_groups <- lapply(start_folds,
                        function(x) groups_v[(x+train_length):(x+train_length+tune_length-1)])
  tune_sets <- lapply(tune_groups, get_row_ids)

  # test train and tune
  test_1 = vapply(seq_along(train_groups), function(i) {
    mondf(
      tail(as.Date(train_groups[[i]], origin = "1970-01-01"), 1),
      head(as.Date(tune_groups[[i]], origin = "1970-01-01"), 1)
    )
  }, FUN.VALUE = numeric(1L))
  stopifnot(all(test_1 == 1))
  test_2 = vapply(seq_along(train_groups), function(i) {
    unlist(head(tune_sets[[i]], 1) - tail(train_sets[[i]], 1))
  }, FUN.VALUE = numeric(1L))
  stopifnot(all(test_2 == 1))

  # create test sets
  insample_length = train_length + tune_length
  test_groups <- lapply(start_folds,
                        function(x) groups_v[(x+insample_length):(x+insample_length+test_length-1)])
  test_sets <- lapply(test_groups, get_row_ids)

  # test tune and test
  test_3 = vapply(seq_along(train_groups), function(i) {
    mondf(
      tail(as.Date(tune_groups[[i]], origin = "1970-01-01"), 1),
      head(as.Date(test_groups[[i]], origin = "1970-01-01"), 1)
    )
  }, FUN.VALUE = numeric(1L))
  stopifnot(all(test_1 == 1))
  test_4 = vapply(seq_along(train_groups), function(i) {
    unlist(head(test_sets[[i]], 1) - tail(tune_sets[[i]], 1))
  }, FUN.VALUE = numeric(1L))
  stopifnot(all(test_2 == 1))

  # create inner and outer resamplings
  custom_inner$instantiate(task, train_sets, tune_sets)
  inner_sets = lapply(seq_along(train_groups), function(i) {
    c(train_sets[[i]], tune_sets[[i]])
  })
  custom_outer$instantiate(task, inner_sets, test_sets)
  return(list(custom_inner = custom_inner, custom_outer = custom_outer))
}

# test
custom_cvs = nested_cv_split(task_mom)
custom_inner = custom_cvs$custom_inner
custom_outer = custom_cvs$custom_outer

# generate cv's
train_sets = seq(12, 12 * 6, 12 * 2)
validation_sets = train_sets / 12
custom_cvs = list()
for (i in seq_along(train_sets)) {
  print(i)
  custom_cvs[[i]] = nested_cv_split(task_mom,
                                    train_sets[[i]],
                                    validation_sets[[i]],
                                    1)
}

# test if tain , validation and tst set follow logic
lapply(seq_along(custom_cvs), function(i) {
  # extract custyom cv
  custom_cvs_ = custom_cvs[[i]]
  custom_inner = custom_cvs_$custom_inner
  custom_outer = custom_cvs_$custom_outer

  # test set start after train set
  test1 = all(vapply(1:custom_inner$iters, function(i) {
    (tail(custom_inner$train_set(i), 1) + 1) == custom_inner$test_set(i)[1]
  }, FUN.VALUE = logical(1L)))

  # train set in outersample contains ids in innersample 1
  test2  = all(vapply(1:custom_inner$iters, function(i) {
    all(c(custom_inner$train_set(i),
          custom_inner$test_set(i)) == custom_outer$train_set(i))
  }, FUN.VALUE = logical(1L)))
  c(test1, test2)
})



# ADD PIPELINES -----------------------------------------------------------
# source pipes, filters and other
source("R/mlr3_winsorization.R")
source("R/mlr3_uniformization.R")
source("R/mlr3_gausscov_f1st.R")
source("R/mlr3_gausscov_f3st.R")
source("R/mlr3_dropna.R")
source("R/mlr3_dropnacol.R")
source("R/mlr3_filter_drop_corr.R")
source("R/mlr3_winsorizationsimple.R")
source("R/mlr3_winsorizationsimplegroup.R")
source("R/PipeOpPCAExplained.R")
# measures
source("R/Linex.R")
source("R/PortfolioRet.R")
source("R/AdjLoss2.R")

# add my pipes to mlr dictionary
mlr_pipeops$add("uniformization", PipeOpUniform)
mlr_pipeops$add("winsorize", PipeOpWinsorize)
mlr_pipeops$add("winsorizesimple", PipeOpWinsorizeSimple)
mlr_pipeops$add("winsorizesimplegroup", PipeOpWinsorizeSimpleGroup)
mlr_pipeops$add("dropna", PipeOpDropNA)
mlr_pipeops$add("dropnacol", PipeOpDropNACol)
mlr_pipeops$add("dropcorr", PipeOpDropCorr)
mlr_pipeops$add("pca_explained", PipeOpPCAExplained)
mlr_filters$add("gausscov_f1st", FilterGausscovF1st)
mlr_filters$add("gausscov_f3st", FilterGausscovF3st)
mlr_measures$add("linex", Linex)
mlr_measures$add("portfolio_ret", PortfolioRet)
mlr_measures$add("adjloss2", AdjLoss2)



# GRAPH -------------------------------------------------------------------
graph_template =
  # po("subsample") %>>% # uncomment this for hyperparameter tuning
  po("dropnacol", id = "dropnacol", cutoff = 0.05) %>>%
  po("dropna", id = "dropna") %>>%
  po("removeconstants", id = "removeconstants_1", ratio = 0)  %>>%
  po("fixfactors", id = "fixfactors") %>>%
  # po("winsorizesimple", id = "winsorizesimple", probs_low = 0.01, probs_high = 0.99, na.rm = TRUE) %>>%
  po("winsorizesimplegroup", group_var = "year_month_id", id = "winsorizesimplegroup", probs_low = 0.001, probs_high = 0.999, na.rm = TRUE) %>>%
  po("removeconstants", id = "removeconstants_2", ratio = 0)  %>>%
  po("dropcorr", id = "dropcorr", cutoff = 0.99) %>>%
  po("uniformization") %>>%
  po("dropna", id = "dropna_v2") %>>%
  po("filter", filter = flt("gausscov_f1st"), filter.cutoff = 0) %>>%
  po("branch", options = c("nop_filter", "modelmatrix"), id = "interaction_branch") %>>%
  gunion(list(
    po("nop", id = "nop_filter"),
    po("modelmatrix", formula = ~ . ^ 2))) %>>%
  po("unbranch", id = "interaction_unbranch") %>>%
  po("removeconstants", id = "removeconstants_3", ratio = 0)

# hyperparameters template
search_space_template = ps(
  # subsample for hyperband
  # subsample.frac = p_dbl(0.3, 1, tags = "budget"), # unccoment this if we want to use hyperband optimization
  # preprocessing
  dropcorr.cutoff = p_fct(
    levels = c("0.80", "0.90", "0.95", "0.99"),
    trafo = function(x, param_set) {
      switch(x,
             "0.80" = 0.80,
             "0.90" = 0.90,
             "0.95" = 0.95,
             "0.99" = 0.99)
    }
  ),
  # dropcorr.cutoff = p_fct(levels = c(0.8, 0.9, 0.95, 0.99)),
  winsorizesimplegroup.probs_high = p_fct(levels = c(0.999, 0.99, 0.98, 0.97, 0.90, 0.8)),
  winsorizesimplegroup.probs_low = p_fct(levels = c(0.001, 0.01, 0.02, 0.03, 0.1, 0.2)),
  # filters
  interaction_branch.selection = p_fct(levels = c("nop_filter", "modelmatrix"))
)

# random forest graph
graph_rf = graph_template %>>%
  po("learner", learner = lrn("regr.ranger"))
graph_rf = as_learner(graph_rf)
as.data.table(graph_rf$param_set)[, .(id, class, lower, upper, levels)]
search_space_rf = search_space_template$clone()
search_space_rf$add(
  ps(regr.ranger.max.depth = p_int(1, 40))
)

# xgboost graph
graph_xgboost = graph_template %>>%
  po("learner", learner = lrn("regr.xgboost"))
graph_xgboost = as_learner(graph_xgboost)
as.data.table(graph_xgboost$param_set)[grep("alpha", id), .(id, class, lower, upper, levels)]
search_space_xgboost = ps(
  # subsample for hyperband
  # subsample.frac = p_dbl(0.3, 1, tags = "budget"), # unccoment this if we want to use hyperband optimization
  # preprocessing
  dropcorr.cutoff = p_fct(
    levels = c("0.80", "0.90", "0.95", "0.99"),
    trafo = function(x, param_set) {
      switch(x,
             "0.80" = 0.80,
             "0.90" = 0.90,
             "0.95" = 0.95,
             "0.99" = 0.99)
    }
  ),
  # dropcorr.cutoff = p_fct(levels = c(0.8, 0.9, 0.95, 0.99)),
  winsorizesimplegroup.probs_high = p_fct(levels = c(0.999, 0.99, 0.98, 0.97, 0.90, 0.8)),
  winsorizesimplegroup.probs_low = p_fct(levels = c(0.001, 0.01, 0.02, 0.03, 0.1, 0.2)),
  # filters
  interaction_branch.selection = p_fct(levels = c("nop_filter", "modelmatrix")),
  # learner
  regr.xgboost.alpha = p_dbl(0.001, 100, logscale = TRUE)
)

# inspect search space
design = rbindlist(generate_design_grid(search_space_xgboost, 3)$transpose(), fill = TRUE)
design



# NESTED CV BENCHMARK -----------------------------------------------------
# nested for loop
# plan("multises(sion", workers = 2L)
lapply(custom_cvs, function(cv_) {
  # for (cv_ in custom_cvs) {

  # debug
  # cv_ = custom_cvs[[1]]

  # get cv inner object
  cv_inner = cv_$custom_inner
  cv_outer = cv_$custom_outer
  cat("Number of iterations fo cv inner is ", cv_inner$iters, "\n")

  for (i in 1:custom_inner$iters) {
    # debug
    # i = 1
    print(i)

    # inner resampling
    custom_ = rsmp("custom")
    custom_$instantiate(task_mom,
                        list(cv_inner$train_set(i)),
                        list(cv_inner$test_set(i)))

    # auto tuner rf
    at_rf = auto_tuner(
      tuner = tnr("mbo"), # tnr("hyperband", eta = 5),
      learner = graph_rf,
      resampling = custom_,
      measure = msr("adjloss2"),
      search_space = search_space_rf,
      term_evals = 10
      # terminator = trm("none")
    )

    # auto tuner rf
    at_xgboost = auto_tuner(
      tuner = tnr("mbo"), # tnr("hyperband", eta = 5),
      learner = graph_xgboost,
      resampling = custom_,
      measure = msr("adjloss2"),
      search_space = search_space_xgboost,
      term_evals = 10
      # terminator = trm("none")
    )

    # outer resampling
    customo_ = rsmp("custom")
    customo_$instantiate(task_mom, list(cv_outer$train_set(i)), list(cv_outer$test_set(i)))

    # nested CV for one round
    design = benchmark_grid(
      tasks = list(task_mom),
      learners = list(at_rf, at_xgboost),
      resamplings = customo_
    )
    bmr = benchmark(design, store_models = TRUE)

    # save locally and to list
    time_ = format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
    file_name = paste0("cv-", cv_$custom_inner$iters, "-", i, "-", time_, ".rds")
    saveRDS(bmr, file.path(MLR3SAVEPATH, file_name))
  }
})



# test
dt[symbol == "gef" & date %between% c("2021-09-01", "2021-09-03"), .(symbol, date, open, open_adj, close, close_adj)]


