library(data.table)
library(Rcpp)
library(TTR)
library(lubridate)
library(ggplot2)
library(PerformanceAnalytics)
library(runner)
library(glue)


# UTILS -------------------------------------------------------------------
# date segments
GFC         = c("2007-01-01", "2010-01-01")
COVID       = c("2020-01-01", "2021-06-01")
AFTER_COVID = c("2021-06-01", "2022-01-01")
CORECTION   = c("2022-01-01", "2022-08-01")
NEW         = c("2022-08-01", as.character(Sys.Date()))

# Globals
RESULTS = "F:/strategies/MinMaxWfo"


# DATA --------------------------------------------------------------------
# SPY data
spy = fread("F:/predictors/minmax/20240228.csv")
spy[, attr(date, "tz")]
spy[, date := with_tz(date, tzone = "America/New_York")]
spy[, attr(date, "tz")]
spy = spy[symbol == "spy", .(date, close, returns)]
gc()

# Import indicators
indicators = fread("F:/predictors/minmax/indicators.csv")

# Merge spy and indicators
sysrisk = merge(indicators, spy, by = "date", all.x = TRUE, all.y = FALSE)
sysrisk = na.omit(sysrisk, cols = "returns")

# Visualize minmax variables
ggplot(sysrisk, aes(x = date)) +
  geom_line(aes(y = pmax(pmin(mean_excess_p_999_2year, 0.01), -0.01)))
ggplot(sysrisk, aes(x = date)) +
  geom_line(aes(y = pmax(pmin(sd_excess_p_999_2year, 0.1), -0.1)))
ggplot(sysrisk, aes(x = date)) +
  geom_line(aes(y = pmax(pmin(sum_excess_p_999_2year, 1), -1)))
ggplot(sysrisk, aes(x = date)) +
  geom_line(aes(y = pmax(pmin(skewness_excess_p_999_2year, 0.001), -0.001)))
ggplot(sysrisk, aes(x = date)) +
  geom_line(aes(y = pmax(pmin(kurtosis_excess_p_999_2year, 0.001), -0.001)))

# Scatterplot of minmax and returns
ggplot(sysrisk, aes(x = shift(pmax(pmin(mean_excess_p_999_year, 0.015), -0.015)), y = returns)) +
  geom_point() +
  geom_smooth()
ggplot(sysrisk, aes(x = shift(pmax(pmin(sd_excess_p_999_year, 0.1), -0.1)), y = returns)) +
  geom_point() +
  geom_smooth()
ggplot(sysrisk, aes(x = shift(pmax(pmin(sum_excess_p_999_year, 0.1), -0.1)), y = returns)) +
  geom_point() +
  geom_smooth()
ggplot(sysrisk, aes(x = shift(pmax(pmin(skewness_excess_p_999_year, 0.1), -0.1)), y = returns)) +
  geom_point() +
  geom_smooth()

# Returns by value of in
data_plot = sysrisk[, .(date, returns, alpha = shift(mean_excess_p_999_4year > 0.01))]
na.omit(data_plot)[, mean(returns), by = .(alpha)] |>
  ggplot(aes(x = alpha, y = V1)) +
  geom_bar(stat = "identity")

# Choose columns
# 1) Choose subset of columns
cols = c("date", "close", "returns", colnames(indicators)[grepl("sum_ex", colnames(indicators))])
backtest_dt = sysrisk[, ..cols]
# 2) choose all columns
backtest_dt = copy(sysrisk)

# Remove columns with many NA values
cols_keep = colnames(backtest_dt)[sapply(backtest_dt, function(x) sum(is.na(x))/length(x) < 0.5)]
backtest_dt = backtest_dt[, ..cols_keep]

# Remove NA values
backtest_dt = na.omit(backtest_dt)

# Define predictors
predictors = setdiff(colnames(backtest_dt), c("date", "close", "returns"))
predictors_excess = predictors[grepl("excess", predictors)]
predictors_sum = predictors[grepl("sum", predictors)]
predictors_sd = predictors[grepl("sd", predictors)]
predictors_skew = predictors[grepl("skew", predictors)]
predictors_kurtosis = predictors[grepl("kurtosis", predictors)]


# INSAMPLE OPTIMIZATION ---------------------------------------------------
# backtest Rcpp
Rcpp::cppFunction("
  double backtest_cpp(NumericVector returns, NumericVector indicator, double threshold) {
    int n = indicator.size();
    NumericVector sides(n);

    for(int i=0; i<n; i++){
      if(i==0 || R_IsNA(indicator[i-1])) {
        sides[i] = 1;
      } else if (indicator[i-1] < threshold){
        sides[i] = 0;
      } else {
        sides[i] = 1;
      }
    }

    NumericVector returns_strategy = returns * sides;

    double cum_returns{ 1 + returns_strategy[0]} ;
    for(int i=1; i<n; i++){
      cum_returns *= (1 + returns_strategy[i]);
    }
    cum_returns = cum_returns - 1;

    return cum_returns;
  }
", rebuild = TRUE)
Rcpp::cppFunction("
  double backtest_above_threshold(NumericVector returns, NumericVector indicator, double threshold) {
    int n = indicator.size();
    NumericVector sides(n);

    for(int i=0; i<n; i++){
      if(i==0 || R_IsNA(indicator[i-1])) {
        sides[i] = 1;
      } else if (indicator[i-1] > threshold){
        sides[i] = 0;
      } else {
        sides[i] = 1;
      }
    }

    NumericVector returns_strategy = returns * sides;

    double cum_returns{ 1 + returns_strategy[0]} ;
    for(int i=1; i<n; i++){
      cum_returns *= (1 + returns_strategy[i]);
    }
    cum_returns = cum_returns - 1;

    return cum_returns;
  }
", rebuild = TRUE)

backtest_vectorized = function(returns, indicator, threshold, return_cumulative = TRUE) {
  # returns = returns_
  # i = 1
  # indicator = SMA(backtest_dt[, get(vars[i])], ns[i])
  # threshold = thresholds_[i]
  # return_cumulative = TRUE

  # sides = ifelse(c(NA, head(indicator, -1)) > threshold, 0, 1)
  sides = ifelse(shift(indicator) < threshold, 0, 1)
  sides[is.na(sides)] = 1

  returns_strategy = returns * sides
  # returns_strategy_1 = returns_strategy

  if (return_cumulative) {
    # cum_returns = 1 + returns_strategy[1]
    # for (i in 2:length(returns_strategy)) {
    #   cum_returns = cum_returns * (1 + returns_strategy[i])
    # }
    # return(cum_returns - 1)
    return(PerformanceAnalytics::Return.cumulative(returns_strategy))
  } else {
    return(returns_strategy)
  }
}
backtest <- function(returns, indicator, threshold, return_cumulative = TRUE) {
  # returns = returns_
  # i = 1
  # indicator = SMA(backtest_dt[, get(vars[i])], ns[i])
  # threshold = thresholds_[i]
  # return_cumulative = TRUE

  sides <- vector("integer", length(indicator))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(indicator[i-1])) {
      sides[i] <- 1
    } else if (indicator[i-1] < threshold) {
      sides[i] <- 0
    } else {
      sides[i] <- 1
    }
  }
  sides <- ifelse(is.na(sides), 1, sides)
  returns_strategy <- returns * sides
  # returns_strategy_2 = returns_strategy

  if (return_cumulative) {
    return(PerformanceAnalytics::Return.cumulative(returns_strategy))
  } else {
    return(returns_strategy)
  }
}
performance <- function(x) {
  cumRetx = Return.cumulative(x)
  annRetx = Return.annualized(x, scale=252)
  sharpex = SharpeRatio.annualized(x, scale=252)
  winpctx = length(x[x > 0])/length(x[x != 0])
  annSDx = sd.annualized(x, scale=252)

  DDs <- findDrawdowns(x)
  maxDDx = min(DDs$return)
  maxLx = max(DDs$length)

  Perf = c(cumRetx, annRetx, sharpex, winpctx, annSDx, maxDDx, maxLx)
  names(Perf) = c("Cumulative Return", "Annual Return","Annualized Sharpe Ratio",
                  "Win %", "Annualized Volatility", "Maximum Drawdown", "Max Length Drawdown")
  return(Perf)
}

# Function to get parameterss
get_params = function(dt, predictors) {
  # Optimization insample parameters
  params = dt[, ..predictors]
  params = params[, lapply(.SD, quantile, probs = seq(0, 1, 0.02), na.rm = TRUE)]
  params = melt(params, variable.factor = FALSE)
  param_sman = c(1, 5, 15, 22, 44, 66)

  # Combine variables, thresholds and sma_n
  params_expanded = params[rep(1:.N, each = length(param_sman))]
  params_expanded[, new_col := rep(param_sman, times = nrow(params))]
  params_expanded = unique(params_expanded)
  setnames(params_expanded, c("variable", "thresholds", "sma_n"))

  return(params_expanded)
}

# Parameters
params_above_threshold = get_params(backtest_dt, predictors_sd)

# help vectors
returns_    = backtest_dt[, returns]
vars        = params_expanded[, 1][[1]]
thresholds_ = params_expanded[, 2][[1]]
ns          = params_expanded[, 3][[1]]
vars_above       = params_above_threshold[, 1][[1]]
thresholds_above = params_above_threshold[, 2][[1]]
ns_above         = params_above_threshold[, 3][[1]]

# optimization loop
system.time({
  opt_results = vapply(1:nrow(params_expanded), function(i) {
    backtest_cpp(returns_, SMA(backtest_dt[, get(vars[i])], ns[i]), thresholds_[i])
  }, numeric(1))
})
opt_results_dt = as.data.table(
  cbind.data.frame(params_expanded, cum_return = opt_results)
)
setnames(opt_results_dt, c("var", "threshold", "sma_n", "cum_return"))
setorder(opt_results_dt, -cum_return)
first(opt_results_dt, 10)

# optimization loop for above threshold backtest
system.time({
  opt_results = vapply(1:nrow(params_expanded), function(i) {
    backtest_above_threshold(returns_, SMA(backtest_dt[, get(vars[i])], ns[i]), thresholds_[i])
  }, numeric(1))
})
opt_results_dt = as.data.table(
  cbind.data.frame(params_expanded, cum_return = opt_results)
)
setnames(opt_results_dt, c("var", "threshold", "sma_n", "cum_return"))
setorder(opt_results_dt, -cum_return)
first(opt_results_dt, 10)

# optimization loop vectorized
system.time({
  opt_results_vect =
    vapply(1:nrow(params_expanded), function(i)
      backtest_vectorized(returns_, SMA(backtest_dt[, get(vars[i])], ns[i]), thresholds_[i]),
      numeric(1))
})
opt_results_vect_dt = as.data.table(
  cbind.data.frame(params_expanded, cum_return = opt_results_vect)
)
setnames(opt_results_vect_dt, c("var", "threshold", "sma_n", "cum_return"))
setorder(opt_results_vect_dt, -cum_return)
first(opt_results_vect_dt, 10)

# # optimization loop with backtest
# system.time({
#   opt_results_r =
#     vapply(1:nrow(params_expanded), function(i)
#       backtest(returns_, SMA(backtest_dt[, get(vars[i])], ns[i]), thresholds_[i]),
#       numeric(1))
# })
# # user  system elapsed
# # 1619.35    1.19 1622.57
# opt_results_r_dt = cbind.data.frame(params_expanded, opt_results_r)
# setnames(opt_results_r_dt, c("threshold", "sma_n", "var", "cum_return"))
# setorder(opt_results_r_dt, -cum_return)
# first(opt_results_r_dt, 10)

# Compare above results. Should be all the same. If all test TRUE, they are same
all.equal(length(opt_results_vect), length(opt_results_r), length(opt_results))
all(round(opt_results_vect, 2) == round(opt_results_r, 2))
all(round(opt_results_vect, 2) == round(opt_results, 2))

# Results across vars
vars_results = opt_results_dt[, .(var_mean = mean(cum_return)), by = var]
vars_results[, var_agg := gsub("_.*", "", var)]
vars_results[, mean(var_mean), by = var_agg]

# inspect best results
best_strategy = opt_results_dt[1, ]
strategy_returns = backtest(returns_,
                            SMA(backtest_dt[, .SD, .SDcols = best_strategy$var],
                                best_strategy$sma_n),
                            best_strategy$threshold,
                            FALSE)
dt_xts = xts(cbind(returns_, strategy_returns), order.by = backtest_dt[, date])
charts.PerformanceSummary(dt_xts)
charts.PerformanceSummary(dt_xts["2020/"])
charts.PerformanceSummary(dt_xts["2022/"])
charts.PerformanceSummary(dt_xts["2023/"])

# Results across lags
dt_ = as.data.table(opt_results_dt)
dt_[, .(mean = mean(cum_return),
        median = median(cum_return)), by = sma_n]

# Optimization for above threshold
opt_results_above = vapply(1:nrow(params_above_threshold), function(i) {
  backtest_cpp(returns_,
               SMA(backtest_dt[, get(vars_above[i])], ns_above[i]),
               thresholds_above[i])
}, numeric(1))
opt_results_above_dt = as.data.table(
  cbind.data.frame(params_above_threshold, cum_return = opt_results_above)
)
setnames(opt_results_above_dt, c("var", "threshold", "sma_n", "cum_return"))
setorder(opt_results_above_dt, -cum_return)
first(opt_results_above_dt, 10)


# RCPP VS R ---------------------------------------------------------------
# Source backtest.cpp file
sourceCpp("backtest.cpp")

# Backtest function
system.time({
  x = backtest(
    returns_,
    SMA(backtest_dt[, .SD, .SDcols = best_strategy$var], best_strategy$sma_n),
    best_strategy$threshold,
    TRUE)
})
system.time({
  y = backtest_sell_below_threshold(
    returns_,
    SMA(backtest_dt[, .SD, .SDcols = best_strategy$var], best_strategy$sma_n),
    best_strategy$threshold)
})
all(round(x, 3) == round(y, 3))

# SMA function
x_= runif(100)
sma_x = SMA(x_, 50)
sma_y = calculate_sma(x_, 50)
all(sma_x == sma_y, na.rm = TRUE)

# Optimization function
params = backtest_dt[, ..predictors]
params = params[, lapply(.SD, quantile, probs = seq(0, 1, 0.3), na.rm = TRUE)]
params = melt(params, variable.factor = FALSE)
param_sman = c(1, 5, 15, 22, 44, 66)
params_expanded = params[rep(1:.N, each = length(param_sman))]
params_expanded[, new_col := rep(param_sman, times = nrow(params))]
params_expanded = unique(params_expanded)
setnames(params_expanded, c("variable", "thresholds", "sma_n"))
returns_    = backtest_dt[, returns]
vars        = params_expanded[, 1][[1]]
thresholds_ = params_expanded[, 2][[1]]
ns          = params_expanded[, 3][[1]]
system.time({
  x = vapply(1:nrow(params_expanded), function(i) {
    backtest_cpp(returns_, SMA(backtest_dt[, get(vars[i])], ns[i]), thresholds_[i])
  }, numeric(1))
})
# Optimization function cpp
params_ = copy(params_expanded)
setnames(params_, c("variable", "thresholds", "sma_n"))
system.time({y = opt_with_sma(df = backtest_dt, params = params_)})
length(x) == length(y)
all(round(x, 3) == round(y, 3))

# WFO approaches
windows = 7 * 22
system.time({
  x = lapply(windows, function(w) {
    bres = runner(
      x = as.data.frame(backtest_dt),
      f = function(x) {
        ret = vapply(1:nrow(params_), function(i) {
          backtest_vectorized(x$returns,
                              SMA(x[, params_[i, variable]], params_[i, sma_n]),
                              params_[i, thresholds])
        }, numeric(1))
        returns_strategies = cbind.data.frame(params_, ret)
        returns_strategies[order(returns_strategies$ret, decreasing = TRUE), ]
      },
      k = w,
      at = 154:250,
      na_pad = TRUE,
      simplify = FALSE
    )
  })
})
df_ = as.data.frame(backtest_dt[1:250])
colnames(df_)[1] = "time"

system.time({y = wfo_with_sma(df_, params_, windows, "rolling")})
length(x[[1]])
length(y)
nrow(x[[1]][[1]])
nrow(y[[1]])
y = lapply(y, function(df_) {
  df_[, 1] = as.POSIXct(df_[, 1])
  df_
})
y = lapply(y, function(df_) {
  df_[, 1] = with_tz(df_[, 1], tzone = "America/New_York")
  df_
})
y = lapply(y, function(df_) {
  cbind.data.frame(df_, params_)
})
y = rbindlist(y)
setorder(y, Scalar, -Vector)
head(x[[1]][[1]]); head(y)
all(round(x[[1]][[1]][, "ret"], 2) == y[1:nrow(x[[1]][[1]]), round(Vector, 2)])


# WALK FORWARD OPTIMIZATION -----------------------------------------------
# Optimization params
predictors_ = c(predictors_skew, predictors_kurtosis)
params = backtest_dt[, ..predictors_]
params = params[, lapply(.SD, quantile, probs = seq(0, 1, 0.02), na.rm = TRUE)]
params = melt(params, variable.factor = FALSE)
param_sman = c(1, 5, 15, 22, 44, 66)
params_expanded = params[rep(1:.N, each = length(param_sman))]
params_expanded[, new_col := rep(param_sman, times = nrow(params))]
params_expanded = unique(params_expanded)
setnames(params_expanded, c("variable", "thresholds", "sma_n"))

# WFO utils
clean_wfo = function(y) {
  y = lapply(y, function(df_) {
    df_[, 1] = as.POSIXct(df_[, 1])
    df_[, 1] = with_tz(df_[, 1], tzone = "America/New_York")
    cbind.data.frame(df_, params_expanded)
  })
  y = rbindlist(y)
  return(y)
}
calcualte_and_save_wfo = function(window) {
  # window = 7 * 22 * 6
  wfo_results_ = wfo(df, params_expanded, window, "rolling")
  wfo_results_ = clean_wfo(wfo_results_)
  fwrite(wfo_results_, file.path(RESULTS, glue("wfo_results_{window}.csv")))
  return(0)
}

# Walk forward optimization
df = as.data.frame(backtest_dt)
colnames(df)[1] = "time"
calcualte_and_save_wfo(7 * 22 * 1)
# calcualte_and_save_wfo(7 * 22 * 6)
# calcualte_and_save_wfo(7 * 22 * 12)
# calcualte_and_save_wfo(7 * 22 * 18) # never finished try before go home

# Import results
list.files(RESULTS)
wfo_results = fread(file.path(RESULTS, "wfo_results_154.csv"))
setnames(wfo_results, c("date", "return", "variable", "thresholds", "n"))
setorder(wfo_results, date, return)

# Keep only sd
# wfo_results = wfo_results[variable %like% "sd_"]

# Keep best
wfo_results_best_n = wfo_results[, first(.SD, 50), by = date]
wfo_results_best_n[, unique(variable)]

# Merge results with backtest data, that is price data
backtest_long = melt(backtest_dt, id.vars = c("date", "returns"))
backtest_long = merge(backtest_long, wfo_results_best_n, by = c("date", "variable"),
                      all.x = TRUE, all.y = FALSE)
backtest_long = na.omit(backtest_long, cols = "return")
backtest_long[, signal := value < thresholds]
backtest_long[, signal_ensamble := sum(signal) > 20, by = date]
backtest_long = unique(backtest_long[, .(date, returns, signal = signal_ensamble)])

# Backtest
backtest_xts = backtest_long[, .(date, benchmark = returns, signal = shift(signal))]
backtest_xts[, strategy := benchmark * signal]
backtest_xts = as.xts.data.table(na.omit(backtest_xts))
charts.PerformanceSummary(backtest_xts)



# GAUSSCOV PREDICTIONS ----------------------------------------------------
#
# cols = c("date", "close", "returns", excess_cols)
cols_keep = colnames(sysrisk)[sapply(sysrisk, function(x) sum(is.na(x))/length(x) < 0.1)]
gausscov_dt = sysrisk[, ..cols_keep]
dim(gausscov_dt)
gausscov_dt = na.omit(gausscov_dt)

# define feature matrix
cols_keep = c(colnames(gausscov_dt)[2:(ncol(gausscov_dt)-3)], "returns", "date")
DT = gausscov_dt[, ..cols_keep]
cols_ = colnames(DT)[1:(ncol(DT)-2)]
DT = DT[, (cols_) := lapply(.SD, shift, n = 1), .SDcols = cols_]
DT = na.omit(DT)
DT_dates = DT[, date]
X = as.matrix(DT[, .SD, .SDcols = -c("date")])
dim(X)

# f1st
f1st_fi_= f1st(X[, ncol(X)], X[, -ncol(X)], p0 = 0.01)
cov_index_f1st_ = colnames(X[, -ncol(X)])[f1st_fi_[[1]][, 1]]
# [1] "mean_below_p_001_2year"    "mean_below_p_001_halfyear" "sd_below_p_05_halfyear"
# [4] "sum_below_p_001_2year"     "sum_below_p_001_year"      "sum_excess_p_95_4year"

# f3st_1
f3st_fi_ = f3st(X[, ncol(X)], X[, -ncol(X)], m = 1)
cov_index_f3st = unique(as.integer(f3st_fi_[[1]][1, ]))[-1]
cov_index_f3st = cov_index_f3st[cov_index_f3st != 0]
cov_index_f3st = colnames(X[, -ncol(X)])[cov_index_f3st]

# Calculate gausscov on rolling winow
cl = makeCluster(4L)
clusterExport(cl, c("DT"), envir = environment())
clusterEvalQ(cl, library(gausscov))
clusterEvalQ(cl, library(data.table))
res = runner(
  x = DT,
  f = function(x) {
    # "20010314100000" 2001 03 14 10:00:00
    # DT[1:2045, last(date)]
    # x = DT[1:2045]
    last_date = strftime(max(x[, date], na.rm = TRUE),
                         format = "%Y%m%d%H%M%S",
                         tz = "America/New_York")
    print(last_date)
    file_name = paste0("F:/predictors/minmax/gausscov/", last_date, ".rds")
    if (file.exists(file_name)) return(NULL)
    x = as.matrix(x[, .SD, .SDcols = -c("date")])
    p0_ = 0.01
    f3st_fi_ = f3st(x[, ncol(x)], x[, -ncol(x)], m = 1, p0 = p0_)
    cov_index_f3st = f3st_fi_[[1]]
    while (length(cov_index_f3st) < 3 || inherits(cov_index_f3st, "numeric")) {
      p0_ = p0_ + 0.01
      print(p0_)
      f3st_fi_ = f3st(x[, ncol(x)], x[, -ncol(x)], m = 1, p0 = p0_)
      cov_index_f3st = f3st_fi_[[1]]
    }
    cov_index_f3st = unique(as.integer(cov_index_f3st[1, ]))[-1]
    cov_index_f3st = cov_index_f3st[cov_index_f3st != 0]
    cov_index_f3st = colnames(x[, -ncol(x)])[cov_index_f3st]
    saveRDS(cov_index_f3st, file_name)
    return(cov_index_f3st)
  },
  at = 1764:nrow(DT),
  cl = cl,
  simplify = FALSE,
  na_pad = TRUE
)
stopCluster(cl)

# Import important Minmax predictors
files = list.files("F:/predictors/minmax/gausscov", full.names = TRUE)
predictions = lapply(seq_along(files), function(i) {
  # i = 1
  file_ = files[i]
  res_ = tryCatch(readRDS(file_), error = function(e) NULL)
  if (length(res_) == 0) return(NULL)
  date_ = as.POSIXct(gsub("\\.rds|.*/", "", file_),
                     format = "%Y%m%d%H%M%S",
                     tz = "America/New_York")
  dt_ = DT[date <= date_, .SD, .SDcols = c("date", "returns", res_)]
  fromula_ = as.formula(paste0("returns ~ ", res_))
  res_ = lm(fromula_, data = as.data.frame(dt_))
  as.data.table(last(cbind.data.frame(date_, pred = predict(res_))))
})
predictions_dt = rbindlist(predictions)
setnames(predictions_dt, "date_", "date")

# Merge predictions and spy
backtest_data = predictions_dt[spy, on = "date"]
backtest_data[, pred := nafill(pred, "locf")]
backtest_data = na.omit(backtest_data)
backtest_data[, signal := shift(pred, 1) >= 0]

# Scaterplot of predictions and returns
# ggplot(backtest_data, aes(x = shift(pred), y = returns)) +
#   geom_point() +
#   geom_smooth()
# ggplot(backtest_data[pred > -0.01 & pred < 0.004], aes(x = shift(pred), y = returns)) +
#   geom_point() +
#   geom_smooth()

# Backtest
backtest_data[, strategy_returns := returns * signal]
backtest_data = na.omit(backtest_data)
charts.PerformanceSummary(as.xts.data.table(backtest_data[, .(date, returns, strategy_returns)]))


# # FINNTS ------------------------------------------------------------------
# library(finnts)
#
# # prepare historical data
# hist_data <- timetk::m4_monthly %>%
#   dplyr::rename(Date = date) %>%
#   dplyr::mutate(id = as.character(id))
#
# # call main finnts modeling function
# finn_outp33ut <- forecast_time_series(
#   input_data = hist_data,
#   combo_variables = c("id"),
#   target_variable = "value",
#   date_type = "month",
#   forecast_horizon = 3,
#   back_test_scenarios = 6,
#   models_to_run = c("arima", "ets"),
#   run_global_models = FALSE,
#   run_model_parallel = FALSE
# )


# PORTFOLIO SORT ----------------------------------------------------------
# NOT ENOUGH INFORMATION FOR PORTFOLIO SORT

# # Create month colummn
# dt[, month := data.table::yearmon(date)]
#
# # predictors columns when using OHLCV daily features
# predictors = colnames(dt)[(which(colnames(dt) == "above_p_999_4year")):(ncol(dt)-1)]
#
# # downsample to monthly frequency
# dtm = dt[, .(
#   date = last(date),
#   close = last(close),
#   volume_mean = mean(volume, na.rm = TRUE),
#   dollar_volume_mean = mean(volume * close, na.rm = TRUE)
# ), by = .(symbol, month)]
#
# # convert ohlcv daily predictors to monthly freq by moments
# setorder(dt, symbol, date)
# predictorsm = unique(dt, by = c("symbol", "month"), fromLast = TRUE)
# predictorsm = predictorsm[, .SD, .SDcols = c("symbol", "month", predictors)]
# predictorsm_sd = dt[, lapply(.SD, sd, na.rm = TRUE), .SDcols = predictors, by = c("symbol", "month")]
# setnames(predictorsm_sd, predictors, paste0(predictors, "_sd"))
# predictorsm_mean = dt[, lapply(.SD, mean, na.rm = TRUE), .SDcols = predictors, by = c("symbol", "month")]
# setnames(predictorsm_mean, predictors, paste0(predictors, "_mean"))
# predictorsm = Reduce(function(x, y) merge(x, y, by = c("symbol", "month")),
#                      list(predictorsm, predictorsm_sd, predictorsm_mean))
# cols = c("symbol", "month", predictors, paste0(predictors, c("_sd", "_mean")))
# predictorsm = predictorsm[, ..cols]
#
# # merge predictors by month and prices_m
# dtm = merge(dtm, predictorsm, by = c("symbol", "month"))
#
# # create forward returns
# dtm[, ret_forward := shift(close, -1, type = "shift") / close - 1, by = symbol]
# dtm[, .(symbol, date, month, close, ret_forward)]
#
# # check for duplicates
# dup_index = which(dtm[, duplicated(.SD[, .(symbol, month)])])
# if (length(dup_index) > 0) dtm = unique(dtm, by = c("symbol", "year_month_id"))
#
# # select cols
# predictorsm_cols = colnames(predictorsm)
# predictorsm_cols = predictorsm_cols[grepl("mean|sd", predictorsm_cols)]
# cols_ = c("symbol", "date", "month", "ret_forward", "dollar_volume_mean", predictorsm_cols)
# dtm = dtm[, ..cols_]
#
# # filter n with highest volume
# filter_var = "dollar_volume_mean" # PARAMETER
# num_coarse = 4000                  # PARAMETER
# setorderv(dtm, c("month", filter_var), order = 1L)
# dtm_filter = dtm[, tail(.SD, num_coarse), by = month]
#
# # remove observations with many NA's
# # threshold = 0.3
# # na_cols <- sapply(dtm_filter, function(x) sum(is.na(x))/length(x) > threshold)
# # print(na_cols[na_cols == TRUE])
# # print(na_cols[na_cols == FALSE])
# # predictors_cleaned = setdiff(predictors, names(na_cols[na_cols == TRUE]))
# # cols = c("symbol", "year_month_id", "ret_forward", predictors_cleaned)
# # dtm_filter = dtm_filter[, ..cols]
# dtm_filter = na.omit(dtm_filter)
#
# # remove NA values for target
# # dtm_filter = na.omit(dtm_filter, cols = "ret_forward")
#
# # winsorize extreme values
# # predictors_reduced = intersect(predictors, colnames(dtm_filter))
# # dtm_filter[, (predictors_reduced) := lapply(.SD, as.numeric), .SDcols = predictors_reduced]
# # dtm_filter[, (predictors_reduced) := lapply(.SD, function(x) DescTools::Winsorize(x, probs = c(0.01, 0.99), na.rm = TRUE)),
# #            by = year_month_id, .SDcols = predictors_reduced]
#
# # visualize some vars
# var_ = sample(predictorsm_cols, 1) # var_ = "freeCashFlowYield"
# date_ = dtm_filter[, sample(month, 1)]
# x = dtm_filter[month == date_, ..var_]
# hist(unlist(x), main=var_, xlab="dots", ylab=var_)
#
# # make function that returns results for every predictor
# dtm_filter_sample = copy(dtm_filter)
# dtm_filter_sample[, date_month := ceiling_date(as.Date(date), "month") - 1]
# dtm_filter_sample[, .(symbol, date, month, date_month)]
#
# # Function to check if more than 50% of a column's values are zeroes
# check_zero_proportion <- function(column, prop = 0.5) {
#   zero_count <- sum(column == 0, na.rm = TRUE)
#   total_count <- length(column)
#   return(zero_count / total_count > prop)
# }
#
# library(portsort)
# portsort_results_l = lapply(predictorsm_cols, function(p) {
#   # debug
#   # p = "above_p_05_year_mean"
#   print(p)
#
#   # sample only relevant data
#   cols = c("symbol", "date_month", "ret_forward", p)
#   dtm_filter_sample_ = dtm_filter_sample[, ..cols]
#
#   # predictors matrix
#   Fa = dcast(dtm_filter_sample_, date_month ~ symbol, value.var = p)
#   setorder(Fa, date_month)
#   Fa = as.xts.data.table(Fa)
#   cols_with_na <- apply(Fa, MARGIN = 2, FUN = function(x) sum(is.na(x)) > as.integer(nrow(Fa) * 0.6))
#   # dim(Fa)
#   Fa = Fa[, !cols_with_na]
#
#   # remove all NA values
#   rows_with_na <- apply(Fa, MARGIN = 1, FUN = function(x) sum(is.na(x)) > as.integer(ncol(Fa) * 0.99))
#   Fa = Fa[!rows_with_na, ]
#
#   # remove zero values
#   columns_to_remove <- apply(Fa, 2, check_zero_proportion, 0.5)
#   Fa = Fa[, !columns_to_remove]
#   dim(Fa)
#
#   # Forward returns
#   R_forward = as.xts.data.table(dcast(dtm_filter_sample_, date_month ~ symbol, value.var = "ret_forward"))
#   R_forward = R_forward[, !cols_with_na]
#   R_forward = R_forward[!rows_with_na, ]
#   R_forward = R_forward[, !columns_to_remove]
#
#   # uncondtitional sorting
#   dimA = 0:3/3
#   # dimA = c(0.1, 0.02, 0.01, 0.2, 0.5, 0.7, 0.9, 0.98, 1)
#   psort_out = tryCatch({
#     sort.output.uncon = unconditional.sort(Fa,
#                                            Fb=NULL,
#                                            Fc=NULL,
#                                            R_forward,
#                                            dimA=dimA,
#                                            dimB=NULL,
#                                            dimC=NULL,
#                                            type = 7)
#
#   }, error = function(e) NULL)
#   if (is.null(psort_out)) {
#     print(paste0("Remove variable ", p))
#     return(NULL)
#   } else {
#     return(sort.output.uncon)
#   }
#   # table.AnnualizedReturns(sort.output.uncon$returns)
# })
#
# # extract CAR an SR
# index_keep = sapply(portsort_results_l, function(x) !is.null(x))
# portsort_l <- portsort_results_l[index_keep]
# portsort_l = lapply(portsort_l, function(x) table.AnnualizedReturns(x$returns))
# portsort_l = lapply(portsort_l, function(x) as.data.table(x, keep.rownames = TRUE))
# names(portsort_l) = predictors_cleaned[index_keep]
# portsort_dt = rbindlist(portsort_l, idcol = "predictor")
#
# # save data
# fwrite(portsort_dt, "data/port_sort.csv")

