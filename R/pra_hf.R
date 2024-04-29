library(data.table)
library(arrow)
library(lubridate)
library(QuantTools)
library(PerformanceAnalytics)
library(runner)
library(future.apply)
library(doParallel)
library(Rcpp)
library(glue)



# Import Databento minute data
prices = read_parquet("F:/databento/minute_dt.parquet")
setDT(prices)

# Change timezone
attr(prices$ts_event, "tz")
prices[, ts_event := with_tz(ts_event, tz = "America/New_York")]
attr(prices$ts_event, "tz")

# Filter trading minutes
prices = prices[as.ITime(ts_event) %between% c(as.ITime("09:30:00"), as.ITime("16:00:00"))]

# Keep observations with at least one month of observations
id_n = prices[, .N, by = symbol]
remove_symbols = id_n[N < (60 * 8 * 22), unique(symbol)]
prices = prices[symbol %notin% remove_symbols]


# HF PRA ------------------------------------------------------------------
# Select coluns we need
prices = prices[, .(symbol, time = ts_event, close = close)]

# Upsample to 5 Minute
prices = prices[, .(close = last(close)), by = .(symbol, time = round_date(time, "5 mins"))]
gc()

################ TEST ################
# prices = prices[53500000:54500000]
################ TEST ################

# Calculate PRA
windows = c(90 * 5, 90 * 22, 90 * 66, 90 * 132)
cols_pra = paste0("pra_", windows)
prices[, (cols_pra) := lapply(windows, function(x) roll_percent_rank(close, x)), by = symbol]

# crate dummy variables
cols = paste0("pra_", windows)
cols_above_999 = paste0("pr_above_dummy_", windows)
prices[, (cols_above_999) := lapply(.SD, function(x) ifelse(x > 0.999, 1, 0)), .SDcols = cols]
cols_below_001 = paste0("pr_below_dummy_", windows)
prices[, (cols_below_001) := lapply(.SD, function(x) ifelse(x < 0.001, 1, 0)), .SDcols = cols]
cols_net_1 = paste0("pr_below_dummy_net_", windows)
prices[, (cols_net_1) := prices[, ..cols_above_999] - prices[, ..cols_below_001]]

cols_above_99 = paste0("pr_above_dummy_99_", windows)
prices[, (cols_above_99) := lapply(.SD, function(x) ifelse(x > 0.99, 1, 0)), .SDcols = cols]
cols_below_01 = paste0("pr_below_dummy_01_", windows)
prices[, (cols_below_01) := lapply(.SD, function(x) ifelse(x < 0.01, 1, 0)), .SDcols = cols]
cols_net_2 = paste0("pr_below_dummy_net_0199", windows)
prices[, (cols_net_2) := prices[, ..cols_above_99] - prices[, ..cols_below_01]]

cols_above_97 = paste0("pr_above_dummy_97_", windows)
prices[, (cols_above_97) := lapply(.SD, function(x) ifelse(x > 0.97, 1, 0)), .SDcols = cols]
cols_below_03 = paste0("pr_below_dummy_03_", windows)
prices[, (cols_below_03) := lapply(.SD, function(x) ifelse(x < 0.03, 1, 0)), .SDcols = cols]
cols_net_3 = paste0("pr_below_dummy_net_0397", windows)
prices[, (cols_net_3) := prices[, ..cols_above_97] - prices[, ..cols_below_03]]

cols_above_95 = paste0("pr_above_dummy_95_", windows)
prices[, (cols_above_95) := lapply(.SD, function(x) ifelse(x > 0.95, 1, 0)), .SDcols = cols]
cols_below_05 = paste0("pr_below_dummy_05_", windows)
prices[, (cols_below_05) := lapply(.SD, function(x) ifelse(x < 0.05, 1, 0)), .SDcols = cols]
cols_net_4 = paste0("pr_below_dummy_net_0595", windows)
prices[, (cols_net_4) := prices[, ..cols_above_95] - prices[, ..cols_below_05]]

# get risk measures
indicators_sum = prices[, lapply(.SD, sum, na.rm = TRUE),
                        .SDcols = c(
                          colnames(prices)[grep("pra_\\d+", colnames(prices))],
                          cols_above_999,
                          cols_above_99,
                          cols_below_001,
                          cols_below_01,
                          cols_above_97,
                          cols_below_03,
                          cols_above_95,
                          cols_below_05,
                          cols_net_1,
                          cols_net_2,
                          cols_net_3,
                          cols_net_4
                        ),
                        by = .(time)]
setnames(indicators_sum, c("time", paste0("sum_", colnames(indicators_sum)[-1])))
indicators_sd = prices[, lapply(.SD, sd, na.rm = TRUE),
                       .SDcols = c(
                         colnames(prices)[grep("pra_\\d+", colnames(prices))],
                         cols_above_999,
                         cols_above_99,
                         cols_below_001,
                         cols_below_01,
                         cols_above_97,
                         cols_below_03,
                         cols_above_95,
                         cols_below_05,
                         cols_net_1,
                         cols_net_2,
                         cols_net_3,
                         cols_net_4
                       ),
                       by = .(time)]
setnames(indicators_sd, c("time", paste0("sd_", colnames(indicators_sd)[-1])))
indicators = Reduce(function(x, y) merge(x, y, by = "time"),
                    list(indicators_sum, indicators_sd))
indicators = unique(indicators, by = c("time"))
setorder(indicators, "time")

# Merge indicators and spy
backtest_data = prices[symbol == "SPY"][indicators, on = 'time']
setorder(backtest_data, "time")
backtest_data = na.omit(backtest_data)
backtest_data[, returns := close / shift(close) - 1]
backtest_data = na.omit(backtest_data)

# Save backtest data
# fwrite(backtest_data, "F:/predictors/pra5m/backtest_data.csv")

# Read backtest_data
backtest_data = fread("F:/predictors/pra5m/backtest_data.csv")


# INSAMPLE OPTIMIZATION ---------------------------------------------------
# Optimization params
variables = colnames(backtest_data)[grepl("sum_pr_|sd_pr_", colnames(backtest_data))]
params = backtest_data[, ..variables][, lapply(.SD, quantile, probs = seq(0, 1, 0.05))]
params = melt(params)
setnames(params, c("variable", "thresholds"))
# params = merge(data.frame(sma_width=c(1, 7, 35)), params, by=NULL)

# help vectors
returns_    = backtest_data[, returns]
thresholds_ = params[, 2][[1]]
vars        = as.character(params[, 1][[1]])
# ns          = params[, 1]

# backtest vectorized
BacktestVectorized = function(returns, indicator, threshold, return_cumulative = TRUE) {
  sides <- ifelse(c(NA, head(indicator, -1)) > threshold, 0, 1)
  sides[is.na(sides)] <- 1

  returns_strategy <- returns * sides

  if (return_cumulative) {
    return(PerformanceAnalytics::Return.cumulative(returns_strategy))
  } else {
    return(returns_strategy)
  }
}

# Optimization vectorized
system.time({
  opt_results_vect_l =
    vapply(1:nrow(params), function(i)
      BacktestVectorized(returns_,
                         backtest_data[, get(vars[i])],
                         # TTR::SMA(backtest_data[, get(vars[i])], ns[i]),
                         thresholds_[i]),
      numeric(1))
})
# user  system elapsed
# 12.45    3.69   16.11

# Optimization vectorized with cpp
Rcpp::sourceCpp("backtest.cpp")
system.time({opt_results_vect_l_cpp = opt(backtest_data, params)})
# user  system elapsed
# 1.08    0.23    1.34

# compare R and cpp
all(round(opt_results_vect_l, 1) == round(opt_results_vect_l_cpp, 1))

# Inspect results
opt_results_vectorized = cbind.data.frame(params, opt_results_vect_l)
setorder(opt_results_vectorized, -opt_results_vect_l)
head(opt_results_vectorized, 30)

# Inspect individual results
strategy_returns = BacktestVectorized(returns_,
                                      backtest_data[, sd_pr_below_dummy_net_03975940],
                                      0.44,
                                      FALSE)
charts.PerformanceSummary(xts(cbind(returns_, strategy_returns), order.by = backtest_data[, time]))


# BUY / SELL WF OPTIMIZATION ----------------------------------------------
# Define params
variables_wf  = unique(as.character(head(opt_results_vectorized$variable, 100)))
# variables_wf = colnames(backtest_data)[grepl("sum_pr_.*below|sd_pr_.*below",
#                                              colnames(backtest_data))]
quantiles_wf = backtest_data[, ..variables_wf][, lapply(.SD, quantile, probs = seq(0, 1, 0.05))]
params_wf = melt(quantiles_wf)
setnames(params_wf, c("variable", "thresholds"))

# Prepare walk forward optimization
cols = c("time", "returns", variables_wf)
df = as.data.frame(backtest_data[, ..cols])
df[, 2:ncol(df)] = lapply(df[, 2:ncol(df)], function(x) as.numeric(x))
returns_wf    = df$returns
thresholds_wf = params_wf[, 2][[1]]
vars_wf       = as.character(params_wf[, 1][[1]])
# cl = makeCluster(8)
# clusterExport(
#   cl,
#   c("df", "BacktestVectorized", "params_wf", "returns_wf", "thresholds_wf", "vars_wf"),
#   envir = environment())
# windows = c(90 * 252, 90 * 252 * 2)
windows = c(90 * 252)
# clusterEvalQ(cl, {library(tsDyn)})

#
w = 90 * 252
system.time({
  bres_1 = runner(
    x = df,
    f = function(x) {
      head(cbind(
        time = tail(x$time, 1),
        params_wf,
        sr = vapply(1:nrow(params_wf), function(i) {
          BacktestVectorized(x$returns, x[, vars_wf[i]], thresholds_wf[i])
        }, numeric(1))
      ), 1)
    },
    k = w,
    at = (90 * 252):22730, # 22730 - 22680
    # cl = cl,
    na_pad = TRUE,
    simplify = FALSE
  )
})
# user  system elapsed
# 5.64    5.07   10.72
system.time({
  bres_2 = runner(
    x = df,
    f = function(x) {
      head(cbind(
        time = tail(x$time, 1),
        params_wf,
        sr = opt(x, params_wf)
      ), 1)
    },
    k = w,
    at = (90 * 252):22730, # nrow(df)
    # cl = cl,
    na_pad = TRUE,
    simplify = FALSE
  )
})
# user  system elapsed
# 1.70    0.23    1.93

length(bres_1)
length(bres_2)
all(
  vapply(seq_along(bres_1), function(i) {
    all(bres_1[[i]][, round(sr, 1)] == bres_2[[i]][, round(sr, 1)])
  }, logical(1))
)

# Rcpp runner
# Rcpp::sourceCpp("C:/Users/Mislav/Documents/GitHub/alphar/backtest.cpp")
df_test = df[1:70,]
system.time({
  runner_cpp = wfo(df_test, params_wf, 20, "rolling")
})
runner_cpp = lapply(runner_cpp, function(df_) {
  df_[, 1] = as.POSIXct(df_[, 1])
  df_
})
runner_cpp = lapply(runner_cpp, function(df_) {
  df_[, 1] = with_tz(df_[, 1], tzone = "America/New_York")
  df_
})
runner_cpp = lapply(runner_cpp, function(df_) {
  cbind.data.frame(df_, params_wf)
})
runner_cpp[[1]]
df_test[20, "time"]

# Compare results between cpp and R WFO
system.time({
  bres_1 = runner(
    x = df,
    f = function(x) {
      y = cbind.data.frame(
        time = tail(x$time, 1),
        params_wf,
        sr = vapply(1:nrow(params_wf), function(i) {
          BacktestVectorized(x$returns, x[, vars_wf[i]], thresholds_wf[i])
        }, numeric(1))
      )
      # order y dta.table by sr colun
      y = y[order(y$sr, decreasing = TRUE),]
      head(y, 1)
    },
    k = w,
    at = (90 * 252):22730, # 22730 - 22680
    # cl = cl,
    na_pad = TRUE,
    simplify = FALSE
  )
})
# user  system elapsed
# 4.20    3.67    7.87
df_test = df[1:22730,]
system.time({
  runner_cpp = wfo(df_test, params_wf, 22680, "rolling")
})
# user  system elapsed
# 0.78    0.14    0.92
length(runner_cpp) == length(bres_1)
runner_cpp = lapply(runner_cpp, function(df_) {
  df_[, 1] = as.POSIXct(df_[, 1])
  df_
})
runner_cpp = lapply(runner_cpp, function(df_) {
  df_[, 1] = with_tz(df_[, 1], tzone = "America/New_York")
  df_
})
runner_cpp = lapply(runner_cpp, function(df_) {
  cbind.data.frame(df_, params_wf)
})
runner_cpp_dt = rbindlist(runner_cpp)
runner_cpp_dt = runner_cpp_dt[, .SD[which.max(Vector)], by = Scalar]
head(runner_cpp_dt); head(rbindlist(bres_1));
tail(runner_cpp_dt); tail(rbindlist(bres_1));

# Walk forward optimization
w = 84 * 252 # number of bars per day * number of days
wfo_results = wfo(df = df,
                  params = params_wf,
                  window = w,
                  window_type = "expanding")
time_ = format.POSIXct(Sys.time(), format = '%Y%m%d%H%M%S')
file_name = glue("exuber_bestparams_all_{w}_{time_}_expanding.rds")
file_name = file.path("F:/predictors/pra5m", file_name)
# saveRDS(wfo_results, file_name)


# Continue with data cleaning
# wfo_results = readRDS("F:/predictors/pra5m/exuber_bestparams_all_22680_20240418110756.rds")
# wfo_results = readRDS("F:/predictors/pra5m/exuber_bestparams_all_22680_20240422124700_expanding.rds")
wfo_results = lapply(wfo_results, function(df_) {
  cbind.data.frame(df_, params_wf)
})
wfo_results = rbindlist(wfo_results)
setnames(wfo_results, c("time", "sr", "variable", "threshold"))
wfo_results[, time := as.POSIXct(time)]
# wfo_results[, time := with_tz(time, tzone = "America/New_York")]
wfo_results = wfo_results[, .SD[which.max(sr)], by = time]
wfo_results[, variable := as.character(variable)]

# Reshape results
cols_keep_spy = c("time", "close", wfo_results[, unique(variable)])
wfo_dt = backtest_data[, ..cols_keep_spy]
wfo_dt = melt(wfo_dt, id.vars = c("time", "close"))
setorder(wfo_dt, time)
wfo_dt = merge(wfo_dt, wfo_results, by = c("time", "variable"), all.x = TRUE, all.y = FALSE)
wfo_dt = na.omit(wfo_dt, cols = "threshold")

wfo_dt[, signal := value < shift(threshold)]
wfo_dt[, returns := close / shift(close) - 1]
wfo_dt = na.omit(wfo_dt)
wfo_dt[, strategy_returns := returns * shift(signal)]
charts.PerformanceSummary(as.xts.data.table(wfo_dt[, .(time, returns, strategy_returns)]))
