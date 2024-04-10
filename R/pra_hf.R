library(data.table)
library(arrow)
library(lubridate)
library(QuantTools)
library(PerformanceAnalytics)
library(runner)
library(future.apply)
library(doParallel)


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


# INSAMPLE OPTIMIZATION ---------------------------------------------------
# Optimization params
variables = colnames(backtest_data)[grepl("sum_pr_|sd_pr_", colnames(backtest_data))]
params = backtest_data[, ..variables][, lapply(.SD, quantile, probs = seq(0, 1, 0.05))]
params = melt(params)
params = merge(data.frame(sma_width=c(1, 7, 35)), params, by=NULL)

# help vectors
returns_    = backtest_data[, returns]
thresholds_ = params[, 3]
vars        = as.character(params[, 2])
ns          = params[, 1]

# backtest vectorized
BacktestVectorized <- function(returns, indicator, threshold, return_cumulative = TRUE) {
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
                         # backtest_data[, get(vars[i])],
                         TTR::SMA(backtest_data[, get(vars[i])], ns[i]),
                         thresholds_[i]),
      numeric(1))
})
opt_results_vectorized = cbind.data.frame(params, opt_results_vect_l)
setorder(opt_results_vectorized, -opt_results_vect_l)
head(opt_results_vectorized, 10)

# Inspect individual results
strategy_returns <- BacktestVectorized(returns_,
                                       backtest_data[, sd_pr_below_dummy_net_03975940],
                                       0.44,
                                       FALSE)
charts.PerformanceSummary(xts(cbind(returns_, strategy_returns), order.by = backtest_data[, time]))


# BUY / SELL WF OPTIMIZATION ----------------------------------------------
# Define params
variables_wf = unique(as.character(head(opt_results_vectorized$variable, 10)))
params_wf = backtest_data[, ..variables_wf][, lapply(.SD, quantile, probs = seq(0, 1, 0.05))]
params_wf = melt(params_wf)

# Prepare walk foward optimization
cols = c("time", "returns", variables_wf)
df = as.data.frame(backtest_data[, ..cols])
returns_wf    = df$returns
thresholds_wf = params_wf[, 2][[1]]
vars_wf       = as.character(params_wf[, 1][[1]])
cl = makeCluster(8)
clusterExport(
  cl,
  c("df", "BacktestVectorized", "params_wf", "returns_wf", "thresholds_wf", "vars_wf"),
  envir = environment())
windows = c(90 * 252, 90 * 252 * 2)
# clusterEvalQ(cl, {library(tsDyn)})

# Walk forward optimization
# plan("multisession", workers = 4L)
start_time = Sys.time()
best_params_window_l = lapply(windows, function(w) {
  bres = runner(
    x = df,
    f = function(x) {
      cbind(tail(x$time, 1),
            params_wf,
            vapply(1:nrow(params_wf), function(i) {
              BacktestVectorized(returns_wf, x[, vars_wf[i]], thresholds_wf[i])
            }, numeric(1)))
    },
    k = w,
    at = (90 * 252):nrow(df), # 23680
    cl = cl,
    na_pad = TRUE,
    simplify = FALSE
  )
  file_name = paste0(
    "exuber_bestparams_all_", w, "_",
    format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S"), ".rds"
  )
  file_name = file.path("F:/predictors/pra5m", file_name)
  saveRDS(bres, file_name)
})
end_time = Sys.time()
time_elapsed = end_time - start_time
stopCluster(cl)
# No parallel - Time difference of 2.205164 mins
# Parallel    - Time difference of 15.86806 secs

# Import optimmized WF data
best_params = readRDS("F:/predictors/pra5m/exuber_bestparams_all_22680_20240409110941.rds")


# Prepare data for WF optimization
var_ = "sum_pr_below_dummy_01_1980"
backtest_data_wf = backtest_data[, .(time, close, x), env = list(x = var_)]
backtest_data_wf[, returns := close / shift(close) - 1]
backtest_data_wf = na.omit(backtest_data_wf)

# Optimization params
params = melt(params)
# params = merge(data.frame(sma_width=c(1, 7, 35)), params, by=NULL)

# Help vectors
returns_    = backtest_data_wf[, returns]
thresholds_ = quantile(backtest_data_wf[, ..var_][[1]], probs = seq(0, 1, 0.05))




# CROSS VALIDATION USING C++ ----------------------------------------------
library(RCPP)

# Cpp version of above backtest function
Rcpp::cppFunction("
  double backtest_cpp(NumericVector returns, NumericVector indicator, double threshold) {
    int n = indicator.size();
    NumericVector sides(n);

    for(int i=0; i<n; i++){
      if(i==0 || R_IsNA(indicator[i-1])) {
        sides[i] = 1;
      } else if(indicator[i-1] > threshold){
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

###### POSIBLY FASTER #######
# // [[Rcpp::export]]
# NumericVector backtest_cpp(NumericVector returns, NumericVector indicator, double threshold) {
#   int n = indicator.size();
#   NumericVector sides(n, 1.0); // Default to 1
#
#   for(int i = 1; i < n; ++i) {
#     if(!NumericVector::is_na(indicator[i-1]) && indicator[i-1] > threshold) {
#       sides[i] = 0;
#     }
#   }
#
#   NumericVector returns_strategy = returns * sides;
#   NumericVector cum_returns(n);
#   cum_returns[0] = 1 + returns_strategy[0];
#   for(int i = 1; i < n; ++i) {
#     cum_returns[i] = cum_returns[i-1] * (1 + returns_strategy[i]);
#   }
#
#   return cum_returns - 1;
# }
###### POSIBLY FASTER #######

# Inspect individual results
system.time({
  backtest_cpp(returns_,
               backtest_data[, sd_pr_below_dummy_net_03975940],
               0.44)
})
system.time({
  BacktestVectorized(returns_,
                     backtest_data[, sd_pr_below_dummy_net_03975940],
                     0.44,
                     FALSE)
})

