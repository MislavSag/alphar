library(fastverse)
library(lubridate)
library(ggplot2)
library(PerformanceAnalytics)
library(QuantTools)


# DATA --------------------------------------------------------------------
# Import ETF data
etf_files = list.files("F:/data/indecies/etf/hour/download", full.names = TRUE)
prices_etf = lapply(etf_files, fread)
names(prices_etf) = gsub("-.*", "", basename(etf_files))
prices_etf = rbindlist(prices_etf, idcol = "symbol")

# Remove 0 volumes
nrow(prices_etf[volume == 0]) / nrow(prices_etf)
nrow(prices_etf[volume == 0 & close == shift(close)]) / nrow(prices_etf)
prices_etf = prices_etf[!(volume == 0 & close == shift(close))]

# Import daily data
eu_files = list.files("F:/data/equity/eu/hour/download", full.names = TRUE)
prices = lapply(eu_files, fread)
names(prices) = gsub("-.*", "", basename(eu_files))
prices = rbindlist(prices, idcol = "symbol")

# Remove 0 volumes
nrow(prices[volume == 0]) / nrow(prices)
nrow(prices[volume == 0 & close == shift(close)]) / nrow(prices)
prices = prices[!(volume == 0 & close == shift(close))]

# Import dukascopy meta
dukascopy_meta = fread("F:/data/dukascopy_meta.csv")

# Choose country
dukascopy_meta[, unique(group)]
prices = prices[symbol %in% dukascopy_meta[group == "germany", id]]

# plot some tickers
tickers_ = prices[, sample(unique(symbol), 6)]
data_plot = prices[symbol %in% tickers_, .(symbol, timestamp, close)]
data_plot = dcast(data_plot, timestamp ~ symbol, value.var = "close")
plot(as.xts.data.table(data_plot))


# PREDICTORS --------------------------------------------------------------
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
                        by = .(timestamp)]
setnames(indicators_sum, c("timestamp", paste0("sum_", colnames(indicators_sum)[-1])))
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
                       by = .(timestamp)]
setnames(indicators_sd, c("timestamp", paste0("sd_", colnames(indicators_sd)[-1])))
indicators = Reduce(function(x, y) merge(x, y, by = "timestamp"),
                    list(indicators_sum, indicators_sd))
indicators = unique(indicators, by = c("timestamp"))
setorder(indicators, "timestamp")

# ETF data
etf_symbols = prices_etf[, unique(symbol)]
etf_symbols[grepl("dax", etf_symbols)]
dax = prices_etf[symbol == "tecdaxedeeur"]
plot(as.xts.data.table(dax[, .(timestamp, close)]))
dax = dax[, .(timestamp, close, returns = close / shift(close) - 1)]

# Merge spy and indicators
backtest_data = merge(dax, indicators, by = "timestamp", all.x = TRUE, all.y = FALSE)
backtest_data = na.omit(backtest_data, cols = "returns")
setorder(backtest_data, "timestamp")


# INSAMPLE OPTIMIZATION ---------------------------------------------------
# backtest Rcpp
Rcpp::cppFunction("
  double backtest_cpp(NumericVector returns, NumericVector indicator, double threshold) {
    int n = indicator.size();
    NumericVector sides(n);

    for(int i=0; i<n; i++){
      if(i==0 || R_IsNA(indicator[i-1]) || R_IsNA(indicator[i])) {
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
      if(i==0 || R_IsNA(indicator[i-1]) || R_IsNA(indicator[i])) {
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
  # indicator = SMA(backtest_data[, get(vars[i])], ns[i])
  # threshold = thresholds_[i]
  # return_cumulative = TRUE

  # sides = ifelse(c(NA, head(indicator, -1)) > threshold, 0, 1)
  sides = ifelse(shift(indicator) > threshold, 0, 1)
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
  # indicator = SMA(backtest_data[, get(vars[i])], ns[i])
  # threshold = thresholds_[i]
  # return_cumulative = TRUE

  sides <- vector("integer", length(indicator))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(indicator[i-1])) {
      sides[i] <- 1
    } else if (indicator[i-1] > threshold) {
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


######### TODO ###########
setnafill(backtest_data, fill = 0)
######### TODO ###########

# Optimization params
predictors = setdiff(colnames(backtest_data), c("timestamp", "close", "returns"))
predictors_below = predictors[grep("below", predictors)]
params = backtest_data[, ..predictors_below]
params = params[, lapply(.SD, quantile, probs = seq(0, 1, 0.3), na.rm = TRUE)]
params = melt(params, variable.factor = FALSE)
param_sman = c(1, 5, 15, 22, 44, 66)
setnames(params, c("variable", "thresholds"))
params_expanded = params[rep(1:.N, each = length(param_sman))]
params_expanded[, new_col := rep(param_sman, times = nrow(params))]
params_expanded = unique(params_expanded)
setnames(params_expanded, c("variable", "thresholds", "sma_n"))
returns_    = backtest_data[, returns]
vars        = params_expanded[, 1][[1]]
thresholds_ = params_expanded[, 2][[1]]
ns          = params_expanded[, 3][[1]]

# Parameters
params_above_threshold = get_params(backtest_data, predictors_below)

# help vectors
returns_    = backtest_data[, returns]
vars        = params_expanded[, 1][[1]]
thresholds_ = params_expanded[, 2][[1]]
ns          = params_expanded[, 3][[1]]
vars_above       = params_above_threshold[, 1][[1]]
thresholds_above = params_above_threshold[, 2][[1]]
ns_above         = params_above_threshold[, 3][[1]]

# optimization loop
system.time({
  opt_results = vapply(1:nrow(params_expanded), function(i) {
    backtest_cpp(returns_, TTR::SMA(backtest_data[, get(vars[i])], ns[i]), thresholds_[i])
  }, numeric(1))
})
opt_results_dt = as.data.table(
  cbind.data.frame(params_expanded, cum_return = opt_results)
)
setnames(opt_results_dt, c("var", "threshold", "sma_n", "cum_return"))
setorder(opt_results_dt, -cum_return)
first(opt_results_dt, 10)
Return.cumulative(backtest_data[, returns])

# optimization loop for above threshold backtest
system.time({
  opt_results = vapply(1:nrow(params_expanded), function(i) {
    backtest_above_threshold(returns_, TTR::SMA(backtest_data[, get(vars[i])], ns[i]), thresholds_[i])
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
      backtest_vectorized(returns_, TTR::SMA(backtest_data[, get(vars[i])], ns[i]), thresholds_[i]),
      numeric(1))
})
opt_results_vect_dt = as.data.table(
  cbind.data.frame(params_expanded, cum_return = opt_results_vect)
)
setnames(opt_results_vect_dt, c("var", "threshold", "sma_n", "cum_return"))
setorder(opt_results_vect_dt, -cum_return)
first(opt_results_vect_dt, 10)

# compare R and cpp
all(round(opt_results_vect_l, 1) == round(opt_results_vect_l_cpp, 1))

# Inspect results
opt_results_vectorized = cbind.data.frame(params, opt_results_vect_l)
setorder(opt_results_vectorized, -opt_results_vect_l)
head(opt_results_vectorized, 30)

# inspect best results
best_strategy = opt_results_dt[1, ]
strategy_returns = backtest(returns_,
                            TTR::SMA(backtest_data[, .SD, .SDcols = best_strategy$var],
                                     best_strategy$sma_n),
                            best_strategy$threshold,
                            FALSE)
dt_xts = xts(cbind(returns_, strategy_returns), order.by = backtest_data[, timestamp])
charts.PerformanceSummary(dt_xts)


# RCPP VS R ---------------------------------------------------------------
# Source backtest.cpp file
sourceCpp("backtest.cpp")

# Backtest function
system.time({
  x = backtest(
    returns_,
    SMA(backtest_data[, .SD, .SDcols = best_strategy$var], best_strategy$sma_n),
    best_strategy$threshold,
    TRUE)
})
system.time({
  y = backtest_above_threshold(
    returns_,
    SMA(backtest_data[, .SD, .SDcols = best_strategy$var], best_strategy$sma_n),
    best_strategy$threshold)
})
all(round(x, 3) == round(y, 3))

# SMA function
x_= runif(100)
sma_x = SMA(x_, 50)
sma_y = calculate_sma(x_, 50)
all(sma_x == sma_y, na.rm = TRUE)

# Optimization function
params = backtest_data[, ..predictors]
params = params[, lapply(.SD, quantile, probs = seq(0, 1, 0.3), na.rm = TRUE)]
params = melt(params, variable.factor = FALSE)
param_sman = c(1, 5, 15, 22, 44, 66)
params_expanded = params[rep(1:.N, each = length(param_sman))]
params_expanded[, new_col := rep(param_sman, times = nrow(params))]
params_expanded = unique(params_expanded)
setnames(params_expanded, c("variable", "thresholds", "sma_n"))
returns_    = backtest_data[, returns]
vars        = params_expanded[, 1][[1]]
thresholds_ = params_expanded[, 2][[1]]
ns          = params_expanded[, 3][[1]]
system.time({
  x = vapply(1:nrow(params_expanded), function(i) {
    backtest_above_threshold(returns_, SMA(backtest_data[, get(vars[i])], ns[i]), thresholds_[i])
  }, numeric(1))
})
# Optimization function cpp
params_ = copy(params_expanded)
setnames(params_, c("variable", "thresholds", "sma_n"))
system.time({y = opt_with_sma(df = backtest_data, params = params_)})
length(x) == length(y)
all(round(x, 3) == round(y, 3))

# WFO approaches
windows = 7 * 22
system.time({
  x = lapply(windows, function(w) {
    bres = runner(
      x = as.data.frame(backtest_data),
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
df_ = as.data.frame(backtest_data[1:250])
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
