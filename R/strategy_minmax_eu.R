library(fastverse)
library(finutils)
library(roll)
library(PerformanceAnalytics)
library(ggplot2)
library(Rcpp)
library(TTR)
library(runner)
library(lubridate)


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


# MINMAX INDICATORS -------------------------------------------------------
# Calculate returns
setorder(prices, symbol, timestamp)
prices[, returns := close / shift(close) - 1, by = "symbol"]

# Keep only columns we need
prices = prices[, .(symbol, timestamp, close, volume, returns)]
prices = na.omit(prices)

# free memory
gc()

# calculate rolling quantiles
prices[, p_999_4year    := roll_quantile(returns, 255*7*4, p = 0.999), by = .(symbol)]
prices[, p_001_4year    := roll_quantile(returns, 255*7*4, p = 0.001), by = .(symbol)]
prices[, p_999_2year    := roll_quantile(returns, 255*7*2, p = 0.999), by = .(symbol)]
prices[, p_001_2year    := roll_quantile(returns, 255*7*2, p = 0.001), by = .(symbol)]
prices[, p_999_year     := roll_quantile(returns, 255*7, p = 0.999), by = .(symbol)]
prices[, p_001_year     := roll_quantile(returns, 255*7, p = 0.001), by = .(symbol)]
prices[, p_999_halfyear := roll_quantile(returns, 255*4, p = 0.999), by = .(symbol)]
prices[, p_001_halfyear := roll_quantile(returns, 255*4, p = 0.001), by = .(symbol)]

prices[, p_99_4year    := roll_quantile(returns, 255*7*4, p = 0.99), by = .(symbol)]
prices[, p_01_4year    := roll_quantile(returns, 255*7*4, p = 0.01), by = .(symbol)]
prices[, p_99_2year    := roll_quantile(returns, 255*7*2, p = 0.99), by = .(symbol)]
prices[, p_01_2year    := roll_quantile(returns, 255*7*2, p = 0.01), by = .(symbol)]
prices[, p_99_year     := roll_quantile(returns, 255*7, p = 0.99), by = .(symbol)]
prices[, p_01_year     := roll_quantile(returns, 255*7, p = 0.01), by = .(symbol)]
prices[, p_99_halfyear := roll_quantile(returns, 255*4, p = 0.99), by = .(symbol)]
prices[, p_01_halfyear := roll_quantile(returns, 255*4, p = 0.01), by = .(symbol)]

prices[, p_97_4year    := roll_quantile(returns, 255*7*4, p = 0.97), by = .(symbol)]
prices[, p_03_4year    := roll_quantile(returns, 255*7*4, p = 0.03), by = .(symbol)]
prices[, p_97_2year    := roll_quantile(returns, 255*7*2, p = 0.97), by = .(symbol)]
prices[, p_03_2year    := roll_quantile(returns, 255*7*2, p = 0.03), by = .(symbol)]
prices[, p_97_year     := roll_quantile(returns, 255*7, p = 0.97), by = .(symbol)]
prices[, p_03_year     := roll_quantile(returns, 255*7, p = 0.03), by = .(symbol)]
prices[, p_97_halfyear := roll_quantile(returns, 255*4, p = 0.97), by = .(symbol)]
prices[, p_03_halfyear := roll_quantile(returns, 255*4, p = 0.03), by = .(symbol)]

prices[, p_95_4year    := roll_quantile(returns, 255*7*4, p = 0.95), by = .(symbol)]
prices[, p_05_4year    := roll_quantile(returns, 255*7*4, p = 0.05), by = .(symbol)]
prices[, p_95_2year    := roll_quantile(returns, 255*7*2, p = 0.95), by = .(symbol)]
prices[, p_05_2year    := roll_quantile(returns, 255*7*2, p = 0.05), by = .(symbol)]
prices[, p_95_year     := roll_quantile(returns, 255*7, p = 0.95), by = .(symbol)]
prices[, p_05_year     := roll_quantile(returns, 255*7, p = 0.05), by = .(symbol)]
prices[, p_95_halfyear := roll_quantile(returns, 255*4, p = 0.95), by = .(symbol)]
prices[, p_05_halfyear := roll_quantile(returns, 255*4, p = 0.05), by = .(symbol)]

# save market data
# time_ = format(Sys.Date(), format = "%Y%m%d")
# file_name = file.path("F:/predictors/minmax", paste0(time_,".csv"))
# fwrite(prices, file_name)

# Extreme returns
cols = colnames(prices)[grep("^p_9", colnames(prices))]
cols_new_up = paste0("above_", cols)
prices[, (cols_new_up) := lapply(.SD, function(x) ifelse(returns > x, returns - shift(x), 0)),
       by = .(symbol), .SDcols = cols] # Shifted to remove look-ahead bias
cols = colnames(prices)[grep("^p_0", colnames(prices))]
cols_new_down = paste0("below_", cols)
prices[, (cols_new_down) := lapply(.SD, function(x) ifelse(returns < x, abs(returns - shift(x)), 0)),
       by = .(symbol), .SDcols = cols]


# SYSTEMIC RISK -----------------------------------------------------------
# help function to calcualte tail risk measures from panel
tail_risk = function(dt, FUN = mean, cols_prefix = "mean_") {
  # dt = copy(prices)
  dt_ = copy(dt)
  cols = colnames(dt_)[grep("below_p|above_p", colnames(dt))]
  indicators_ = dt_[, lapply(.SD, function(x) f(x, na.rm = TRUE)),
                    by = .(timestamp), .SDcols = cols,
                    env = list(f = FUN)]
  colnames(indicators_) = c("timestamp", paste0(cols_prefix, cols))
  setorder(indicators_, timestamp)
  above_sum_cols = colnames(indicators_)[grep("above", colnames(indicators_))]
  below_sum_cols = colnames(indicators_)[grep("below", colnames(indicators_))]
  excess_sum_cols = gsub("above", "excess", above_sum_cols)
  indicators_[, (excess_sum_cols) := indicators_[, ..above_sum_cols] - indicators_[, ..below_sum_cols]]
}

# Get tail risk mesures
indicators_mean      = tail_risk(prices, FUN = "mean", cols_prefix = "mean_")
indicators_sd        = tail_risk(prices, FUN = "sd", cols_prefix = "sd_")
indicators_sum       = tail_risk(prices, FUN = "sum", cols_prefix = "sum_")
indicators_skewness  = tail_risk(prices, FUN = "skewness", cols_prefix = "skewness_")
indicators_kurtosis  = tail_risk(prices, FUN = "kurtosis", cols_prefix = "kurtosis_")

# Merge indicators and spy
indicators = Reduce(function(x, y) merge(x, y, by = "timestamp", all.x = TRUE, all.y = FALSE),
                    list(indicators_mean, indicators_sd, indicators_sum,
                         indicators_skewness, indicators_kurtosis))

# Plot some
plot(as.xts.data.table(indicators[, .(timestamp, sd_excess_p_999_halfyear)]))

# Save indicators
# fwrite(indicators, "F:/predictors/minmax/indicators.csv")


# BACKTEST DATA -----------------------------------------------------------
# ETF data
etf_symbols = prices_etf[, unique(symbol)]
etf_symbols[grepl("dax", etf_symbols)]
dax = prices_etf[symbol == "tecdaxedeeur"]
plot(as.xts.data.table(dax[, .(timestamp, close)]))
dax = dax[, .(timestamp, close, returns = close / shift(close) - 1)]

# Merge spy and indicators
sysrisk = merge(dax, indicators, by = "timestamp", all.x = TRUE, all.y = FALSE)
sysrisk = na.omit(sysrisk, cols = "returns")

# Visualize minmax variables
ggplot(sysrisk, aes(x = timestamp)) +
  geom_line(aes(y = pmax(pmin(mean_excess_p_999_2year, 0.01), -0.01)))
ggplot(sysrisk, aes(x = timestamp)) +
  geom_line(aes(y = pmax(pmin(sd_excess_p_999_2year, 0.1), -0.1)))
ggplot(sysrisk, aes(x = timestamp)) +
  geom_line(aes(y = pmax(pmin(sum_excess_p_999_2year, 1), -1)))
ggplot(sysrisk, aes(x = timestamp)) +
  geom_line(aes(y = pmax(pmin(skewness_excess_p_999_2year, 0.001), -0.001)))

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
data_plot = sysrisk[, .(timestamp, returns, alpha = shift(mean_excess_p_999_4year < -0.001))]
na.omit(data_plot)[, mean(returns), by = .(alpha)] |>
  ggplot(aes(x = alpha, y = V1)) +
  geom_bar(stat = "identity")

# Choose columns
# 1) Choose subset of columns
cols = c("timestamp", "close", "returns",
         colnames(indicators)[grepl("sum_ex", colnames(indicators))])
backtest_dt = sysrisk[, ..cols]
# 2) choose all columns
backtest_dt = copy(sysrisk)

# Remove columns with many NA values
cols_keep = colnames(backtest_dt)[sapply(backtest_dt, function(x) sum(is.na(x))/length(x) < 0.5)]
backtest_dt = backtest_dt[, ..cols_keep]

# Remove NA values
backtest_dt = na.omit(backtest_dt)

# Define predictors
predictors = setdiff(colnames(backtest_dt), c("timestamp", "close", "returns"))
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
    backtest_cpp(returns_, TTR::SMA(backtest_dt[, get(vars[i])], ns[i]), thresholds_[i])
  }, numeric(1))
})
opt_results_dt = as.data.table(
  cbind.data.frame(params_expanded, cum_return = opt_results)
)
setnames(opt_results_dt, c("var", "threshold", "sma_n", "cum_return"))
setorder(opt_results_dt, -cum_return)
first(opt_results_dt, 10)
Return.cumulative(backtest_dt[, returns])

# optimization loop for above threshold backtest
system.time({
  opt_results = vapply(1:nrow(params_expanded), function(i) {
    backtest_above_threshold(returns_, TTR::SMA(backtest_dt[, get(vars[i])], ns[i]), thresholds_[i])
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
      backtest_vectorized(returns_, TTR::SMA(backtest_dt[, get(vars[i])], ns[i]), thresholds_[i]),
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
vars_results = opt_results_vect_dt[, .(var_mean = mean(cum_return)), by = var]
vars_results[, var_agg := gsub("_.*", "", var)]
vars_results[, mean(var_mean), by = var_agg]

# inspect best results
best_strategy = opt_results_dt[1, ]
strategy_returns = backtest(returns_,
                            TTR::SMA(backtest_dt[, .SD, .SDcols = best_strategy$var],
                                best_strategy$sma_n),
                            best_strategy$threshold,
                            FALSE)
dt_xts = xts(cbind(returns_, strategy_returns), order.by = backtest_dt[, timestamp])
charts.PerformanceSummary(dt_xts)

# Results across lags
dt_ = as.data.table(opt_results_dt)
dt_[, .(mean = mean(cum_return),
        median = median(cum_return)), by = sma_n]

# Optimization for above threshold
opt_results_above = vapply(1:nrow(params_above_threshold), function(i) {
  backtest_cpp(returns_,
               TTR::SMA(backtest_dt[, get(vars_above[i])], ns_above[i]),
               thresholds_above[i])
}, numeric(1))
opt_results_above_dt = as.data.table(
  cbind.data.frame(params_above_threshold, cum_return = opt_results_above)
)
setnames(opt_results_above_dt, c("var", "threshold", "sma_n", "cum_return"))
setorder(opt_results_above_dt, -cum_return)
first(opt_results_above_dt, 10)

# inspect best results
best_strategy = opt_results_above_dt[1, ]
strategy_returns = backtest(returns_,
                            TTR::SMA(backtest_dt[, .SD, .SDcols = best_strategy$var],
                                     best_strategy$sma_n),
                            best_strategy$threshold,
                            FALSE)
dt_xts = xts(cbind(returns_, strategy_returns), order.by = backtest_dt[, timestamp])
charts.PerformanceSummary(dt_xts)


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
predictors_ = c(predictors_sd)
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
calcualte_and_save_wfo = function(window,
                                  path = "D:/strategies/eu_minmax") {
  # window = 7 * 22 * 6
  wfo_results_ = wfo(df, params_expanded, window, "rolling")
  wfo_results_ = clean_wfo(wfo_results_)
  fwrite(wfo_results_, file.path(path, glue::glue("wfo_results_{window}.csv")))
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
PATH = "D:/strategies/eu_minmax"
list.files(PATH)
wfo_results = fread(file.path(PATH, "wfo_results_154.csv"))
setnames(wfo_results, c("date", "return", "variable", "thresholds", "n"))
setorder(wfo_results, date, return)

# Keep only sd
# wfo_results = wfo_results[variable %like% "sd_"]

# Keep best
wfo_results_best_n = wfo_results[, first(.SD, 50), by = date]
wfo_results_best_n[, unique(variable)]

# Merge results with backtest data, that is price data
backtest_long = melt(backtest_dt, id.vars = c("timestamp", "returns"))
backtest_long = merge(backtest_long, wfo_results_best_n,
                      by.x = c("timestamp", "variable"),
                      by.y = c("date", "variable"),
                      all.x = TRUE, all.y = FALSE)
backtest_long = na.omit(backtest_long, cols = "return")
backtest_long[, signal := value < thresholds]
backtest_long[, signal_ensamble := sum(signal) > 20, by = timestamp]
backtest_long = unique(backtest_long[, .(timestamp, returns, signal = signal_ensamble)])

# Backtest
backtest_xts = backtest_long[, .(timestamp, benchmark = returns, signal = shift(signal))]
backtest_xts[, strategy := benchmark * signal]
backtest_xts = as.xts.data.table(na.omit(backtest_xts))
charts.PerformanceSummary(backtest_xts)
