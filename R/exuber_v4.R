library(data.table)
library(arrow)
library(duckdb)
library(lubridate)
library(PerformanceAnalytics)
library(TTR)
library(future.apply)
library(Rcpp)
library(runner)
library(glue)
library(tsDyn)
library(AzureStor)
library(gausscov)
library(readr)
# library(vars)
# library(ggplot2)


# SET UP ------------------------------------------------------------------
# globals
QCDATA  = "F:/lean/data"
DATA    = "F:/data"
RESULTS = "F:/strategies/Exuber"

# parameters
exuber_window = c(600)
n_most_liquid = 100


# UNIVERSE ----------------------------------------------------------------
# Import daily data
col = c("date", "open", "high", "low", "close", "volume", "close_adj", "symbol")
ohlcvd = fread(file.path(QCDATA, "stocks_daily.csv"), col.names = col)
ohlcvd = unique(ohlcvd, by = c("symbol", "date"))
unadjustd_cols = c("open", "high", "low")
ohlcvd[, (unadjustd_cols) := lapply(.SD, function(x) (close_adj / close) * x),
       .SDcols = unadjustd_cols]
ohlcvd = na.omit(ohlcvd)
setorder(ohlcvd, symbol, date)
setnames(ohlcvd, c("close", "close_adj"), c("close_raw", "close"))
ohlcvd = ohlcvd[open > 0.00003 & high > 0.00003 & low > 0.00003 & close > 0.00003]
ohlcvd[, dollar_volume := close_raw * volume]

# Most liquid
most_liquid = ohlcvd[, .(dollar_volume = sum(dollar_volume, na.rm = TRUE)),
                     by = .(month = data.table::yearmon(date), symbol)]
most_liquid[, rank := frank(-dollar_volume, ties.method = "first"), by = month]
setorder(most_liquid, month, -dollar_volume)
most_liquid = most_liquid[rank < n_most_liquid]


# DATA --------------------------------------------------------------------
# Exuber predictors
files = list.files(file.path(DATA, "equity", "us", "predictors_hour", "exuber"),
                   full.names = TRUE)
symbols_ = gsub(".*/|\\.parquet", "", files)
exuber_dt = lapply(files, read_parquet)
names(exuber_dt) = symbols_
exuber_dt = rbindlist(exuber_dt, idcol = "symbol")

# Filter exuber data
exuber_dt = exuber_dt[symbol %in% most_liquid[, unique(symbol)]]

# Import hour data for universe symbols
con = dbConnect(duckdb::duckdb())
path_ = file.path(QCDATA, "stocks_hour.csv")
symbols_string = paste(sprintf("'%s'", most_liquid[, unique(symbol)]), collapse=", ")
query = sprintf("
  SELECT *
  FROM '%s'
  WHERE Symbol IN (%s)
", path_, symbols_string)
ohlcvh = dbGetQuery(con, query)
dbDisconnect(con)
setDT(ohlcvh)
col = c("date", "open", "high", "low", "close", "volume", "close_adj", "symbol")
setnames(ohlcvh, col)
ohlcvh = unique(ohlcvh, by = c("symbol", "date"))
unadjustd_cols = c("open", "high", "low")
ohlcvh[, (unadjustd_cols) := lapply(.SD, function(x) (close_adj / close) * x),
       .SDcols = unadjustd_cols]
ohlcvh = na.omit(ohlcvh)
setorder(ohlcvh, symbol, date)
setnames(ohlcvh, c("close", "close_adj"), c("close_raw", "close"))
ohlcvh = ohlcvh[open > 0.00003 & high > 0.00003 & low > 0.00003 & close > 0.00003]
ohlcvh[, dollar_volume := close_raw * volume]
ohlcvh[, date := force_tz(date, "America/New_York")]


# PREPARE DATA ------------------------------------------------------------
# Merge market data and exuber data
dt = merge(exuber_dt, ohlcvh, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE)

# Free memory
rm(list = c("exuber_dt", "ohlcvh", "ohlcvd", "most_liquid"))
gc()

# Calculate returns
setorder(dt, symbol, date)
dt[, returns := close / shift(close) - 1, by = symbol]

# Remove missing values
dt = na.omit(dt)

# Add first layer of  aggregation
dt[, radf_sum_600 := exuber_600_1_adf_log + exuber_600_1_sadf_log +
     exuber_600_1_gsadf_log + exuber_600_1_badf_log + exuber_600_1_bsadf_log]
dt[, radf_sum_100 := exuber_100_1_adf_log + exuber_100_1_sadf_log +
     exuber_100_1_gsadf_log + exuber_100_1_badf_log + exuber_100_1_bsadf_log]

# Remove columns we don't need
cols_remove = c("open", "high", "low", "close_raw", "dollar_volume", "volume")
dt = dt[, .SD, .SDcols = -cols_remove]
exuber_cols = colnames(dt)[grep("exuber|radf", colnames(dt))]
setcolorder(dt, c("symbol", "date", "close", "returns", exuber_cols))

# Visualize adf and gsadf corr
unique(dt$symbol)
cols = c("date", "exuber_600_1_adf_log", "exuber_600_1_gsadf_log")
plot(as.xts.data.table(dt[symbol == "spy", ..cols]))


# AGGREGATE PREDICTORS ----------------------------------------------------
# Median aggreagation
radf_vars = colnames(dt)[5:ncol(dt)]
indicators_median = dt[, lapply(.SD, median, na.rm = TRUE), by = c('date'), .SDcols = radf_vars]
setnames(indicators_median, radf_vars, paste0("median_", radf_vars))

# Standard deviation aggregation
indicators_sd = dt[, lapply(.SD, sd, na.rm = TRUE), by = c('date'), .SDcols = radf_vars]
setnames(indicators_sd, radf_vars, paste0("sd_", radf_vars))

# Mean aggregation
indicators_mean = dt[, lapply(.SD, mean, na.rm = TRUE), by = c('date'), .SDcols = radf_vars]
setnames(indicators_mean, radf_vars, paste0("mean_", radf_vars))

# Sum aggreagation
indicators_sum = dt[, lapply(.SD, sum, na.rm = TRUE), by = c('date'), .SDcols = radf_vars]
setnames(indicators_sum, radf_vars, paste0("sum_", radf_vars))

# Quantile aggregations
indicators_q = dt[, lapply(.SD, quantile, probs = 0.99, na.rm = TRUE), by = c('date'), .SDcols = radf_vars]
setnames(indicators_q, radf_vars, paste0("q99_", radf_vars))
indicators_q97 = dt[, lapply(.SD, quantile, probs = 0.97, na.rm = TRUE), by = c('date'), .SDcols = radf_vars]
setnames(indicators_q97, radf_vars, paste0("q97_", radf_vars))
indicators_q95 = dt[, lapply(.SD, quantile, probs = 0.95, na.rm = TRUE), by = c('date'), .SDcols = radf_vars]
setnames(indicators_q95, radf_vars, paste0("q95_", radf_vars))

# Skew aggregation
indicators_skew = dt[, lapply(.SD, skewness, na.rm = TRUE), by = c('date'), .SDcols = radf_vars]
setnames(indicators_skew, radf_vars, paste0("skew_", radf_vars))

# Kurtosis aggregation
indicators_kurt = dt[, lapply(.SD, kurtosis, na.rm = TRUE), by = c('date'), .SDcols = radf_vars]
setnames(indicators_kurt, radf_vars, paste0("kurtosis_", radf_vars))

# Merge indicators
indicators = Reduce(
  function(x, y)
    merge(
      x,
      y,
      by = "date",
      all.x = TRUE,
      all.y = TRUE
    ),
  list(
    indicators_sd,
    indicators_median,
    indicators_sum,
    indicators_mean,
    indicators_q,
    indicators_q97,
    indicators_q95,
    indicators_skew,
    indicators_kurt
  )
)
setorder(indicators, date)

# Remove missing values
indicators = na.omit(indicators)

# visualize
plot(as.xts.data.table(indicators[, .(date, sd_radf_sum_100)])) # sd
plot(as.xts.data.table(indicators[, .(date, median_radf_sum_600)])) # median
plot(as.xts.data.table(indicators[, .(date, q97_radf_sum_600)]))    # q97
plot(as.xts.data.table(indicators[, .(date, q99_radf_sum_600)]))    # q99
plot(as.xts.data.table(indicators[, .(date, skew_radf_sum_600)]))   # skew
plot(as.xts.data.table(indicators[, .(date, kurtosis_radf_sum_600)])) # kurtosis

# Merge SPY and indicators
spy = dt[symbol == "spy", .(date, close, returns)]
backtest_data = indicators[spy, on = "date"]

# Lag all indicators
cols_exuber = colnames(backtest_data)[grep("exuber|radf", colnames(backtest_data))]
backtest_data[, (cols_exuber) := shift(.SD), .SDcols = cols_exuber]

# Remove missing values
backtest_data = na.omit(backtest_data)


# INSAMPLE ONE THRESHOLD OPTIMIZATION -------------------------------------
# backtest function - buy if above threshold and sell if below threshold
Backtest = function(returns, indicator, threshold, return_cumulative = TRUE) {
  sides = vector("integer", length(indicator))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(indicator[i-1])) {
      sides[i] = NA
    } else if (indicator[i-1] > threshold) {
      sides[i] = 0
    } else {
      sides[i] = 1
    }
  }
  sides = ifelse(is.na(sides), 1, sides)
  returns_strategy = returns * sides
  if (return_cumulative) {
    return(PerformanceAnalytics::Return.cumulative(returns_strategy))
  } else {
    return(returns_strategy)
  }
}

# backtest function - same as above but revrse signals
BacktestRev = function(returns, indicator, threshold, return_cumulative = TRUE) {
  sides = vector("integer", length(indicator))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(indicator[i-1])) {
      sides[i] = NA
    } else if (indicator[i-1] < threshold) {
      sides[i] = 0
    } else {
      sides[i] = 1
    }
  }
  sides = ifelse(is.na(sides), 1, sides)
  returns_strategy = returns * sides
  if (return_cumulative) {
    return(PerformanceAnalytics::Return.cumulative(returns_strategy))
  } else {
    return(returns_strategy)
  }
}

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

# Cpp version of above BacktestRev function
Rcpp::cppFunction("
  double backtest_cpp_rev(NumericVector returns, NumericVector indicator, double threshold) {
    int n = indicator.size();
    NumericVector sides(n);

    for(int i=0; i<n; i++){
      if(i==0 || R_IsNA(indicator[i-1])) {
        sides[i] = 1;
      } else if(indicator[i-1] < threshold){
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

# backtest vectorized rev
BacktestVectorizedRev <- function(returns, indicator, threshold, return_cumulative = TRUE) {
  sides <- ifelse(c(NA, head(indicator, -1)) < threshold, 0, 1)
  sides[is.na(sides)] <- 1

  returns_strategy <- returns * sides

  if (return_cumulative) {
    return(PerformanceAnalytics::Return.cumulative(returns_strategy))
  } else {
    return(returns_strategy)
  }
}

# backtest performance
Performance <- function(x) {
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

# optimization params
variables = colnames(indicators)[2:length(colnames(indicators))]
params = indicators[, ..variables][, lapply(.SD, quantile, probs = seq(0, 1, 0.02))]
params = melt(params)
params = merge(data.frame(sma_width=c(1, 7, 35)), params, by=NULL)

# help vectors
returns_    = backtest_data[, returns]
thresholds_ = params[, 3]
vars        = as.vector(params[, 2])
ns          = params[, 1]

# optimization loop
system.time({
  opt_results_l =
    vapply(1:nrow(params), function(i)
      backtest_cpp(returns_,
                   SMA(backtest_data[, get(vars[i])], ns[i]),
                   thresholds_[i]),
      numeric(1))
})
# user  system elapsed
# 83.78    3.53   87.28
opt_results = cbind.data.frame(params, opt_results_l)
opt_results = opt_results[order(opt_results$opt_results_l), ]

# optimization loop vectorized
system.time({
  opt_results_vect_l =
    vapply(1:nrow(params), function(i)
      BacktestVectorized(returns_,
                         SMA(backtest_data[, get(vars[i])], ns[i]),
                         thresholds_[i]),
      numeric(1))
})
# user  system elapsed
# 123.57    9.92  133.53
opt_results_vectorized = cbind.data.frame(params, opt_results_vect_l)
opt_results_vectorized = opt_results_vectorized[order(opt_results_vectorized$opt_results_vect_l), ]

# optimization loop rev
system.time({
  opt_results_rev_l =
    vapply(1:nrow(params), function(i)
      backtest_cpp_rev(returns_,
                       SMA(backtest_data[, get(vars[i])], ns[i]),
                       thresholds_[i]),
      numeric(1))
})
# user  system elapsed
# 83.68    0.58   84.26
opt_results_rev = cbind.data.frame(params, opt_results_rev_l)
opt_results_rev = opt_results_rev[order(opt_results_rev$opt_results_rev_l), ]

# optimization loop with rev and default
system.time({
  opt_results_comb_l =
    vapply(1:nrow(params), function(i) {
      if (grepl("kurtosis", vars[i])) {
        backtest_cpp_rev(returns_,
                         SMA(backtest_data[, get(vars[i])], ns[i]),
                         thresholds_[i])
      } else {
        backtest_cpp(returns_,
                     SMA(backtest_data[, get(vars[i])], ns[i]),
                     thresholds_[i])
      }
    }, numeric(1))
})
# user  system elapsed
# 87.08    0.71   87.72
opt_results_comb = cbind.data.frame(params, opt_results_comb_l)
opt_results_comb = opt_results_comb[order(opt_results_comb$opt_results_comb_l), ]

# Same!
tail(opt_results, 40)            # best results
tail(opt_results_vectorized, 40) # best results
tail(opt_results_rev, 40)        # best results
tail(opt_results_comb, 40)       # best results

# check individual variables
tail(opt_results[grepl("kurtosis", opt_results$variable), ]) # best results
tail(opt_results_rev[grepl("kurtosis", opt_results_rev$variable), ]) # best results
tail(opt_results[grepl("skew", opt_results$variable), ]) # best results
tail(opt_results_rev[grepl("skew", opt_results_rev$variable), ]) # best results
tail(opt_results[grepl("mean", opt_results$variable), ]) # best results
tail(opt_results_rev[grepl("mean", opt_results_rev$variable), ]) # best results
tail(opt_results[grepl("q\\d+", opt_results$variable), ]) # best results
tail(opt_results_rev[grepl("q\\d+", opt_results_rev$variable), ]) # best results
tail(opt_results[grepl("sd_", opt_results$variable), ]) # best results
tail(opt_results_rev[grepl("sd_", opt_results_rev$variable), ]) # best results

# inspect results
strategy_returns = Backtest(returns_, backtest_data[, median_exuber_600_1_gsadf_log], 4.25, FALSE)
charts.PerformanceSummary(xts(cbind(returns_, strategy_returns), order.by = backtest_data[, date]))
strategy_returns_rev = BacktestRev(returns_, backtest_data[, skew_exuber_100_1_gsadf_log], 0.42, FALSE)
charts.PerformanceSummary(xts(cbind(returns_, strategy_returns_rev), order.by = backtest_data[, date]))

# Optimization with optim package (DIFFENRET RESULTS WITH OPTIM)
optimize_strategy = function(threshold) {
  ret = Backtest(returns_, backtest_data[, sd_exuber_300_1_bsadf_log ], threshold)
  return(-ret)
}
system.time({
  result_optim = optim(0, optimize_strategy, method="Brent", lower=-3, upper=3)
})
result_optim
strategy_returns <- Backtest(returns_,
                             backtest_data[, sd_exuber_300_1_bsadf_log],
                             result_optim$par,
                             FALSE)
charts.PerformanceSummary(xts(cbind(returns_, strategy_returns), order.by = backtest_data[, date]))

# SEEMS GOOD!


# COMPARE OPTIMIZATION FUNTCIONS ------------------------------------------
# compare resulsts and speed
system.time({
  backtest_for_loop = Backtest(returns_,
                               backtest_data[, sd_exuber_300_1_bsadf_log  ],
                               0.63,
                               FALSE)
})
system.time({
  backtest_vectorized = BacktestVectorized(returns_,
                                           backtest_data[, sd_exuber_300_1_bsadf_log],
                                           0.63,
                                           FALSE)
})
all(backtest_for_loop == backtest_vectorized)

# Compare optimize_strategy and opt_results_l
plan("multisession", workers = 8L)
optim_res = future_lapply(variables, function(var_) {
  print(var_)
  optimize_strategy_ = function(threshold) {
    ret = Backtest(returns_, backtest_data[, .SD, .SDcols = var_][[1]], threshold)
    return(-ret)
  }
  result_optim = optim(0, optimize_strategy_, method="Brent", lower=-10, upper=10)
  cbind.data.frame(variable = var_, threshold = result_optim$par, res = -result_optim$value)
})
i = 8
optim_res[[i]]
opt_results[opt_results$variable == optim_res[[i]][1][[1]] & opt_results$sma_width == 1, ]
head(opt_results)


# RCPP VS R ---------------------------------------------------------------
# Source backtest.cpp file
sourceCpp("backtest.cpp")

# Backtest function
system.time({
  x = Backtest(returns_, backtest_data[, median_exuber_600_1_gsadf_log], 4.25, TRUE)
})
system.time({
  y = backtest_cpp_gpt(returns_, backtest_data[, median_exuber_600_1_gsadf_log], 4.25)
})
all(round(x, 3) == round(y, 3))

# Optimization function
variables = colnames(indicators)[2:length(colnames(indicators))]
params = indicators[, ..variables][, lapply(.SD, quantile, probs = seq(0, 1, 0.02))]
params = melt(params, variable.factor = FALSE)
returns_    = backtest_data[, returns]
thresholds_ = params[, 2][[1]]
vars        = params[, 1][[1]]
system.time({
  x = vapply(1:nrow(params), function(i)
    BacktestVectorized(returns_, backtest_data[, get(vars[i])], thresholds_[i]),
    numeric(1))
})
params_ = copy(params)
setnames(params_, c("variable", "thresholds"))
system.time({y = opt(df = backtest_data, params = params_)})
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
          BacktestVectorized(x$returns, x[, params_[i, variable]], params_[i, thresholds])
        }, numeric(1))

        returns_strategies = cbind.data.frame(params, ret)
        returns_strategies[order(returns_strategies$ret, decreasing = TRUE), ]
        # head(returns_strategies[order(returns_strategies$ret, decreasing = TRUE), ], 100)
      },
      k = w,
      at = 154:250,
      na_pad = TRUE,
      simplify = FALSE
    )
  })
})
# user  system elapsed
# 35.43    0.69   36.36
df_ = as.data.frame(backtest_data[1:250])
colnames(df_)[1] = "time"
system.time({y = wfo(df_, params_, windows, "rolling")})
# user  system elapsed
# 3.72    0.04    3.71
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


# WFO ---------------------------------------------------------------------
# Optimization params
variables = colnames(indicators)[2:length(colnames(indicators))]
variables = variables[!grepl("skew", variables)]
params = indicators[, ..variables][, lapply(.SD, quantile, probs = seq(0, 1, 0.05))]
params = melt(params, variable.factor = FALSE)
setnames(params, c("variable", "thresholds"))

# WFO utils
clean_wfo = function(y) {
  y = lapply(y, function(df_) {
    df_[, 1] = as.POSIXct(df_[, 1])
    df_[, 1] = with_tz(df_[, 1], tzone = "America/New_York")
    cbind.data.frame(df_, params)
  })
  y = rbindlist(y)
  return(y)
}
calcualte_and_save_wfo = function(window) {
  # window = 7 * 22 * 6
  wfo_results_ = wfo(df, params, window, "rolling")
  wfo_results_ = clean_wfo(wfo_results_)
  fwrite(wfo_results_, file.path(RESULTS, glue("wfo_results_{window}.csv")))
  return(0)
}

# Walk forward optimization
df = as.data.frame(backtest_data)
colnames(df)[1] = "time"
# calcualte_and_save_wfo(7 * 22 * 1)
# calcualte_and_save_wfo(7 * 22 * 6)
# calcualte_and_save_wfo(7 * 22 * 12)
# calcualte_and_save_wfo(7 * 22 * 18) # never finished try before go home

# Import results
list.files(RESULTS)
wfo_results = fread(file.path(RESULTS, "wfo_results_1848.csv"))
setnames(wfo_results, c("date", "return", "variable", "thresholds"))
setorder(wfo_results, date, return)

# Keep only sd
# wfo_results = wfo_results[variable %like% "sd_"]

# Keep best
wfo_results_best_n = wfo_results[, first(.SD, 50), by = date]
wfo_results_best_n[, unique(variable)]

# Merge results with backtest data, that is price data
backtest_long = melt(backtest_data, id.vars = c("date", "returns"))
backtest_long = merge(backtest_long, wfo_results_best_n, by = c("date", "variable"),
                      all.x = TRUE, all.y = FALSE)
backtest_long = na.omit(backtest_long, cols = "return")
backtest_long[, signal := value > thresholds]
backtest_long[, signal_ensamble := sum(signal) > 20, by = date]
backtest_long = unique(backtest_long[, .(date, returns, signal = signal_ensamble)])

# Backtest
backtest_xts = backtest_long[, .(date, benchmark = returns, signal = shift(signal))]
backtest_xts[, strategy := benchmark * signal]
backtest_xts = as.xts.data.table(na.omit(backtest_xts))
charts.PerformanceSummary(backtest_xts)


# ROLLING GAUSSCOV --------------------------------------------------------
# Prepare data
dt = copy(backtest_data)
dt[, y := shift(close, n = 1, type = "lead") / close - 1]
dt = na.omit(dt)
cols_predictors = setdiff(colnames(dt), c("date", "y", "close", "returns"))
cols_predictors_new = paste0("diff_", cols_predictors)
dt[, (cols_predictors_new) := lapply(.SD, function(x) c(NA, diff(x))), .SDcols = cols_predictors]
cols_predictors_pct = paste0("pct_", cols_predictors)
dt[, (cols_predictors_pct) := lapply(.SD, function(x) x / shift(x) - 1), .SDcols = cols_predictors]
cols_predictors_all = c(cols_predictors, cols_predictors_new, cols_predictors_pct)
dt = na.omit(dt)

# Check gausscov functions
X = as.matrix(dt[, .SD, .SDcols = -c("date", "y", "close", "returns")])
y = as.matrix(dt[, .(y)])
system.time({f1st_res = f1st(y = y, x = X, p0 = 0.01)})
f1st_res
colnames(dt)[f1st_res[[1]][, 1]]
# system.time({f3st_res = f3st(y = y, x = X, m = 2, p = 0.01)}) # TOO SLOW
# system.time({f3st_res = f3st(y = y[1:1000, ], x = X[1:1000, ], m = 2, p = 0.01)}) # TOO SLOW

# Expanding f1st
system.time({
  f1st_res_l = runner(
    x = dt,
    f = function(x) {
      cbind.data.frame(
        date = x[, last(date)],
        f1st(y = as.matrix(x[, .(y)]),
             x = as.matrix(x[, .SD, .SDcols = -c("date", "y", "close", "returns")]),
             p0 = 0.6)[[1]]
      )
    },
    at = (7 * 22 * 6):nrow(dt),
    na_pad = TRUE,
    simplify = FALSE
  )
})
# saveRDS(f1st_res_l, file.path(RESULTS, glue("f1st_res_l.rds")))

# Rolling f1st
system.time({
  f1st_res_l_roll = runner(
    x = dt,
    f = function(x) {
      cbind.data.frame(
        date = x[, last(date)],
        f1st(y = as.matrix(x[, .(y)]),
             x = as.matrix(x[, .SD, .SDcols = -c("date", "y", "close", "returns")]),
             p0 = 0.1)[[1]]
      )
    },
    k = 7 * 22 * 36,
    na_pad = TRUE,
    simplify = FALSE
  )
})
# saveRDS(f1st_res_l_roll, file.path(RESULTS, glue("f1st_res_l_roll.rds")))

# Import results
file_ = file.path(RESULTS, glue("f1st_res_l_roll.rds"))
f1st_res_l = readRDS(file_)
lengths(f1st_res_l)
f1st_res_l = f1st_res_l[lengths(f1st_res_l) > 1]
last(lengths(f1st_res_l), 100)
f1st_res_dt = rbindlist(f1st_res_l)
setnames(f1st_res_dt, c("date", "col_ind", "reg_coeff", "p", "standard_p"))
columns_dt = data.table(col_ind = seq_along(colnames(dt)), cols = colnames(dt))
f1st_res_dt = columns_dt[f1st_res_dt, on = "col_ind"]

# Calculate rolling regressions
dates = f1st_res_dt[, unique(date)]
min_date = dt[, min(date)]
predictions = vector("numeric", length(dates))
for (i in seq_along(dates)) {
  # i = 1000
  sample_dt = dt[date %between% c(min_date, dates[i]),
                 .SD,
                 .SDcols = c("y", na.omit(f1st_res_dt[date == dates[i]]$cols))]
  setDF(sample_dt)
  predictions[i] = last(predict(lm(sample_dt, formula = y ~ .)))
}

# Merge predictions and dates and than predictions and dt
predictions_dt = data.table(date = dates, predictions = predictions)
predictions_dt = merge(dt[, .(date, close)], predictions_dt, by = "date")

# Backtest
predictions_dt[, signal := predictions > 0]
predictions_dt[, spy := close / shift(close) - 1]
predictions_dt[, strategy := spy * shift(signal, 1)]
predictions_dt = predictions_dt[date > as.Date("2002-10-01")] # to match QC backtest
backtest_xts = as.xts.data.table(predictions_dt[, .(date, spy, strategy)])
charts.PerformanceSummary(backtest_xts)
# charts.PerformanceSummary(backtest_xts[35000:nrow(backtest_xts)])
# charts.PerformanceSummary(backtest_xts[2180:nrow(backtest_xts)]) # same as QC

# Save for QC backtesting
qc_data = predictions_dt[, .(date, signal)]
qc_data = na.omit(qc_data)
qc_data[, let(
  date = as.character(date),
  signal = as.integer(signal)
)]
bl_endp_key = storage_endpoint(Sys.getenv("BLOB-ENDPOINT-SNP"), Sys.getenv("BLOB-KEY-SNP"))
cont = storage_container(bl_endp_key, "qc-backtest")
storage_write_csv(qc_data, cont, "exuber_gausscov.csv", col_names = FALSE)


# TVAR --------------------------------------------------------------------
# prepare data2
varvar = "sd_radf_sum" # kurtosis_bsadf_log, sd_radf_sum
cols = c("returns", varvar)
X_vars <- as.data.frame(backtest_data[, ..cols])

# descriptive
acf(backtest_data$returns)     # short memory
acf(backtest_data$kurtosis_bsadf_log) # long memory
# X_ <- X[X$sd_radf_sum < 5,]
# x <- shift(X_$sd_radf_sum)
# y <- X_$returns
# m1 <- loess(y ~ x) # local smoothing
# sx <- sort(shift(x),index=TRUE)   # sorting the threshold variable
# par(mfcol=c(1,1))
# ix <- sx$ix # index for orderstatistics
# plot(x, y, xlab='x(t1)', ylab='x(t)')
# lines(x[ix], m1$fitted[ix],col="red")

# window_lengths <- c(7 * 22 * 3, 7 * 22 * 6, 7 * 22 * 12, 7 * 22 * 24)
window_lengths <- c(7 * 22 * 2, 7 * 22 * 6, 7 * 22 * 12, 7 * 22 * 24)
cl <- makeCluster(8)
clusterExport(cl, "X_vars", envir = environment())
clusterEvalQ(cl, {library(tsDyn)})
roll_preds <- lapply(window_lengths, function(x) {
  runner(
    x = X_vars,
    f = function(x) {
      # debug
      # x = X_vars[1:500, ]

      # # TVAR (1)
      tv1 <- tryCatch(TVAR(data = x,
                           lag = 3,       # Number of lags to include in each regime
                           model = "TAR", # Whether the transition variable is taken in levels (TAR) or difference (MTAR)
                           nthresh = 2,   # Number of thresholds
                           thDelay = 1,   # 'time delay' for the threshold variable
                           trim = 0.05,   # trimming parameter indicating the minimal percentage of observations in each regime
                           mTh = 2,       # ombination of variables with same lag order for the transition variable. Either a single value (indicating which variable to take) or a combination
                           plot=FALSE),
                      error = function(e) NULL)
      if (is.null(tv1)) {
        tv1_pred_onestep <- NA
      } else {
        # tv1$coeffmat
        tv1_pred <- predict(tv1)[, 1]
        names(tv1_pred) <- paste0("predictions_", 1:5)
        thresholds <- tv1$model.specific$Thresh
        names(thresholds) <- paste0("threshold_", seq_along(thresholds))
        coef_1 <- tv1$coefficients$Bdown[1, ]
        names(coef_1) <- paste0(names(coef_1), "_bdown")
        coef_2 <- tv1$coefficients$Bmiddle[1, ]
        names(coef_2) <- paste0(names(coef_2), "_bmiddle")
        coef_3 <- tv1$coefficients$Bup[1, ]
        names(coef_3) <- paste0(names(coef_3), "_bup")
        cbind.data.frame(as.data.frame(as.list(thresholds)),
                         as.data.frame(as.list(tv1_pred)),
                         data.frame(aic = AIC(tv1)),
                         data.frame(bic = BIC(tv1)),
                         data.frame(loglik = logLik(tv1)),
                         as.data.frame(as.list(coef_1)),
                         as.data.frame(as.list(coef_2)),
                         as.data.frame(as.list(coef_3)))
      }
    },
    k = x,
    lag = 0L,
    cl = cl,
    na_pad = TRUE
  )
})
stopCluster(cl)
gc()

# save results
file_name <- paste0("exuberv3_threshold2_", varvar, "_", format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S"), ".rds")
file_name <- file.path("D:/features", file_name)
saveRDS(roll_preds, file_name)

# read results
list.files("D:/features", pattern = "exuberv3_threshold2_kurtosis_bsadf_log_", full.names = TRUE)
list.files("D:/features", pattern = "exuberv3_threshold2_sd", full.names = TRUE)
# roll_preds <- readRDS("D:/features/exuberv3_threshold2_kurtosis_bsadf_log_20230711074304.rds")
# roll_preds <- readRDS("D:/features/exuberv3_threshold2_sd_radf_sum_20230804080720.rds")
length(roll_preds)
roll_preds[[1]][[2000]]
varvar = "sd_radf_sum"

# extract info from object
roll_results <- lapply(roll_preds, function(x) lapply(x, as.data.table))
roll_results <- lapply(roll_results, rbindlist, fill = TRUE)
roll_results[[1]]
cols = c("date", "returns", varvar)
tvar_res <- lapply(roll_results, function(x){
  cbind(backtest_data[1:nrow(x), ..cols], x)
})

# visualize
ggplot(tvar_res[[1]], aes(date)) +
  geom_line(aes(y = threshold_1), color = "green") +
  geom_line(aes(y = threshold_2), color = "red") +
  geom_line(aes(y = kurtosis_bsadf_log ))
ggplot(tvar_res[[1]], aes(date)) +
  geom_line(aes(y = bic), color = "green")
ggplot(tvar_res[[1]][date %between% c("2020-01-01", "2022-10-01")], aes(date)) +
  geom_line(aes(y = threshold_1), color = "green") +
  geom_line(aes(y = threshold_2), color = "red") +
  geom_line(aes(y = kurtosis_bsadf_log))
ggplot(tvar_res[[1]][date %between% c("2022-01-01", "2023-06-01")], aes(date)) +
  geom_line(aes(y = threshold_1), color = "green") +
  geom_line(aes(y = threshold_2), color = "red") +
  geom_line(aes(y = kurtosis_bsadf_log))

# threshold based backtest
tvar_backtest <- function(tvar_res_i) {
  # tvar_res_i <- tvar_res[[2]]
  returns <- tvar_res_i$returns
  threshold_1 <- tvar_res_i$threshold_1
  threshold_2 <- tvar_res_i$threshold_2
  coef_1_up <- tvar_res_i$sd_radf_sum..1_bup
  coef_1_down <- tvar_res_i$sd_radf_sum..1_bdown
  coef_ret_1_up <- tvar_res_i$returns..1_bup
  coef_ret_1_down <- tvar_res_i$returns..1_bdown
  coef_ret_1_middle <- tvar_res_i$returns..1_bmiddle
  coef_ret_2_up <- tvar_res_i$returns..2_bup
  coef_ret_2_down <- tvar_res_i$returns..2_bdown
  coef_ret_2_middle <- tvar_res_i$returns..2_bmiddle
  coef_ret_3_up <- tvar_res_i$returns..3_bup
  coef_ret_3_down <- tvar_res_i$returns..3_bdown
  coef_ret_3_middle <- tvar_res_i$returns..3_bmiddle
  predictions_1 = tvar_res_i$predictions_1
  aic_ <- tvar_res_i$aic
  indicator <- tvar_res_i$sd_radf_sum
  predictions <- tvar_res_i$predictions_1
  sides <- vector("integer", length(predictions))
  sides <- vector("integer", length(returns))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(threshold_2[i-1]) || is.na(threshold_2[i-2])) {
      sides[i] <- NA
    } else if (indicator[i-1] > threshold_2[i-1] & coef_ret_1_up[i-1] < 0) {
      sides[i] <- 0
    } else if (indicator[i-1] > threshold_1[i-1] & coef_ret_1_middle[i-1] < 0 & coef_ret_2_middle[i-1] < 0) {
      sides[i] <- 0
      # } else if (indicator[i-1] < threshold_1[i-1] & coef_ret_1_down[i-1] < 0 & coef_ret_2_down[i-1] < 0 & coef_ret_3_down[i-1] < 0) {
      #   sides[i] <- 0
    } else {
      sides[i] <- 1
    }
  }
  sides <- ifelse(is.na(sides), 1, sides)
  returns_strategy <- returns * sides
  returns_strategy
  # Return.cumulative(returns_strategy)
}
lapply(tvar_res, function(x) Return.cumulative(tvar_backtest(x)))
res_ = tvar_res[[1]]
returns_strategy <- tvar_backtest(res_)
PerformanceAnalytics::Return.cumulative(returns_strategy)
charts.PerformanceSummary(xts(cbind(res_$returns, returns_strategy), order.by = res_$date))
charts.PerformanceSummary(xts(tail(cbind(returns, returns_strategy), 2000), order.by = tail(res_$time, 2000)))

# threshold based backtest with multiple windows
tvar_backtest_windows <- function(tvar_res_i) {
  # tvar_res_i <- tvar_res[[1]]
  returns <- tvar_res_i$returns
  threshold_1 <- tvar_res_i$threshold_1
  threshold_2 <- tvar_res_i$threshold_2
  coef_1_up <- tvar_res_i$sd_radf_sum..1_bup
  coef_1_down <- tvar_res_i$sd_radf_sum..1_bdown
  coef_ret_1_up <- tvar_res_i$returns..1_bup
  coef_ret_1_down <- tvar_res_i$returns..1_bdown
  coef_ret_1_middle <- tvar_res_i$returns..1_bmiddle
  coef_ret_2_up <- tvar_res_i$returns..2_bup
  coef_ret_2_down <- tvar_res_i$returns..2_bdown
  coef_ret_2_middle <- tvar_res_i$returns..2_bmiddle
  coef_ret_3_up <- tvar_res_i$returns..3_bup
  coef_ret_3_down <- tvar_res_i$returns..3_bdown
  coef_ret_3_middle <- tvar_res_i$returns..3_bmiddle
  predictions_1 = tvar_res_i$predictions_1
  aic_ <- tvar_res_i$aic
  indicator <- tvar_res_i$sd_radf_sum
  predictions <- tvar_res_i$predictions_1
  sides <- vector("integer", length(predictions))
  sides <- vector("integer", length(returns))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(threshold_2[i-1]) || is.na(threshold_2[i-2])) {
      sides[i] <- NA
    } else if (indicator[i-1] > threshold_2[i-1] & coef_ret_1_up[i-1] < 0) {
      sides[i] <- 0
    } else if (indicator[i-1] > threshold_1[i-1] & coef_ret_1_middle[i-1] < 0 & coef_ret_2_middle[i-1] < 0) {
      sides[i] <- 0
      # } else if (indicator[i-1] < threshold_1[i-1] & coef_ret_1_down[i-1] < 0 & coef_ret_2_down[i-1] < 0 & coef_ret_3_down[i-1] < 0) {
      #   sides[i] <- 0
    } else {
      sides[i] <- 1
    }
  }
  sides <- ifelse(is.na(sides), 1, sides)
  return(sides)
}
sides = lapply(tvar_res[1:3], function(x) tvar_backtest_windows(x))
sides <- Map(cbind, sides[[1]], sides[[2]], sides[[3]])
sides = lapply(sides, as.data.table)
sides = rbindlist(sides)
setnames(sides, c("tvar_308", "tvar_924", "tvar_1848"))
sides$sides_any <- rowSums(sides)
sides[, sides_any := ifelse(sides_any == 3, 1, 0)]
sides[, sides_all := ifelse(sides_any > 0, 1, 0)]
returns <- tvar_res[[1]]$returns
returns_strategies <- returns * sides
returns_strategies = cbind(tvar_res[[1]][, .(date)], returns_strategies)
charts.PerformanceSummary(returns_strategies)

# save TVAR data for QC
qc_data = res_[, .(
  date,
  indicator = sd_radf_sum,
  threshold_1,
  threshold_2,
  coef_ret_1_up = returns..1_bup,
  coef_ret_1_middle = returns..1_bmiddle,
  coef_ret_2_middle = returns..2_bmiddle
)]
# cols <- colnames(qc_data)[2:ncol(qc_data)]
# qc_data[, (cols) := lapply(.SD, shift), .SDcols = cols]
qc_data <- na.omit(qc_data)
qc_data[, date := as.character(date)]
bl_endp_key <- storage_endpoint(Sys.getenv("BLOB-ENDPOINT-SNP"), Sys.getenv("BLOB-KEY-SNP"))
cont <- storage_container(bl_endp_key, "qc-backtest")
time_ <- format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
file_name <- paste0("exuber_tvar_", time_, ".csv")
storage_write_csv(qc_data, cont, file_name, col_names = FALSE)
print(file_name)
