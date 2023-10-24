library(data.table)
library(tiledb)
library(rvest)
library(lubridate)
library(rtsplot)
library(PerformanceAnalytics)
library(TTR)
library(runner)
library(future.apply)
library(vars)
library(tsDyn)
library(parallel)
library(ggplot2)
library(AzureStor)




# SET UP ------------------------------------------------------------------
# globals
DATAPATH      = "F:/lean_root/data/all_stocks_hour.csv"
URIEXUBER     = "F:/equity-usa-hour-exuber"
NASPATH       = "C:/Users/Mislav/SynologyDrive/trading_data"

# parameters
exuber_window = c(600)


# UNIVERSE ----------------------------------------------------------------
# SPY constitues
spy_const = fread(file.path(NASPATH, "spy.csv"))
symbols_spy = unique(spy_const$ticker)

# SP 500
sp500_changes = read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies") %>%
  html_elements("table") %>%
  .[[2]] %>%
  html_table()
sp500_changes = sp500_changes[-1, c(1, 2, 4)]
sp500_changes$Date = as.Date(sp500_changes$Date, format = "%b %d, %Y")
sp500_changes = sp500_changes[sp500_changes$Date < as.Date("2009-06-28"), ]
sp500_changes_symbols = unlist(sp500_changes[, 2:3], use.names = FALSE)
sp500_changes_symbols = unique(sp500_changes_symbols)
sp500_changes_symbols = sp500_changes_symbols[sp500_changes_symbols != ""]

# SPY constitues + SP 500
symbols_sp500 = unique(c(symbols_spy, sp500_changes_symbols))
symbols_sp500 = tolower(symbols_sp500)
symbols_sp500 = c("spy", symbols_sp500)



# IMPORT DATA -------------------------------------------------------------
# import QC data
col = c("date", "open", "high", "low", "close", "volume", "close_adj", "symbol")
dt = fread(DATAPATH, col.names = col)

# filter symbols
dt = dt[symbol %chin% symbols_sp500]

# set time zone
attributes(dt$date)
if (!("tzone" %in% names(attributes(dt$date)))) {
  setattr(dt$date, 'tzone', "America/New_York")
} else if (attributes(dt$date)[["tzone"]] != "America/New_York") {
  dt[, date := force_tz(date, "America/New_York")]
}
attributes(dt$date)

# unique
dt <- unique(dt, by = c("symbol", "date"))

# adjust all columns
unadjustd_cols = c("open", "high", "low")
adjusted_cols = paste0(unadjustd_cols, "_adj")
dt[, (adjusted_cols) := lapply(.SD, function(x) (close_adj / close) * x), .SDcols = unadjustd_cols]

# remove NA values
dt = na.omit(dt)

# order
setorder(dt, symbol, date)

# free resources
gc()

# import exuber data
atributes_cols <- c("exuber_100_1_adf_log", "exuber_100_1_sadf_log",
                    "exuber_100_1_gsadf_log", "exuber_100_1_badf_log",
                    "exuber_100_1_bsadf_log", "exuber_600_1_adf_log",
                    "exuber_600_1_sadf_log", "exuber_600_1_gsadf_log",
                    "exuber_600_1_badf_log", "exuber_600_1_bsadf_log")
arr <- tiledb_array(URIEXUBER,
                    as.data.frame = TRUE,
                    attrs = atributes_cols)
system.time(exuber_data <- arr[])
tiledb_array_close(arr)
exuber_data <- as.data.table(exuber_data)
attributes(exuber_data$date)
setattr(exuber_data$date, "tzone", "UTC")
setorder(exuber_data, symbol, date)


# PREPARE DATA ------------------------------------------------------------
# merge market data and exuber data
exuber_dt <- merge(dt, exuber_data, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE)

# choose parameter set
# cols_by_parameter <- colnames(exuber_dt)[grep(exuber_window, colnames(exuber_dt))]
# cols <- c("symbol", "date", "close", cols_by_parameter)
# exuber_dt <- exuber_dt[, ..cols]
# colnames(exuber_dt) <- gsub(paste0("exuber_", exuber_window, "_\\d+_"), "", colnames(exuber_dt))

# remove missing values
exuber_dt = na.omit(exuber_dt)

# add first layer of  aggregation
# exuber_dt[, radf_sum := adf_log + sadf_log + gsadf_log + badf_log + bsadf_log]
exuber_dt[, radf_sum_600 := exuber_600_1_adf_log + exuber_600_1_sadf_log + exuber_600_1_gsadf_log + exuber_600_1_badf_log + exuber_600_1_bsadf_log]
exuber_dt[, radf_sum_100 := exuber_100_1_adf_log + exuber_100_1_sadf_log + exuber_100_1_gsadf_log + exuber_100_1_badf_log + exuber_100_1_bsadf_log]

# visualize adf and gsadf corr
unique(exuber_dt$symbol)
rtsplot(as.xts.data.table(exuber_dt[symbol == "ibm", .(date, close)]))
rtsplot(as.xts.data.table(exuber_dt[symbol == "f", .(date, exuber_600_1_adf_log)]))
rtsplot(as.xts.data.table(exuber_dt[symbol == "fast", .(date, exuber_600_1_adf_log)]))



# AGGREGATE PREDICTORS ----------------------------------------------------
# median aggreagation
radf_vars <- colnames(exuber_dt)[4:ncol(exuber_dt)]
indicators_median <- exuber_dt[, lapply(.SD, median, na.rm = TRUE), by = c('date'), .SDcols = radf_vars]
setnames(indicators_median, radf_vars, paste0("median_", radf_vars))

# standard deviation aggregation
indicators_sd <- exuber_dt[, lapply(.SD, sd, na.rm = TRUE), by = c('date'), .SDcols = radf_vars]
setnames(indicators_sd, radf_vars, paste0("sd_", radf_vars))

# mean aggregation
indicators_mean <- exuber_dt[, lapply(.SD, mean, na.rm = TRUE), by = c('date'), .SDcols = radf_vars]
setnames(indicators_mean, radf_vars, paste0("mean_", radf_vars))

# sum aggreagation
indicators_sum <- exuber_dt[, lapply(.SD, sum, na.rm = TRUE), by = c('date'), .SDcols = radf_vars]
setnames(indicators_sum, radf_vars, paste0("sum_", radf_vars))

# quantile aggregations
indicators_q <- exuber_dt[, lapply(.SD, quantile, probs = 0.99, na.rm = TRUE), by = c('date'), .SDcols = radf_vars]
setnames(indicators_q, radf_vars, paste0("q99_", radf_vars))
indicators_q97 <- exuber_dt[, lapply(.SD, quantile, probs = 0.97, na.rm = TRUE), by = c('date'), .SDcols = radf_vars]
setnames(indicators_q97, radf_vars, paste0("q97_", radf_vars))
indicators_q95 <- exuber_dt[, lapply(.SD, quantile, probs = 0.95, na.rm = TRUE), by = c('date'), .SDcols = radf_vars]
setnames(indicators_q95, radf_vars, paste0("q95_", radf_vars))

# skew aggregation
indicators_skew <- exuber_dt[, lapply(.SD, skewness, na.rm = TRUE), by = c('date'), .SDcols = radf_vars]
setnames(indicators_skew, radf_vars, paste0("skew_", radf_vars))

# kurtosis aggregation
indicators_kurt <- exuber_dt[, lapply(.SD, kurtosis, na.rm = TRUE), by = c('date'), .SDcols = radf_vars]
setnames(indicators_kurt, radf_vars, paste0("kurtosis_", radf_vars))

# merge indicators
indicators <- Reduce(function(x, y) merge(x, y, by = "date", all.x = TRUE, all.y = TRUE),
                     list(indicators_sd,
                          indicators_median,
                          indicators_sum,
                          indicators_mean,
                          indicators_q,
                          indicators_q97,
                          indicators_q95,
                          indicators_skew,
                          indicators_kurt))
setorder(indicators, date)

# remove missing values
indicators <- na.omit(indicators)

# visualize
rtsplot(as.xts.data.table(indicators[, .(date, sd_radf_sum_100)])) # sd
rtsplot(as.xts.data.table(indicators[, .(date, median_radf_sum)])) # median
rtsplot(as.xts.data.table(indicators[, .(date, q97_radf_sum)]))    # q97
rtsplot(as.xts.data.table(indicators[, .(date, q99_radf_sum)]))    # q99
rtsplot(as.xts.data.table(indicators[, .(date, skew_radf_sum)]))   # skew
rtsplot(as.xts.data.table(indicators[, .(date, kurtosis_radf_sum)])) # kurtosis

# merge SPY and indicators
spy = dt[symbol == "spy", .(date, close = close_adj)]
spy[, returns := close / shift(close) - 1]
backtest_data = indicators[spy, on = "date"]
backtest_data = na.omit(backtest_data)


# INSAMPLE ONE THRESHOLD OPTIMIZATION -------------------------------------
# backtest function - buy if above threshold and sell if below threshold
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

# backtest function - same as above but revrse signals
BacktestRev <- function(returns, indicator, threshold, return_cumulative = TRUE) {
  sides <- vector("integer", length(indicator))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(indicator[i-1])) {
      sides[i] <- NA
    } else if (indicator[i-1] < threshold) {
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
variables <- colnames(indicators)[2:length(colnames(indicators))]
params <- indicators[, ..variables][, lapply(.SD, quantile, probs = seq(0, 1, 0.02))]
params <- melt(params)
params <- merge(data.frame(sma_width=c(1, 7, 35)), params, by=NULL)

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
strategy_returns <- Backtest(returns_,
                             backtest_data[, q99_exuber_600_1_bsadf_log],
                             1.25,
                             FALSE)
charts.PerformanceSummary(xts(cbind(returns_, strategy_returns), order.by = backtest_data[, date]))
strategy_returns_rev <- BacktestRev(returns_,
                                    SMA(backtest_data[, kurtosis_bsadf_log], 35),
                                    0.0015,
                                    FALSE)
charts.PerformanceSummary(xts(cbind(returns_, strategy_returns_rev), order.by = backtest_data[, date]))

# optimization with optim package (DIFFENRET RESULTS WITH OPTIM)
optimize_strategy = function(threshold) {
  ret = Backtest(returns_, backtest_data[, sd_bsadf_log ], threshold)
  return(ret)
}
result_optim <- optim(0, optimize_strategy, method="Brent", lower=-3, upper=3)
result_optim
strategy_returns <- Backtest(returns_,
                             backtest_data[, skew_badf_log ],
                             -5.059788,
                             FALSE)
charts.PerformanceSummary(xts(cbind(returns_, strategy_returns), order.by = backtest_data[, date]))



# COMPARE OPTIMIZATION FUNTCIONS ------------------------------------------
# compare resulsts and speed
system.time({
  backtest_for_loop = Backtest(returns_,
                               backtest_data[, sd_bsadf_log  ],
                               0.63,
                               FALSE)
})
system.time({
  backtest_vectorized = BacktestVectorized(returns_,
                                           backtest_data[, sd_bsadf_log],
                                           0.63,
                                           FALSE)
})
all(backtest_for_loop == backtest_vectorized)


# WALK FORWARD OPTIMIZATION WITH MULTI VARS -----------------------------------
# optimization params
variables <- colnames(indicators)[2:length(colnames(indicators))]
variables = variables[!grepl("skew", variables)]
params <- indicators[, ..variables][, lapply(.SD, quantile, probs = seq(0, 1, 0.03))]
params <- melt(params)
setnames(params, c("variables", "thresholds"))

# init
returns_ <- backtest_data[, returns]
thresholds <- unlist(params[, 2], use.names = FALSE)
vars <- unlist(params[, 1], use.names = FALSE)

# walk forward optimization
start_time = Sys.time()
plan("multisession", workers = 4L)
best_params_window_l <- lapply(c(7 * 22, 7 * 22 * 3, 7 * 22 * 6, 7 * 22 * 9,
                                 7 * 252, 7 * 378, 7 * 252 * 2, 7 * 252 * 3), function(w) {
  bres = runner(
    x = as.data.frame(backtest_data),
    f = function(x) {
      ret <- future_vapply(1:nrow(params), function(i) {
        BacktestVectorized(x$returns,
                           x[, vars[i]],
                           thresholds[i])
      }, numeric(1))

      returns_strategies <- cbind(params, ret)
      tail(returns_strategies[order(returns_strategies$ret),], 100)
    },
    k = w,
    na_pad = TRUE,
    simplify = FALSE
  )
  file_name <- paste0("exuber_bestparams_all_", w, "_",
                      format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S"), ".rds")
  file_name <- file.path("D:/features", file_name)
  saveRDS(bres, file_name)
})
end_time = Sys.time()
time_elapsed = end_time - start_time

# read WF optimization results
files = list.files("D:/features", pattern = "exuber_bestparams_\\d+", full.names = TRUE)
dates_ =  gsub(".*bestparams_\\d+|\\.rds|_", "", files)
dates_ = as.POSIXct(dates_, format = "%Y%m%d%H%M%S")
files = files[which(dates_ < as.POSIXct("2023-08-03"))]
# files = files[-3]
files_int = as.integer(gsub(".*_bestparams_|_\\d+.rds", "", files))
files = files[order(files_int)]
best_params_window = lapply(files, readRDS)

# prepare object for best variable extraction
best_vars = lapply(best_params_window, function(x) x[!is.na(x)])
best_vars = lapply(best_vars, function(x) lapply(x, function(y) as.character(y[, "variables"])))

# help function to extract best vars by n best backtests
extract_best_wfo = function(x, n = 1) {
  best_best_vars = lapply(x, function(x) lapply(x, function(y) tail(y, n)))
  best_best_vars = unlist(best_best_vars)
  as.data.table(best_best_vars)[, .N, best_best_vars][order(N)]
}

# most important variables
extract_best_wfo(best_vars)     # best best variables
extract_best_wfo(best_vars, 5)  # best best variables by extracting 5 best
extract_best_wfo(best_vars, 10) # best best variables by extracting 10 best
extract_best_wfo(best_vars, 50) # best best variables by extracting 50 best

# help function thatcreate list of DT tables with all results
backtest_data_long = melt(backtest_data,
                          id.vars = c("date"),
                          measure.vars = colnames(backtest_data)[2:which(colnames(backtest_data) == "kurtosis_radf_sum")],
                          variable.name = "variables",
                          value.name = "indicator")
to_dt = function(x, n) {
  # x = best_params_window
  # n = 1
  qc_dt_l = lapply(x, function(x) lapply(x, function(y) tail(y, n)))
  qc_dt_l = lapply(qc_dt_l, function(x) rbindlist(lapply(x, as.data.table), fill = TRUE, idcol = "id"))
  qc_dt_l = lapply(qc_dt_l, function(x) x[, V1 := NULL])
  qc_dt_l = lapply(qc_dt_l, function(x) x[, .SD[all(is.na(variables)) | variables == names(which.max(table(variables)))], by = id])
  qc_dt_l = lapply(qc_dt_l, function(x) x[, unique(.SD[all(is.na(variables)) | ret == max(ret, na.rm = TRUE)], by = c("variables")), by = id]) # can be man instead of max
  qc_dt_l = lapply(qc_dt_l, function(x) cbind(backtest_data[, .(date, returns)], x))
  qc_dt_l = lapply(qc_dt_l, function(x) merge(x, backtest_data_long,
                                              by = c("date", "variables"),
                                              all.x = TRUE, all.y = FALSE))
  return(qc_dt_l)
}
results_dt_l = to_dt(best_params_window, 5)
results_dt_l[[5]][, table(variables)]
backtest_wfo = function(wfo_res, indicator = NULL) {
  # wfo_res = results_dt_l[[1]]
  threshold = wfo_res[, thresholds]
  returns   = wfo_res[, returns]
  date      = wfo_res[, date]
  indicator = wfo_res[, indicator]
  variables = wfo_res[, variables]
  sides <- vector("integer", length(indicator))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(indicator[i-1])) {
      sides[i] <- 1
      ## OPTION 1
      # } else if (indicator[i-1] > threshold[i-1])  { # THIS DOESN'T WORK, < or >
      #   sides[i] <- 0
      ## OPTION 2
    } else if (grepl("kurtosis_", variables[i-1]) & indicator[i-1] < threshold[i-1])  {
      sides[i] <- 0
    } else if (grepl("sd_|mean_|q\\d+|median", variables[i-1]) & indicator[i-1] > threshold[i-1])  {
      ## OPTION 2
      # } else if (grepl("q\\d+_", variables[i-1]) & indicator[i-1] > threshold[i-1])  {
        sides[i] <- 0
      # ELSE
    } else {
      sides[i] <- 1
    }
  }
  sides <- ifelse(is.na(sides), 1, sides)
  returns_strategy <- returns * sides
  return(as.data.table(cbind.data.frame(date = date,
                                        strategy = returns_strategy,
                                        benchmark = returns)))
}
wfo_backtests = lapply(results_dt_l, backtest_wfo, indicator = NULL)
lapply(wfo_backtests, function(x) Return.cumulative(as.xts.data.table(x)))
charts.PerformanceSummary(as.xts.data.table(wfo_backtests[[6]]))


# WALK FORWARD OPTIMIZATION WITH ONE VAR --------------------------------------
# optimization params
variables <- colnames(indicators)[5:length(colnames(indicators))]
variables = variables[grepl("sd_", variables)]
params <- indicators[, ..variables][, lapply(.SD, quantile, probs = seq(0, 1, 0.03))]
params <- melt(params)
setnames(params, c("variables", "thresholds"))
# params <- merge(data.frame(sma_width=c(1, 7, 7 * 5)), params, by=NULL)
# setnames(params, c("sma_window", "variables", "thresholds"))

# init
returns_ <- backtest_data[, returns]
thresholds <- unlist(params[, 2], use.names = FALSE)
vars <- unlist(params[, 1], use.names = FALSE)
# ns <- params[, 1]

# walk forward optimization
start_time = Sys.time()
plan("multisession", workers = 5L)
windows = seq(1540, 5544, 7 * 22)
best_params_window_l <- lapply(windows, function(w) {
  bres = runner(
    x = as.data.frame(backtest_data),
    f = function(x) {
      ret <- future_vapply(1:nrow(params), function(i) {
        BacktestVectorized(x$returns,
                           x[, vars[i]],
                           thresholds[i])
      }, numeric(1))

      returns_strategies <- cbind(params, ret)
      tail(returns_strategies[order(returns_strategies$ret),], 100)
    },
    k = w,
    na_pad = TRUE,
    simplify = FALSE
  )
  file_name <- paste0("exuber_bestparams_sd_", w, "_",
                      format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S"), ".rds")
  file_name <- file.path("D:/features", file_name)
  saveRDS(bres, file_name)
})
end_time = Sys.time()
time_elapsed = end_time - start_time

# read WF optimization results
files = list.files("D:/features", pattern = "exuber_bestparams_sd_sma", full.names = TRUE)
# files = files[c(1, 4, 6, 2, 3, 5)] # newest remove old file
dates_ =  gsub(".*bestparams_sd_sma_\\d+|\\.rds|_", "", files)
dates_ = as.POSIXct(dates_, format = "%Y%m%d%H%M%S")
files = files[which(dates_ > as.POSIXct("2023-08-15"))]
files_int = as.integer(gsub(".*_sma_|_\\d+.rds", "", files))
files = files[order(files_int)]
best_params_window = lapply(files, readRDS)

# help function thatcreate list of DT tables with all results
backtest_data_long = melt(backtest_data,
                          id.vars = c("date"),
                          measure.vars = colnames(backtest_data)[2:which(colnames(backtest_data) == "kurtosis_radf_sum_100")],
                          variable.name = "variables",
                          value.name = "indicator")

# try with one indicator
to_dt_one_indicator = function(x) {
  # x = best_params_window
  qc_dt_l = lapply(x, function(x) lapply(x, function(y) if (length(y) > 1) {tail(y[grep("sd_", y$variables), ],1)} else NA))
  qc_dt_l = lapply(qc_dt_l, function(x) lapply(x, function(y) if (length(y) > 1 && nrow(y) == 0) NA else y))
  qc_dt_l = lapply(qc_dt_l, function(x) rbindlist(lapply(x, as.data.table), fill = TRUE, idcol = "id"))
  qc_dt_l = lapply(qc_dt_l, function(x) x[, V1 := NULL])
  qc_dt_l = lapply(qc_dt_l, function(x) {
    x[, variables_int := as.integer(variables)]
    setnafill(x, type="locf", cols=c("thresholds", "variables_int"))
    levs <- levels(x$variables)
    x[, variables := factor(variables_int, seq_along(levs), levs)]
    x[, variables_char := paste(variables)]
  })
  qc_dt_l = lapply(qc_dt_l, function(x) cbind(backtest_data[, .(date, returns)], x))
  qc_dt_l = lapply(qc_dt_l, function(x) merge(x, backtest_data_long,
                                              by = c("date", "variables"),
                                              all.x = TRUE, all.y = FALSE))
  return(qc_dt_l)
}
results_dt_one_l = to_dt_one_indicator(best_params_window)

# backtset with sd
backtest_wfo_sd = function(wfo_res, indicator = NULL) {
  # wfo_res = results_dt_l[[1]]
  threshold = wfo_res[, thresholds]
  returns   = wfo_res[, returns]
  date      = wfo_res[, date]
  indicator = wfo_res[, indicator]
  variables = wfo_res[, variables]
  sides <- vector("integer", length(indicator))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(indicator[i-1])) {
      sides[i] <- 1
    } else if (indicator[i-1] > threshold[i-1])  { # THIS DOESN'T WORK, < or >
      sides[i] <- 0
    } else {
      sides[i] <- 1
    }
  }
  sides <- ifelse(is.na(sides), 1, sides)
  returns_strategy <- returns * sides
  return(as.data.table(cbind.data.frame(date = date,
                                        strategy = returns_strategy,
                                        benchmark = returns)))
}
wfo_backtests_sd = lapply(results_dt_one_l, backtest_wfo_sd, indicator = NULL)
names(wfo_backtests_sd) = files_int
ret_res = lapply(wfo_backtests_sd, function(x) Return.cumulative(as.xts.data.table(x)))
sr_res = lapply(wfo_backtests_sd, function(x) SharpeRatio.annualized(as.xts.data.table(x), scale = 252))

# plot results
wfo_backtests_sd = lapply(seq_along(wfo_backtests_sd), function(i) {
  setnames(wfo_backtests_sd[[i]],
           c("strategy", "benchmark"),
           c(paste0("strategy_", names(wfo_backtests_sd)[i]),
             paste0("benchmark_", names(wfo_backtests_sd)[i])))
})
data_plot = Reduce(function(x, y) merge(x, y, by = "date", all = TRUE), wfo_backtests_sd)
data_plot[, benchmark := benchmark_1540]
cols_benchmark = colnames(data_plot)[grep("benchmark_", colnames(data_plot))]
data_plot[, (cols_benchmark) := NULL]
charts.PerformanceSummary(as.xts.data.table(data_plot), plot.engine = "ggplot2")

# best
charts.PerformanceSummary(as.xts.data.table(data_plot[, .(date,benchmark, strategy_2772)]), plot.engine = "ggplot2")

# plot results - CAR
cars = lapply(wfo_backtests_sd, function(x) Return.cumulative(as.xts.data.table(x)))
cars = lapply(cars, as.data.table)
cars = rbindlist(cars, fill = TRUE)
cars[, benchmark := benchmark_1540]
cols_benchmark = colnames(cars)[grep("benchmark_", colnames(cars))]
cars[, (cols_benchmark) := NULL]
cars = melt(cars)
cars = na.omit(cars)
cars[, window := gsub("strategy_", "", variable)]
ggplot(cars, aes(x = as.factor(window))) +
  geom_col(aes(y = value)) +
  geom_hline(yintercept = cars[nrow(cars), value], color = "red")

# save data for QC
qc_data = results_dt_one_l
names(qc_data) = files_int
sr_max = which.max(unlist(lapply(sr_res, `[[`, 1)))
qc_data = qc_data[[sr_max]]
qc_data = qc_data[, .(date, threshold = thresholds, indicator)]
qc_data <- na.omit(qc_data)
qc_data[, date := as.character(date)]
bl_endp_key <- storage_endpoint(Sys.getenv("BLOB-ENDPOINT-SNP"), Sys.getenv("BLOB-KEY-SNP"))
cont <- storage_container(bl_endp_key, "qc-backtest")
time_ <- format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
file_name <- paste0("exuber_wf_", time_, ".csv")
storage_write_csv(qc_data, cont, file_name, col_names = FALSE)
print(file_name)


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
