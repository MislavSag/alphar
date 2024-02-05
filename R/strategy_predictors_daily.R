library(arrow)
library(fs)
library(data.table)
library(duckdb)
library(PerformanceAnalytics)
library(ggplot2)
library(gausscov)
library(runner)
library(future.apply)
library(parallel)


# import data
backcusum_files = dir_ls("F:/equity/usa/predictors-daily/backcusum")
backusum = lapply(backcusum_files, read_parquet)
names(backusum) = fs::path_ext_remove(fs::path_file(names(backusum)) )
backusum = rbindlist(backusum, idcol = "symbol")

# free memory
gc()

# get securities data from QC
get_sec = function(symbol) {
  con <- dbConnect(duckdb::duckdb())
  query <- sprintf("
    SELECT *
    FROM 'F:/lean_root/data/all_stocks_daily.csv'
    WHERE Symbol = '%s'
", symbol)
  data_ <- dbGetQuery(con, query)
  dbDisconnect(con)
  data_ = as.data.table(data_)
  data_ = data_[, .(date = Date, close = `Adj Close`)]
  data_[, returns := close / shift(close) - 1]
  data_ = na.omit(data_)
  dbDisconnect(con)
  return(data_)
}
spy = get_sec("spy")


# AGGREGATE PREDICTORS ----------------------------------------------------
# aggregate function
agg = function(dt_) {
  # median aggreagation
  vars_ <- colnames(dt_)[3:ncol(dt_)]
  indicators_median <- dt_[, lapply(.SD, median, na.rm = TRUE), by = 'date', .SDcols = vars_]
  setnames(indicators_median, vars_, paste0("median_", vars_))

  # standard deviation aggregation
  indicators_sd <- dt_[, lapply(.SD, sd, na.rm = TRUE), by = c('date'), .SDcols = vars_]
  setnames(indicators_sd, vars_, paste0("sd_", vars_))

  # mean aggregation
  indicators_mean <- dt_[, lapply(.SD, mean, na.rm = TRUE), by = c('date'), .SDcols = vars_]
  setnames(indicators_mean, vars_, paste0("mean_", vars_))

  # sum aggreagation
  indicators_sum <- dt_[, lapply(.SD, sum, na.rm = TRUE), by = c('date'), .SDcols = vars_]
  setnames(indicators_sum, vars_, paste0("sum_", vars_))

  # quantile aggregations
  indicators_q <- dt_[, lapply(.SD, quantile, probs = 0.99, na.rm = TRUE), by = c('date'), .SDcols = vars_]
  setnames(indicators_q, vars_, paste0("q99_", vars_))
  indicators_q97 <- dt_[, lapply(.SD, quantile, probs = 0.97, na.rm = TRUE), by = c('date'), .SDcols = vars_]
  setnames(indicators_q97, vars_, paste0("q97_", vars_))
  indicators_q95 <- dt_[, lapply(.SD, quantile, probs = 0.95, na.rm = TRUE), by = c('date'), .SDcols = vars_]
  setnames(indicators_q95, vars_, paste0("q95_", vars_))

  # skew aggregation
  indicators_skew <- dt_[, lapply(.SD, skewness, na.rm = TRUE), by = c('date'), .SDcols = vars_]
  setnames(indicators_skew, vars_, paste0("skew_", vars_))

  # kurtosis aggregation
  indicators_kurt <- dt_[, lapply(.SD, kurtosis, na.rm = TRUE), by = c('date'), .SDcols = vars_]
  setnames(indicators_kurt, vars_, paste0("kurtosis_", vars_))

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

  return(indicators)
}

# aggreate backcusum
backcusum_agg = agg(backusum)

# merge SPY and indicators
backtest_data = backcusum_agg[spy, on = "date"]
setorder(backtest_data, date)

# remove predictors with many NA values
cols_to_remove <- names(backtest_data)[sapply(backtest_data, function(col) sum(is.na(col)) > 600)]
backtest_data[, (cols_to_remove) := NULL]

# define predictors
predictors = colnames(backtest_data)
cols_ = c("returns", "close", "date")
predictors = setdiff(predictors, cols_)

# # create target varibales
# "ret_d", "ret_w", "ret_bw", "ret_m", "ret_q"
# backtest_data[, `:=`(
#   ret_d = shift(close, n = -1, type = "shift") / close -1,
#   ret_w = shift(close, n = -5, type = "shift") / close -1,
#   ret_bw = shift(close, n = -10, type = "shift") / close -1,
#   ret_m = shift(close, n = -22, type = "shift") / close -1,
#   ret_q = shift(close, n = -66, type = "shift") / close -1
# )]
# backtest_data = na.omit(backtest_data)
# dim(backtest_data)



# INSAMPLE FEATURE IMPORTANCE ---------------------------------------------
# gausscov
X = copy(backtest_data)
X = X[, (predictors) := lapply(.SD, shift), .SDcols = predictors]
X = na.omit(X)

# featues importance
gausscov_imp = function(col = "ret_d") {
  y_ = as.matrix(X[, ..col])
  X_ = as.matrix(X[, ..predictors])
  f1st_res = f1st(y = y_, x = X_)
  res_1 = f1st_res[[1]]
  res_1 = res_1[res_1[, 1] != 0, , drop = FALSE]
  f1st_res_imp = colnames(X_)[res_1[, 1]]
  f1st_res_imp
}
fi_gausscov_d = gausscov_imp()
fi_gausscov_w = gausscov_imp("ret_w")
fi_gausscov_bw = gausscov_imp("ret_bw")
fi_gausscov_m = gausscov_imp("ret_m")
fi_gausscov_q = gausscov_imp("ret_q")


# BACKTESTING FUNCTIONS ---------------------------------------------------
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
  # winpctx = length(x[x > 0])/length(x[x != 0])
  # annSDx = sd.annualized(x, scale=252)

  DDs <- findDrawdowns(x)
  maxDDx = min(DDs$return)
  # maxLx = max(DDs$length)

  Perf = c(cumRetx, annRetx, sharpex, maxDDx)
  names(Perf) = c("Cumulative Return", "Annual Return","Annualized Sharpe Ratio",
                  "Maximum Drawdown")
  # Perf = c(cumRetx, annRetx, sharpex, winpctx, annSDx, maxDDx, maxLx)
  # names(Perf) = c("Cumulative Return", "Annual Return","Annualized Sharpe Ratio",
  #                 "Win %", "Annualized Volatility", "Maximum Drawdown", "Max Length Drawdown")
  return(Perf)
}


# INSAMPLE OPTIMIZATION ---------------------------------------------------
# prepare backtest data
insample_data = na.omit(backtest_data, cols = predictors)
dim(insample_data)

# create returns again
insample_data[, returns := close / shift(close) - 1]

# remove missing values
insample_data = na.omit(insample_data)

# optimization params
params <- insample_data[, ..predictors][, lapply(.SD, quantile, probs = seq(0, 1, 0.04))]
params <- melt(params)

# help vectors
returns_    = insample_data[, returns]
thresholds_ = params[, 2][[1]]
vars        = as.character(params[, 1][[1]])

# optimization loop
system.time({
  opt_results_l =
    vapply(1:nrow(params), function(i)
      backtest_cpp(returns_,
                   insample_data[, get(vars[i])],
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


# WALK FORWARD OPT --------------------------------------------------------
# prepare backtest data
wfo_data = na.omit(backtest_data, cols = predictors)
dim(wfo_data)

# create returns again
wfo_data[, returns := close / shift(close) - 1]

# optimization params
variables = colnames(wfo_data[, ..predictors])
variables = variables[grep("sd_", variables)]
params <- wfo_data[, ..variables][, lapply(.SD, quantile, probs = seq(0, 1, 0.05))]
params <- melt(params)
setnames(params, c("variables", "thresholds"))

# init
thresholds = params[, thresholds]
vars       = as.character(params[, variables])

# walk forward optimization
windows = c(22, 44, 66, 88, 110, 132, 154, 176, 198, 220, 242, 264, 504, 1260)
plan("multisession", workers = 4L)
best_params_window_l <- future_lapply(windows, function(w) {
  # create file_name
  time_ = format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
  file_name = paste0("backcusum_sd_", w, "_", time_, ".rds")
  file_name = file.path("D:/features", file_name)
  if (fs::file_exists(file_name)) return(readRDS(file_name))
  # calucalte beest params on rolling window
  bres = runner(
    x = as.data.frame(wfo_data),
    f = function(x) {
      # x = as.data.frame(wfo_data[1:100])
      ret <- vapply(1:nrow(params), function(i) {
        BacktestVectorized(x$returns,
                           x[, vars[i]],
                           thresholds[i])
      }, numeric(1))

      res = cbind(date = tail(x$date, 1),
                  params,
                  ret)
      tail(res[order(res$ret), ], 100)
    },
    k = w,
    na_pad = TRUE,
    simplify = FALSE
  )
  #save
  saveRDS(bres, file_name)
})

# WFO results
wfo_files = dir_ls("D:/features", regexp = "backcusum_sd")
wfo_l = lapply(wfo_files, readRDS)
names(wfo_l) = gsub(".*sd_|_\\d+\\..*", "", fs::path_file(names(wfo_l)))
wfo_l = lapply(wfo_l, function(x) x[!is.na(x)])
wfo_l = lapply(wfo_l, rbindlist)
wfo = rbindlist(wfo_l, idcol = "window")
setnames(wfo, colnames(wfo)[2], "date")

# merge current values
wfo_res = copy(backtest_data)
wfo_res = melt(backtest_data, id.vars = "date", measure.vars = predictors, variable.name = "variables")
wfo_res = na.omit(wfo_res)
wfo = wfo_res[wfo, on = c("date", "variables")]

# crate signals
wfo[, signal_raw := !(value > thresholds), by = date]
wfo_best = wfo[, .(signal = as.integer(tail(signal_raw, 1))), by = .(window, date)]

# backtet
wfo_best[, unique(window)]
X =  wfo_best[window == 154]
X = X[spy, on = "date"]
X[is.na(signal), signal := 1]
X[, `:=`(
  strategy  = shift(signal) * returns,
  benchmark = returns
)]
Xts = as.xts.data.table(X[, .(date, benchmark, strategy)])
PerformanceAnalytics::charts.PerformanceSummary(Xts)
Performance(Xts[, 1])
Performance(na.omit(Xts[, 2]))


# TVAR --------------------------------------------------------------------
# prepare backtest data
tvar_data = na.omit(backtest_data, cols = predictors)
dim(tvar_data)

# create returns again
tvar_data[, returns := close / shift(close) - 1]

# remove missing values
tvar_data = na.omit(tvar_data)

# choose column to test TVAR for
predictors[grep("sd_", predictors)]
varvar = "sd_backcusum_128_greater_2_backcusum_rejections_10" # kurtosis_bsadf_log, sd_radf_sum
cols = c("returns", varvar)
X_vars <- as.data.frame(tvar_data[, ..cols])

# window_lengths <- c(7 * 22 * 3, 7 * 22 * 6, 7 * 22 * 12, 7 * 22 * 24)
library(tsDyn)
window_lengths <- c(66, 126, 252, 504)
cl <- makeCluster(4)
clusterExport(cl, "X_vars", envir = environment())
clusterEvalQ(cl, {library(tsDyn)})
roll_preds <- lapply(window_lengths, function(w) {
  runner(
    x = X_vars,
    f = function(x) {
      # debug
      # x = X_vars[1:66, ]

      # # TVAR (1)
      tv1 <- tryCatch(TVAR(data = x,
                           lag = 3,       # Number of lags to include in each regime
                           model = "TAR", # Whether the transition variable is taken in levels (TAR) or difference (MTAR)
                           nthresh = 2,   # Number of thresholds
                           thDelay = 1,   # 'time delay' for the threshold variable
                           trim = 0.05,   # trimming parameter indicating the minimal percentage of observations in each regime
                           mTh = 2,       # ombination of variables with same lag order for the transition variable. Either a single value (indicating which variable to take) or a combination
                           plot = FALSE),
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
    k = w,
    lag = 0L,
    # cl = cl,
    na_pad = TRUE
  )
})
stopCluster(cl)
gc()

# save results
time_ = format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
file_name <- paste0("backcusum_", varvar, "_", time_, ".rds")
file_name <- file.path("D:/features", file_name)
saveRDS(roll_preds, file_name)

# read results
list.files("D:/features", pattern = "backcusum_sd", full.names = TRUE)
# roll_preds <- readRDS("D:/features/backcusum_sd_backcusum_128_greater_2_backcusum_rejections_10_20231031212421.rds")
# varvar = "sd_radf_sum"

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
  geom_line(aes(y = sd_backcusum_128_greater_2_backcusum_rejections_10))
ggplot(tvar_res[[1]], aes(date)) +
  geom_line(aes(y = bic), color = "green")
ggplot(tvar_res[[1]][date %between% c("2020-01-01", "2022-10-01")], aes(date)) +
  geom_line(aes(y = threshold_1), color = "green") +
  geom_line(aes(y = threshold_2), color = "red") +
  geom_line(aes(y = sd_backcusum_128_greater_2_backcusum_rejections_10))
ggplot(tvar_res[[1]][date %between% c("2022-01-01", "2023-06-01")], aes(date)) +
  geom_line(aes(y = threshold_1), color = "green") +
  geom_line(aes(y = threshold_2), color = "red") +
  geom_line(aes(y = sd_backcusum_128_greater_2_backcusum_rejections_10))

# threshold based backtest
tvar_backtest <- function(tvar_res_i) {
  # tvar_res_i <- tvar_res[[1]]
  returns <- tvar_res_i$returns
  threshold_1 <- tvar_res_i$threshold_1
  threshold_2 <- tvar_res_i$threshold_2
  coef_1_up <- tvar_res_i[, .SD, .SDcols = grep("sd.*1_bup", colnames(tvar_res_i))]
  coef_1_down <- tvar_res_i[, .SD, .SDcols = grep("sd.*1_bdown", colnames(tvar_res_i))]
  coef_ret_1_up <- tvar_res_i[, .SD, .SDcols = grep("returns.*1_bup", colnames(tvar_res_i))]
  coef_ret_1_down <- tvar_res_i[, .SD, .SDcols = grep("returns.*1_bdown", colnames(tvar_res_i))]
  coef_ret_1_middle <- tvar_res_i[, .SD, .SDcols = grep("returns.*1_bmid", colnames(tvar_res_i))]
  coef_ret_2_up <- tvar_res_i[, .SD, .SDcols = grep("returns.*2_bup", colnames(tvar_res_i))]
  coef_ret_2_down <- tvar_res_i[, .SD, .SDcols = grep("returns.*2_bdown", colnames(tvar_res_i))]
  coef_ret_2_middle <- tvar_res_i[, .SD, .SDcols = grep("returns.*2_bmid", colnames(tvar_res_i))]
  coef_ret_3_up <- tvar_res_i[, .SD, .SDcols = grep("returns.*3_bup", colnames(tvar_res_i))]
  coef_ret_3_down <- tvar_res_i[, .SD, .SDcols = grep("returns.*3_bdow", colnames(tvar_res_i))]
  coef_ret_3_middle <- tvar_res_i[, .SD, .SDcols = grep("returns.*3_bmid", colnames(tvar_res_i))]
  predictions_1 = tvar_res_i$predictions_1
  aic_ <- tvar_res_i$aic
  indicator <- tvar_res_i$sd_backcusum_128_greater_2_backcusum_rejections_10
  predictions <- tvar_res_i$predictions_1
  # sides <- vector("integer", length(predictions))
  sides <- vector("integer", length(returns))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(threshold_2[i-1]) || is.na(threshold_2[i-2]) || is.na(coef_ret_1_up[i-1]) || is.na(indicator[i-2])) {
      sides[i] <- NA
    } else if (indicator[i-1] > threshold_2[i-1]) {
      sides[i] <- 0
    # } else if (indicator[i-1] > threshold_1[i-1] & coef_ret_1_middle[i-1] < 0 & coef_ret_2_middle[i-1] < 0) {
    #   sides[i] <- 0
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
res_ = tvar_res[[3]]
returns_strategy <- tvar_backtest(res_)
PerformanceAnalytics::Return.cumulative(returns_strategy)
charts.PerformanceSummary(xts(cbind(res_$returns, returns_strategy), order.by = res_$date))

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

