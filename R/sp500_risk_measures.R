library(data.table)
library(AzureStor)
library(roll)
library(TTR)
library(future.apply)
library(ggplot2)
library(PerformanceAnalytics)
library(runner)
library(DescTools)
library(mlr3verse)
require(finautoml)
library(QuantTools)
library(mlr3temporal)
library(torch)
require(finfeatures, lib.loc = "C:/Users/Mislav/Documents/GitHub/finfeatures/renv/library/R-4.1/x86_64-w64-mingw32")
library(reticulate)
# python packages
reticulate::use_python("C:/ProgramData/Anaconda3/envs/mlfinlabenv/python.exe", required = TRUE)
mlfinlab = reticulate::import("mlfinlab", convert = FALSE)
pd = reticulate::import("pandas", convert = FALSE)
builtins = import_builtins(convert = FALSE)
main = import_main(convert = FALSE)



# SET UP ------------------------------------------------------------------

# get data from azure
ENDPOINT = storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), key=Sys.getenv("BLOB-KEY"))
CONT = storage_container(ENDPOINT, "equity-usa-hour-fmpcloud-adjusted")
CONTMIN = storage_container(ENDPOINT, "equity-usa-minute-fmpcloud")
fmpcloudr::fmpc_set_token(Sys.getenv("APIKEY-FMPCLOUD"))

# params
features_file = "sp500-risk-features-20220121085324.csv" # save features to azure blob


# IMPORT DATA -------------------------------------------------------------

# import features from blob
if (!is.null(features_file)) {
  cont <- storage_container(ENDPOINT, "features")
  features <- storage_read_csv(cont, features_file)
  features <- as.data.table(features)
  feature_cols <- setdiff(colnames(features), c(colnames(features)[grep("_changes_", colnames(features))]))
  features <- features[, .SD, .SDcols = c(feature_cols)]
}


# import all data from Azure storage
azure_blobs <- list_blobs(CONT)
market_data_list <- lapply(azure_blobs$name, function(x) {
  print(x)
  y <- tryCatch(storage_read_csv2(CONT, x), error = function(e) NA)
  if (is.null(y) | all(is.na(y))) return(NULL)
  y <- cbind(symbol = x, y)
  return(y)
})
market_data <- rbindlist(market_data_list)
market_data[, symbol := toupper(gsub("\\.csv", "", symbol))]
market_data[, returns := close / shift(close) - 1, by = .(symbol)]
market_data <- na.omit(market_data)
market_data$datetime <- as.POSIXct(as.numeric(market_data$datetime),
                                   origin=as.POSIXct("1970-01-01", tz="EST"),
                                   tz="EST")
market_data <- market_data[close > 1e-005 & open > 1e-005 & high > 1e-005 & low > 1e-005]
market_data <- unique(market_data, by = c("symbol", "datetime"))
market_data_n <- market_data[, .N, by = symbol]
market_data_n <- market_data_n[which(market_data_n$N > 8 * 5 * 22 * 12)]  # remove prices with only 60 or less observations
market_data <- market_data[symbol %in% market_data_n$symbol]
X <- merge(market_data[symbol == "SPY"], features, by = "datetime", all.x = TRUE, all.y = FALSE)


# ADD FEATURES ----------------------------------------------------------

# makr OHLCV instance
OhlcvInstance = Ohlcv$new(market_data, date_col = "datetime")
lag_ = 0 # OVO VIDJETI KASNIJE!

# Features from OHLLCV
OhlcvFeaturesInit = OhlcvFeatures$new(windows = c(8, 8 * 5, 8 * 5 * 22, 8 * 5 * 22 * 6, 8 * 5 * 22 * 12),
                                      quantile_divergence_window =  c(8 * 5 * 22, 8 * 5 * 22 * 6, 8 * 5 * 22 * 12))
OhlcvFeaturesSet = OhlcvFeaturesInit$get_ohlcv_features(OhlcvInstance)

# inspect
OhlcvFeaturesSet[, 1:80]


# get statistics for every features
cols_features <- colnames(OhlcvFeaturesSet)[9:ncol(OhlcvFeaturesSet)]
# sum
indicators_sum <- OhlcvFeaturesSet[, lapply(.SD, function(x) sum(x, na.rm = TRUE)), by = .(date), .SDcols = cols_features]
colnames(indicators_sum) <- c("datetime", paste0("sum_", cols_features))
setorder(indicators_sum, datetime)
# sd
indicators_sd <- OhlcvFeaturesSet[, lapply(.SD, function(x) sd(x, na.rm = TRUE)), by = .(date), .SDcols = cols_features]
colnames(indicators_sd) <- c("datetime", paste0("sd_", cols_features))
setorder(indicators_sd, datetime)
# mean
indicators_mean <- OhlcvFeaturesSet[, lapply(.SD, function(x) mean(x, na.rm = TRUE)), by = .(date), .SDcols = cols_features]
colnames(indicators_mean) <- c("datetime", paste0("mean_", cols_features))
setorder(indicators_mean, datetime)
# median
indicators_median <- OhlcvFeaturesSet[, lapply(.SD, function(x) median(as.numeric(x), na.rm = TRUE)), by = .(date), .SDcols = cols_features]
colnames(indicators_median) <- c("datetime", paste0("median_", cols_features))
setorder(indicators_median, datetime)
# quantile
indicators_p_75 <- OhlcvFeaturesSet[, lapply(.SD, function(x) quantile(x, p = 0.75, na.rm = TRUE)), by = .(date), .SDcols = cols_features]
colnames(indicators_p_75) <- c("datetime", paste0("p_75_", cols_features))
setorder(indicators_p_75, datetime)
indicators_p_25 <- OhlcvFeaturesSet[, lapply(.SD, function(x) quantile(x, p = 0.25, na.rm = TRUE)), by = .(date), .SDcols = cols_features]
colnames(indicators_p_25) <- c("datetime", paste0("p_25_", cols_features))
setorder(indicators_p_25, datetime)
indicators_p_95 <- OhlcvFeaturesSet[, lapply(.SD, function(x) quantile(x, p = 0.95, na.rm = TRUE)), by = .(date), .SDcols = cols_features]
colnames(indicators_p_95) <- c("datetime", paste0("p_95_", cols_features))
setorder(indicators_p_95, datetime)
indicators_p_05 <- OhlcvFeaturesSet[, lapply(.SD, function(x) quantile(x, p = 0.05, na.rm = TRUE)), by = .(date), .SDcols = cols_features]
colnames(indicators_p_05) <- c("datetime", paste0("p_05_", cols_features))
setorder(indicators_p_05, datetime)
indicators_p_99 <- OhlcvFeaturesSet[, lapply(.SD, function(x) quantile(x, p = 0.99, na.rm = TRUE)), by = .(date), .SDcols = cols_features]
colnames(indicators_p_99) <- c("datetime", paste0("p_99_", cols_features))
setorder(indicators_p_99, datetime)
indicators_p_01 <- OhlcvFeaturesSet[, lapply(.SD, function(x) quantile(x, p = 0.01, na.rm = TRUE)), by = .(date), .SDcols = cols_features]
colnames(indicators_p_01) <- c("datetime", paste0("p_01_", cols_features))
setorder(indicators_p_01, datetime)
# kurtosis
indicators_kurtosis <- OhlcvFeaturesSet[, lapply(.SD, function(x) kurtosis(as.numeric(x), na.rm = TRUE)), by = .(date), .SDcols = cols_features]
colnames(indicators_kurtosis) <- c("datetime", paste0("kurtosis_", cols_features))
setorder(indicators_kurtosis, datetime)
# kurtosis
indicators_skewness <- OhlcvFeaturesSet[, lapply(.SD, function(x) skewness(as.numeric(x), na.rm = TRUE)), by = .(date), .SDcols = cols_features]
colnames(indicators_skewness) <- c("datetime", paste0("skewness_", cols_features))
setorder(indicators_skewness, datetime)

# merge all indicators
features <- Reduce(function(x, y) merge(x, y, by = c("datetime"), all.x = TRUE, all.y = FALSE),
                   list(indicators_sum, indicators_sd, indicators_mean, indicators_median ,
                        indicators_p_01, indicators_p_05, indicators_p_25, indicators_p_75,
                        indicators_p_95, indicators_p_99))

# save features to azure blob
cont <- storage_container(ENDPOINT, "features")
time_ <- format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
file_name_ <- paste0("sp500-risk-features-", time_, ".csv")
storage_write_csv(features, cont, file_name_)




# BACKTET UTILS -----------------------------------------------------------
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

# backtest rusk up
backtest <- function(returns, indicator, threshold, return_cumulative = TRUE) {
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

# backtest risk down
backtest_rev <- function(returns, indicator, threshold, return_cumulative = TRUE) {
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


# LEVEL TRADING RULE ------------------------------------------------------
# optimization loop
# na_threshold = 0.9
# keep_cols <- names(which(colMeans(!is.na(backtest_data)) > na_threshold))
# backtest_data <- X[, .SD, .SDcols = keep_cols]
# backtest_data <- na.omit(backtest_data)
vars_ <- colnames(X)[9:ncol(X)]
vars_ <- vars_[grep("sum_|^sd_|^mean_", vars_)]
sma_width = c(4, 8, 16, 40)
params <- expand.grid(vars_, sma_width, stringsAsFactors = FALSE)

# optimizations
returns_strategies <- list()
opt_results_l <- future_lapply(1:length(vars_), function(i) {

  # devugging
  # i = 1
  print(i)

  # param
  param_ <- params[i, ]

  # sample
  var_ <- param_[, 1][[1]]
  sma_width = param_[, 2][[1]]
  cols <- c("datetime", "returns", var_)
  sample_ <- X[, ..cols]
  p_ <- c(0.99, 0.99, 0.95, 0.9, 0.85, 0.80, 0.2, 0.15, 0.10, 0.05, 0.01, 0.001)
  p = quantile(sample_[, get(var_)], probs = p_, na.rm = TRUE)

  # calculate percentiles
  bs <- c()
  for (j in 1:length(p)) {
    bs[j] <- backtest(sample_$returns,
                      sma(sample_[, ..var_][[1]], sma_width),
                      p[j])
  }
  bs_rev <- c()
  for (j in 1:length(p)) {
    bs_rev[j] <- backtest_rev(sample_$returns,
                              sma(sample_[, ..var_][[1]], sma_width),
                              p[j])
  }
  backtest_res <- c(bs, bs_rev)
  names_res <- c(paste0(var_, "_", sma_width, "_up_", p_ * 100), paste0(var_, "_", sma_width, "_down_", p_ * 100))
  res <- cbind.data.frame(backtest_res, names_res)

  return(res)
})
optimization_results <- rbindlist(lapply(opt_results_l, as.data.table))
# optimization_results$backtest_res <- unlist(optimization_results$backtest_res)
# optimization_results$names_res <- unlist(optimization_results$names_res)
setorderv(optimization_results, "backtest_res")
tail(optimization_results, 100)
fwrite(optimization_results, paste0("D:/risks/opt_results/optimiztion_results_", Sys.Date(), ".csv"))

# plot best var
ggplot(X[, .(datetime, sum_volume_rate_880)], aes(x = datetime, y = sum_volume_rate_880)) + geom_line()
ggplot(X[, .(datetime, sum_volume_rate_880)], aes(x = datetime, y = sma(sum_volume_rate_880, 4))) + geom_line()
ggplot(X[datetime %between% c("2020-01-01", "2020-06-01"), .(datetime, sum_bbands_dn_8)], aes(x = datetime, y = sum_bbands_dn_8)) + geom_line()

# PERCENTILES TRADING RULE -----------------------------------------------

# backtest percentiles
backtest_percentiles <- function(returns, indicator,
                                 indicator_percentil, return_cumulative = TRUE) {
  sides <- vector("integer", length(indicator))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(indicator[i-1]) || is.na(indicator_percentil[i-1])) {
      sides[i] <- NA
    } else if (indicator[i-1] > indicator_percentil[i-1]) {
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

# calculate percentiles
backtest_data <- merge(market_data[symbol == "SPY"], features, by = "datetime", all.x = TRUE, all.y = FALSE)

# optimization loop
p <- c(0.8, 0.9, 0.95, 0.99, 0.999)
roll_width <- c(8 * 22 * seq(1, 12, 1), 8 * 22 * 12 * seq(2, 6, 1))
vars_ <- colnames(backtest_data)[9:ncol(backtest_data)]
params <- expand.grid(p, roll_width, vars_, stringsAsFactors = FALSE)
plan(multicore(workers = 8))
returns_strategies <- list()

# optimizations
opt_results_l <- future_lapply(1:nrow(params), function(i) {

  # devugging
  # i = 1052
  print(i)

  # param row
  param_ <- params[i, ]

  # sample
  var_ <- param_[, 3]
  cols <- c("datetime", "returns", var_)
  sample_ <- backtest_data[, ..cols]

  # calculate percentiles
  cols_new <- paste0('p_', param_[, 3])
  sample_[, (cols_new) := lapply(.SD, function(x) roll::roll_quantile(x, param_[2], p = param_[1])),
          .SDcols = param_[, 3]]
  backtets <- backtest_percentiles(sample_$returns,
                                   sample_[, ..var_][[1]],
                                   sample_[, ..cols_new][[1]])


  # return
  result <- cbind(param_, backtets)
  return(result)
})
optimization_results <- rbindlist(lapply(opt_results_l, as.data.table))
optimization_results$backtest_res <- unlist(optimization_results$backtest_res)
optimization_results$backtest_res <- unlist(optimization_results$backtest_res)
setorderv(optimization_results, "backtest_res")
tail(optimization_results, 100)
fwrite(optimization_results, paste0("D:/risks/opt_results/optimiztion_results_", Sys.Date(), ".csv"))

# changes worng!
cols_breaks <- colnames(backtest_data)[grep("breaks", colnames(backtest_data))]
x <- optimization_results[Var3 %in% cols_breaks]
ggplot(x, aes(x = backtets)) + geom_histogram()

# opt summary
summary_resuts <- optimization_results[, max(backtets, na.rm = TRUE), by = Var3]
setorder(summary_resuts, V1)
tail(summary_resuts, 50)


data(ttrc)
bbands.HLC <- BBands( ttrc[,c("High","Low","Close")] )
bbands.close <- BBands( ttrc[,"Close"] )
tail(bbands.close)


# PERCENTILES TRADING RULE OTHER SIDE -----------------------------------------------

# backtest percentiles
backtest_percentiles <- function(returns, indicator,
                                 indicator_percentil, return_cumulative = TRUE) {
  sides <- vector("integer", length(indicator))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(indicator[i-1]) || is.na(indicator_percentil[i-1])) {
      sides[i] <- NA
    } else if (indicator[i-1] < indicator_percentil[i-1]) {
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

# calculate percentiles
backtest_data <- merge(market_data[symbol == "SPY"], features, by = "datetime", all.x = TRUE, all.y = FALSE)

# optimization loop
p <- c(0.2, 0.1, 0.05, 0.01, 0.001)
roll_width <- c(8 * 22 * seq(1, 12, 1), 8 * 22 * 12 * seq(2, 6, 1))
vars_ <- colnames(backtest_data)[9:ncol(backtest_data)]
params <- expand.grid(p, roll_width, vars_, stringsAsFactors = FALSE)
plan(multicore(workers = 8))
returns_strategies <- list()

# optimizations
xx <- future_lapply(1:nrow(params), function(i) {

  # devugging
  # i = 1052
  print(i)

  # param row
  param_ <- params[i, ]

  # sample
  var_ <- param_[, 3]
  cols <- c("datetime", "returns", var_)
  sample_ <- backtest_data[, ..cols]

  # calculate percentiles
  cols_new <- paste0('p_', param_[, 3])
  sample_[, (cols_new) := lapply(.SD, function(x) roll::roll_quantile(x, param_[2], p = param_[1])),
          .SDcols = param_[, 3]]
  backtets <- backtest_percentiles(sample_$returns,
                                   sample_[, ..var_][[1]],
                                   sample_[, ..cols_new][[1]])


  # return
  result <- cbind(param_, backtets)
  return(result)
})
optimization_results_down <- rbindlist(xx)
setorderv(optimization_results_down, "backtets")
tail(optimization_results_down, 100)

# individual backtests
p <- 0.90
window_size <- 200
var_ <- "sd_bbands_up_8"
cols <- c("datetime", "returns", var_)
sample_ <- backtest_data[, ..cols]
cols_new <- paste0('p_', var_)
sample_[, (cols_new) := lapply(.SD, function(x) roll::roll_quantile(x, window_size, p = p)),
        .SDcols = var_]
backtets <- backtest_percentiles(sample_$returns, sample_[, ..var_][[1]], sample_$p_sd_bbands_up_8, FALSE)
charts.PerformanceSummary(xts(cbind(sample_$returns, backtets), order.by = sample_$datetime))

# inspect best variable
best_indicator <- features[, .(datetime, mean_changes_500)]
cols_features <- colnames(best_indicator)[2:ncol(best_indicator)]
cols_new <- paste0("p_", cols_features)
best_indicator[, (cols_new) := lapply(.SD, function(x) roll::roll_quantile(x, 176, p = 0.9)), .SDcols = cols_features]
best_indicator[, side := as.integer(mean_changes_500 < p_mean_changes_500)]
table(best_indicator$side)
ggplot(best_indicator, aes(x = datetime)) +
  geom_line(aes(y = mean_changes_500)) +
  geom_line(aes(y = p_mean_changes_500), color = "red")
ggplot(best_indicator[datetime %between% c("2020-02-15", "2020-04-01")], aes(x = datetime)) +
  geom_line(aes(y = mean_changes_500)) +
  geom_line(aes(y = p_mean_changes_500), color = "red")
ggplot(best_indicator[datetime %between% c("2019-02-01", "2020-06-01")], aes(x = datetime)) +
  geom_line(aes(y = mean_changes_500)) +
  geom_line(aes(y = p_mean_changes_500), color = "red")


# save



# BEST VARS ---------------------------------------------------------------

# plots
best_dt <- backtest_data[, .(datetime, mean_breaks_500)]
ggplot(best_dt, aes(x = datetime, y = mean_changes_500)) +
  geom_line()
ggplot(best_dt[datetime %between% c("2020-02-01", "2020-04-01")], aes(x = datetime, y = mean_changes_500)) +
  geom_line()
ggplot(best_dt, aes(x = datetime, y = QuantTools::sma(mean_changes_500, 10))) +
  geom_line()
ggplot(best_dt[datetime %between% c("2020-02-01", "2020-04-01")], aes(x = datetime, y = QuantTools::sma(mean_changes_500, 32))) +
  geom_line()
ggplot(best_dt[datetime %between% c("2007-08-01", "2008-04-01")], aes(x = datetime, y = QuantTools::sma(mean_changes_500, 32))) +
  geom_line()


# params for returns
sma_width <- 1:30
threshold <- seq(0.01, 0.1, by = 0.01)
vars <- colnames(features)[grep("_changes_500", colnames(features))]
paramset <- expand.grid(sma_width, threshold, vars, stringsAsFactors = FALSE)
colnames(paramset) <- c('sma_width', 'threshold', "vars")

# backtst function
backtest <- function(returns, indicator, threshold, return_cumulative = TRUE) {
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

# backtset
cum_returns_f <- function(paramset) {
  cum_returns <- vapply(1:nrow(paramset), function(x) {
    excess_sma <- QuantTools::sma(backtest_data[, get(paramset[x, 3])], paramset[x, 1])
    results <- backtest(backtest_data$returns, excess_sma, paramset[x, 2])
    return(results)
  }, numeric(1))
  results <- as.data.table(cbind(paramset, cum_returns))
}
cum_returns_dt <- cum_returns_f(paramset)
setorder(cum_returns_dt, cum_returns)
tail(cum_returns_dt, 150)

# best backtest
sma_width_param <- 8
p <- 0.75
p_width <- 1000
vars_param <- "mean_changes_5000"
excess_sma <- SMA(backtest_data[, get(vars_param)], sma_width_param)
q <- roll::roll_quantile(excess_sma, p_width, p = p)
strategy_returns <- backtest_percentiles(backtest_data$returns, excess_sma, q, return_cumulative = FALSE)
charts.PerformanceSummary(xts(cbind(backtest_data$returns, strategy_returns), order.by = backtest_data$datetime))
charts.PerformanceSummary(xts(cbind(backtest_data$returns[28000:nrow(backtest_data)], strategy_returns[28000:nrow(backtest_data)]),
                              order.by = indicators$datetime[28000:nrow(indicators)]))



# save best strategy to azure
indicators <- copy(backtest_data)
keep_cols <- colnames(indicators)[grep("datetime|mean_changes_", colnames(indicators))]
indicators_azure <- indicators[, ..keep_cols]
cols_features <- setdiff(keep_cols, "datetime")
indicators_azure <- indicators_azure[, (cols_features) := lapply(.SD, QuantTools::sma, n = 8), .SDcols = cols_features]
indicators_azure <- na.omit(indicators_azure)
new_cols_azure <- paste0("p_", setdiff(keep_cols, "datetime"))
indicators_azure[, (new_cols_azure) := lapply(.SD, function(x) roll::roll_quantile(x, 1000, p = 0.75)), .SDcols = setdiff(keep_cols, "datetime")]
sides_dt <- as.data.table(indicators_azure[, 2:5] < indicators_azure[, 6:9])
sides_dt <- sides_dt[, lapply(.SD, as.integer), .SDcols = colnames(sides_dt)]
colnames(sides_dt) <- paste0("side_", colnames(indicators_azure)[2:5])
indicators_azure <- cbind(indicators_azure, sides_dt)
cols <- colnames(indicators_azure)[2:ncol(indicators_azure)]
indicators_azure[, (cols) := lapply(.SD, shift), .SDcols = cols]
indicators_azure <- na.omit(indicators_azure)
file_name <- "D:/risks/minmax/p_changes.csv"
fwrite(indicators_azure, file_name, col.names = FALSE, dateTimeAs = "write.csv")
bl_endp_key <- storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), key=Sys.getenv("BLOB-KEY"))
cont <- storage_container(bl_endp_key, "qc-backtest")
storage_upload(cont, file_name, basename(file_name))
# https://contentiobatch.blob.core.windows.net/qc-backtest/minmax.csv?sp=r&st=2022-01-01T11:15:54Z&se=2023-01-01T19:15:54Z&sv=2020-08-04&sr=b&sig=I0Llnk3ELMOJ7%2FJ2i2VzQOxCFEOTbmFaEHzXnzLJ7ZQ%3D

# save
backtest_data[, .(datetime, p_99_changes_500)]





# FEATURE SELECTION -------------------------------------------------------
# remove cols with many NA
na_threshold = 0.8
keep_cols <- names(which(colMeans(!is.na(X)) > na_threshold))
clf_data <- X[, .SD, .SDcols = keep_cols]

# bins
clf_data[, bin := shift(close, 8 * 10, type = "lead") / shift(close, 1, type = "lead") - 1]
clf_data[, bin := as.factor(ifelse(bin > 0, 1L, 0L))]
table(clf_data$bin)

# select features vector
feature_cols <- setdiff(colnames(clf_data)[9:ncol(clf_data)],
                        c(colnames(clf_data)[grep("_changes_", colnames(clf_data))], "bin"))
clf_data <- clf_data[, .SD, .SDcols = c(feature_cols, "bin")]

#  NA omit and winsorization (remove ooutliers)
clf_data <- na.omit(clf_data)
clf_data[, (feature_cols) := lapply(.SD, Winsorize, probs = c(0.01, 0.99), na.rm = TRUE), .SDcols = feature_cols] # winsorize across dates

# define task
task = TaskClassif$new(id = "indicators_ml", backend = clf_data, target = 'bin')

# rpart tree
library(rpart.plot)
learner = lrn("classif.rpart", maxdepth = 4, predict_type = "prob")
task_ <- task$clone()
learner$train(task_)
predictins = learner$predict(task_)
predictins$score(msr("classif.acc"))
learner$importance()
rpart_model <- learner$model
rpart.plot(rpart_model)

# ranger learner: https://arxiv.org/pdf/1804.03515.pdf section 2.1
learner_ranger = lrn("classif.ranger")
learner_ranger$param_set$values = list(num.trees = 5000)
at_rf = AutoFSelector$new(
  learner = learner_ranger,
  resampling = rsmp("holdout"),
  measure = msr("classif.ce"),
  terminator = trm("evals", n_evals = 20),
  fselector = fs("random_search")
)
at_rpart = AutoFSelector$new(
  learner = lrn("classif.rpart"),
  resampling = rsmp("holdout"),
  measure = msr("classif.ce"),
  terminator = trm("evals", n_evals = 20),
  fselector = fs("sequential")
)
at_rpart_genetic = AutoFSelector$new(
  learner = lrn("classif.rpart"),
  resampling = rsmp("holdout"),
  measure = msr("classif.ce"),
  terminator = trm("evals", n_evals = 20),
  fselector = fs("genetic_search")
)
grid = benchmark_grid(
  task = task,
  learner = list(at_rf, at_rpart, at_rpart_genetic),
  resampling = rsmp("cv", folds = 3)
)
bmr_fs = benchmark(grid, store_models = TRUE)
bmr_fs$aggregate(msrs(c("classif.ce", "time_train")))

# exract features
important_features <- lapply(as.data.table(bmr_fs)$learner, function(x) unlist(x$fselect_result$features))
important_features <- as.data.table(table(unlist(important_features)))
important_features <- important_features[order(N, decreasing = TRUE), ]
head(important_features, 30)
important_features_n <- head(important_features[, V1], 20) # top n features selected through ML
important_features_most_important <- important_features[N == max(N, na.rm = TRUE), V1] # most importnatn feature(s)
features_short <- unique(c(important_features_most_important, important_features_n))
features_long <- unique(c(important_features_most_important, important_features_n))
features_long <- features_long[!grepl("lm_", features_long)]

# plot most important features
plot(features$sd_sd_rogers.satchell_880)


# save feature importance results
save_path <- "D:/mlfin/mlr3_models"
file_name <- file.path(save_path, paste0('fi-', task$id, "-", format.POSIXct(Sys.time(), "%Y%m%d-%H%M%S"), '.rds'))
saveRDS(bmr_fs, file = file_name)




# TRAIN AUTOML ------------------------------------------------------------

# raw data
clf_data <- merge(market_data[symbol == "SPY"], features, by = "datetime", all.x = TRUE, all.y = FALSE)

# filters
clf_data[, sd_filter := QuantTools::roll_sd_filter(close, 8 * 5 * 22, 2, 2)]

# remove cols with many NA
na_threshold = 0.8
keep_cols <- names(which(colMeans(!is.na(clf_data)) > na_threshold))
clf_data <- clf_data[, .SD, .SDcols = keep_cols]

# remove cols with many infinitive values
inf_threshold = 0.95
keep_cols <- names(which(colMeans(sapply(clf_data, is.finite)) > na_threshold))
clf_data <- clf_data[, .SD, .SDcols = keep_cols]

# remove inf data
cols <- vapply(clf_data, is.numeric, FUN.VALUE = logical(1), USE.NAMES = FALSE)
df <- as.data.frame(clf_data)
df <- df[!is.infinite(rowSums(df[, cols])), ]
cols <- vapply(df, is.numeric, FUN.VALUE = logical(1), USE.NAMES = FALSE)
# df[, cols] <- df[!is.nan(rowSums(df[, cols])), cols]
clf_data <- as.data.table(df)

# bins
clf_data[, bin := shift(close, 8, type = "lead") / shift(close, 1, type = "lead") - 1]
clf_data[, bin := as.factor(ifelse(bin > 0, 1L, 0L))]
table(clf_data$bin)

# holdout set
clf_data <- na.omit(clf_data)
holdout_ind <- which(clf_data$datetime > as.Date("2020-01-01"))
est_ind <- setdiff(1:nrow(clf_data), holdout_ind)

# select features vector
feature_cols <- setdiff(colnames(clf_data)[9:ncol(clf_data)],
                        c(colnames(clf_data)[grep("_changes_", colnames(clf_data))],
                          # colnames(clf_data)[grep("^p_", colnames(clf_data))],
                          "bin"))

#  winsorization (remove ooutliers)
clf_data[, (feature_cols) := lapply(.SD, Winsorize, probs = c(0.01, 0.99), na.rm = TRUE), .SDcols = feature_cols] # winsorize across dates

# define task
X <- clf_data[est_ind]
# X <- X[sd_filter == FALSE, ]
X_holdout <- clf_data[holdout_ind]
# X_holdout <- X_holdout[sd_filter == FALSE, ]
task = TaskClassif$new(id = "indicators_ml", backend = X[, .SD, .SDcols = c(feature_cols, "bin")], target = 'bin')
task_holdout = TaskClassif$new(id = "indicators_ml_holdout", backend = X_holdout[, .SD, .SDcols = c(feature_cols, "bin")], target = 'bin')

# train automl!
graph <- Graph()
sp <- Parameters()
at_1 = auto_tuner(
  method = "random_search",                          # other optimization methods?
  learner = graph,                                   # out graph learner, other learners can be adde for benchmark
  resampling = mlr3::rsmp("RollingWindowCV", folds = 3, window_size = 8 * 5 * 22), # inner resampling
  measure = msr("classif.acc"),                      # think on different measures, esc. for twoclass
  search_space = sp,                                 # search space defined above
  term_evals = 20L                                   # number o evaluations inside CV
)
design = benchmark_grid(task, list(at_1), rsmp('forecastHoldout', ratio = 0.8))
plan("multisession", workers = 4L)
bmr = benchmark(design, store_models = TRUE)

# inspect results
autoplot(bmr_automl)
bmr_automl$aggregate(msr("classif.acc"))
bmr_automl$score(msr("classif.acc"))
extract_inner_tuning_results(bmr_automl) # xtract inner tuning results of nested resampling
mlr3tuning::extract_inner_tuning_archives(bmr_automl) # Extract inner tuning archives of nested resampling.
# unique(mlr3tuning::extract_inner_tuning_archives(bmr_automl)[, "branch_learners.selection"])
mlr3tuning::extract_inner_tuning_archives(bmr_automl)[branch_learners.selection == "ranger"][9]

# save benchmark results
save_path <- "D:/mlfin/mlr3_models"
time_ <- format.POSIXct(Sys.time(), "%Y%m%d-%H%M%S")
file_name <- file.path(save_path, paste0('bmr-', task$id, "-", time_, '.rds'))
saveRDS(bmr_automl, file = file_name)

# import benchmark rsults
save_path <- "D:/mlfin/mlr3_models"
bmr_automl = readRDS(file.path(save_path, "bmr-indicators_ml-20220123-110748.rds"))



# FINAL MODEL -------------------------------------------------------------

# train automl!
graph <- Graph2()
sp <- Parameters2()
at_1 = auto_tuner(
  method = "random_search",                          # other optimization methods?
  learner = graph,                                   # out graph learner, other learners can be adde for benchmark
  resampling = mlr3::rsmp("forecastHoldout", ratio = 0.9),   # inner resampling
  measure = msr("classif.fbeta"),                      # think on different measures, esc. for twoclass
  search_space = sp,                                 # search space defined above
  term_evals = 10L                                    # number o evaluations inside CV
)

# train best model on whole dataset
final_model = at_1$train(task)

# model evaluation
final_model$archive
final_model$tuning_result
final_model$tuning_result

# predicions
preds_train <- final_model$predict(task)
preds_train$confusion
preds_train$score(msr("classif.acc"))

# prediction on holdout
preds_holdout <- final_model$predict(task_holdout)
preds_holdout$confusion
preds_holdout$score(msr("classif.acc"))

# backtest
backtest_dt <- cbind(X_holdout[, 1], as.data.table(preds_holdout))
table(backtest_dt$response)
backtest_dt[, sign := ifelse(prob.1 > 0.45, 1, 0)]
backtest_dt <- merge(market_data[symbol == "SPY" & datetime > as.Date("2020-01-01")], backtest_dt, by = "datetime", all.x = TRUE, all.y = FALSE)
backtest_dt[, sign := na.locf(sign, na.rm = FALSE)]
backtest_dt[, returns_strategy := returns * sign]
charts.PerformanceSummary(as.xts.data.table(backtest_dt[, .(datetime, returns, returns_strategy)]))




# VAR ---------------------------------------------------------------------
library(vars)
keep_cols_var <- colnames(X)[grepl("bband.*_8$", colnames(X))]
keep_cols_var <- c("datetime", "returns", keep_cols_var)
X <- backtest_data[, ..keep_cols_var]
X <- na.omit(X)
# X <- X[datetime %between% c("2015-01-01", "2020-02-26")]
# X <- as.xts.data.table(X)
# res <- VAR(as.xts.data.table(X[1:1000]), p = 8, type = "both")
# preds <- predict(res, n.ahead = 8, ci = 0.95)
# p <- preds$fcst$returns


# predictions for every period
roll_var <- runner(
  x = X,
  f = function(x) {
    res <- VAR(as.xts.data.table(x), p = 8, type = "both")
    p <- predict(res, n.ahead = 8, ci = 0.95)
    p <- p$fcst$returns
    data.frame(first = p[1, 1], median = median(p[, 1]), mean = mean(p[, 1]), sd = sd(p[, 1]), min = min(p[, 1]))
  },
  k = 1500,
  lag = 0L,
  na_pad = TRUE
)
predictions_var <- lapply(roll_var, as.data.table)
predictions_var <- rbindlist(predictions_var, fill = TRUE)
predictions_var[, V1 := NULL]
predictions_var <- cbind(datetime = X[, datetime], predictions_var)
predictions_var <- merge(market_data[symbol == "SPY"], predictions_var, by = "datetime", all.x = TRUE, all.y = FALSE)
predictions_var <- na.omit(predictions_var)
tail(predictions_var, 10)

# backtest apply
backtest_var <- function(returns, indicator, threshold, return_cumulative = TRUE) {
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
Return.cumulative(predictions_var$returns)
backtest_var(predictions_var$returns, predictions_var$first, -0.001)
backtest_var(predictions_var$returns, predictions_var$median, -0.002)
backtest_var(predictions_var$returns, predictions_var$mean, -0.001)
backtest_var(predictions_var$returns, predictions_var$min, -0.04)
backtest(predictions_var$returns, predictions_var$sd, 0.0002)
x <- backtest_var(predictions_var$returns, predictions_var$first, 0, FALSE)
charts.PerformanceSummary(as.xts(cbind(predictions_var$returns, x), order.by = predictions_var$datetime))
x <- backtest_var(predictions_var$returns, predictions_var$mean, 0, FALSE)
charts.PerformanceSummary(as.xts(cbind(predictions_var$returns, x), order.by = predictions_var$datetime))
x <- backtest_dummy(predictions_var$returns, predictions_var$sd, 0.002, FALSE)
charts.PerformanceSummary(as.xts(cbind(predictions_var$returns, x), order.by = predictions_var$datetime))



# VARS ALTERNATIVES ---------------------------------------------------------------
# prepare data
tail(optimization_results, 50)
keep_cols_var <- colnames(backtest_data)[grepl("bband.*8$|bband.*40$", colnames(backtest_data))]
keep_cols_var <- c("datetime", "returns", keep_cols_var)
X <- backtest_data[, ..keep_cols_var]
X <- na.omit(X)
X <- as.matrix(X[, 2:ncol(X)])
X <- scale(X)
head(X)
# plot(X[, c("sum_bbands_dn_10560", "sum_bbands_dn_5280")])
# columns_to_remove <- caret::findCorrelation(cor(X), cutoff = 0.9)
# head(X)
# X <- X[, -columns_to_remove]
# head(X)

# remove ocnstants and highly correlated
# ncol(X)
# task <- TaskRegr$new("example", as.data.frame(X), target = "returns")
# # prep = po("removeconstants", ratio = 0.1) %>>%
# prep = po("filter", flt("find_correlation", method = "spearman"), filter.cutoff = 0.05)
# X = prep$train(list(task))
# X <- X[[1]]$data()
# ncol(X)
# head(X)

# varhi VAR
Ncore = 1
pmdl = 4
Nfinal <- varhi.select(X, Ncore, pmdl, 0.01)
# alpha <- .05
# mdl <- varhi.fitmodel(X, pmdl, Nfinal, alpha, 10^(-12))
# phi.arry <- mdl[[2]]
# var.pred <- mdl[[3]]
# print(mdl[[1]])		# Whittle likelihood


# predictions for every period
X_ <- as.data.table(X[, Nfinal])
X_ <- cbind(backtest_data$datetime, X_)
roll_var <- runner(
  x = X_,
  f = function(x) {
    # x = X_[1:1500]
    res <- VAR(as.xts.data.table(x), p = 8, type = "both")
    p <- predict(res, n.ahead = 8, ci = 0.95)
    p <- p$fcst$returns
    data.frame(first = p[1, 1], median = median(p[, 1]), mean = mean(p[, 1]), sd = sd(p[, 1]), min = min(p[, 1]))
  },
  k = 8 * 8 * 22 * 6,
  lag = 0L,
  na_pad = TRUE
)
predictions_var <- lapply(roll_var, as.data.table)
predictions_var <- rbindlist(predictions_var, fill = TRUE)
predictions_var[, V1 := NULL]
predictions_var <- cbind(datetime = X_[, V1], predictions_var)
predictions_var <- merge(market_data[symbol == "SPY"], predictions_var, by = "datetime", all.x = TRUE, all.y = FALSE)
predictions_var <- na.omit(predictions_var)
tail(predictions_var, 10)

# backtest apply
backtest_var <- function(returns, indicator, threshold, return_cumulative = TRUE) {
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
Return.cumulative(predictions_var$returns)
backtest_var(predictions_var$returns, predictions_var$first, 0)
backtest_var(predictions_var$returns, predictions_var$median, 0)
backtest_var(predictions_var$returns, predictions_var$mean, 0)
backtest_var(predictions_var$returns, predictions_var$min, -0.5)
backtest(predictions_var$returns, predictions_var$sd, 0.3)
x <- backtest_var(predictions_var$returns, predictions_var$first, -0.01, FALSE)
charts.PerformanceSummary(as.xts(cbind(predictions_var$returns, x), order.by = predictions_var$datetime))
x <- backtest_var(predictions_var$returns, predictions_var$mean, 0, FALSE)
charts.PerformanceSummary(as.xts(cbind(predictions_var$returns, x), order.by = predictions_var$datetime))
x <- backtest(predictions_var$returns, predictions_var$sd, 0.25, FALSE)
charts.PerformanceSummary(as.xts(cbind(predictions_var$returns, x), order.by = predictions_var$datetime))




# VAR BIGTIME -------------------------------------------------------------
library(bigtime)
library(VARshrink)
keep_cols_var <- colnames(X)[grepl("sum_bband.*_8$", colnames(X))]
keep_cols_var <- c("datetime", "returns", keep_cols_var)
X_ <- X[, ..keep_cols_var]
X_ <- scale(as.matrix(X_[, 2:ncol(X_)]))
keep_cols <- names(which(colMeans(!is.na(X_)) > 0.8))
X_ <- X_[, keep_cols]
X_ <- na.omit(X_)
head(X_)

# est
# test <- VAR.L1 <- sparseVAR(Y=X_[1:500,],  # standardising the data
#                             selection = "bic", # using time series cross-validation
#                             VARpen = "L1") # using the lasso penalty
# is.stable(test)
# plot_cv(test)
# LhatL1 <- lagmatrix(fit=VAR.L1, returnplot=TRUE)
# fcst_recursive <- recursiveforecast(test, h = 4)
# fcst_recursive$fcst
# plot(fcst_recursive)
# fcst_direct <- directforecast(test)
# fcst_direct
# plot(fcst_direct, type = "line")
# diagnostics_plot(test, variable = "returns")

# ARMAfit <- sparseVARMA(Y=X_[1:500,], VARMAselection = "cv") # VARselection="cv" as default.
# LhatVARMA <- lagmatrix(fit=ARMAfit, returnplot=TRUE)
# fcst_direct <- directforecast(ARMAfit)
# fcst_direct
# plot(fcst_direct, type = "line")
# diagnostics_plot(ARMAfit, variable = "returns")

# test
y <- VARshrink(X_[1:1000, ], p = 2, type = "none", method = "kcv")
y$varresult$returns


# predictions for every period
roll_var <- runner(
  x = X,
  f = function(x) {
    res <- VAR(as.xts.data.table(x), p = 8, type = "both")
    p <- predict(res, n.ahead = 8, ci = 0.95)
    p <- p$fcst$returns
    data.frame(first = p[1, 1], median = median(p[, 1]), mean = mean(p[, 1]), sd = sd(p[, 1]), min = min(p[, 1]))
  },
  k = 1500,
  lag = 0L,
  na_pad = TRUE
)
predictions_var <- lapply(roll_var, as.data.table)
predictions_var <- rbindlist(predictions_var, fill = TRUE)
predictions_var[, V1 := NULL]
predictions_var <- cbind(datetime = X[, datetime], predictions_var)
predictions_var <- merge(market_data[symbol == "SPY"], predictions_var, by = "datetime", all.x = TRUE, all.y = FALSE)
predictions_var <- na.omit(predictions_var)
tail(predictions_var, 10)

# backtest apply
backtest_var <- function(returns, indicator, threshold, return_cumulative = TRUE) {
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
Return.cumulative(predictions_var$returns)
backtest_var(predictions_var$returns, predictions_var$first, -0.001)
backtest_var(predictions_var$returns, predictions_var$median, -0.002)
backtest_var(predictions_var$returns, predictions_var$mean, -0.001)
backtest_var(predictions_var$returns, predictions_var$min, -0.04)
backtest(predictions_var$returns, predictions_var$sd, 0.0002)
x <- backtest_var(predictions_var$returns, predictions_var$first, 0, FALSE)
charts.PerformanceSummary(as.xts(cbind(predictions_var$returns, x), order.by = predictions_var$datetime))
x <- backtest_var(predictions_var$returns, predictions_var$mean, 0, FALSE)
charts.PerformanceSummary(as.xts(cbind(predictions_var$returns, x), order.by = predictions_var$datetime))
x <- backtest_dummy(predictions_var$returns, predictions_var$sd, 0.002, FALSE)
charts.PerformanceSummary(as.xts(cbind(predictions_var$returns, x), order.by = predictions_var$datetime))



# ONNLINE VAR -------------------------------------------------------------
library("onlineVAR")
keep_cols_var <- colnames(X)[grepl("sum_bband.*_8$", colnames(X))]
keep_cols_var <- c("datetime", "returns", keep_cols_var)
X_ <- X[, ..keep_cols_var]
cols_scale <- colnames(X_)[2:ncol(X_)] # maybe 3 insted of 2
X_[, (cols_scale) := lapply(.SD, scale), .SDcols = cols_scale]
keep_cols <- names(which(colMeans(!is.na(as.matrix(X_))) > 0.8))
X_ <- X_[, ..keep_cols]
X_ <- na.omit(X_)
X_ <- X_[10:nrow(X_), ]
head(X_)

# fit
onlinefit <- onlineVAR(as.matrix(X_[, 2:ncol(X_)]), nu = 0.99, lags = 8, ahead = 1)
plot(onlinefit)
summary(onlinefit)
preds <- onlinefit$pred
colnames(preds) <- paste0("preds_", colnames(preds))

# backtest
backtest_dt <- cbind(X_, preds)
# backtest_dt <- merge(backtest_dt, X[, .(datetime, returns)], by = "datetime", all.x = TRUE, all.y = FALSE)




# TORCH -------------------------------------------------------------------
#
# keep_cols_var <- colnames(X)[grepl("sum_bband.*_8$", colnames(X))]
# keep_cols_var <- c("datetime", "returns", keep_cols_var)
# X_ <- X[, ..keep_cols_var]
# X_ <- as.matrix(X_[, 2:ncol(X_)])
# X_ <- X_[10:nrow(X_), ]

# X_tensor <- torch_tensor(X_)
# length(X_tensor)
# length(X_)

X_ <- as.matrix(X[, .(returns)])

train_set <- X_[1:as.integer((nrow(X_) * 0.75)), , drop = FALSE]
val_set <- X_[(as.integer((nrow(X_) * 0.75))+1):as.integer((nrow(X_) * 0.9)), , drop = FALSE]
test_set <- X_[(as.integer((nrow(X_) * 0.9))+1):nrow(X_), , drop = FALSE]

n_timesteps <- 8 * 5

# create torch dataset
sp500_risk = dataset(
  name = "sp500_risk",

  initialize = function(x, n_timestamps, sample_frac = 1) {

    self$x = torch_tensor(x)
    # self$x <- torch_tensor((x - train_mean) / train_sd)
    self$n_timestamps <- n_timestamps

    n <- length(self$x) - self$n_timestamps
    # this is important if we want to use only sample of our data (for exmaple 0.1 uses 10% of data)
    self$starts = sort(sample.int(
      n = n,
      size = n * sample_frac
    ))
  },

  .getitem = function(index) {

    start <- self$starts[index]
    end <- start + self$n_timestamps - 1

    x = self$x[start:end]
    y = self$x[end + 1]

    list(x = x, y = y)
  },

  .length = function() {
    length(self$starts)
  }
)

# data
train_ds <- sp500_risk(train_set, n_timesteps, sample_frac = 0.5)
length(train_ds)
length(X_tensor)
train_ds[1]

# batches
batch_size <- 32
train_dl <- dataloader(train_ds, batch_size = batch_size, shuffle = TRUE)
length(train_dl)

b <- train_dl %>% dataloader_make_iter() %>% dataloader_next()
b

# validation and test data
valid_ds <- sp500_risk(val_set, n_timesteps, sample_frac = 0.5)
valid_dl <- valid_ds %>% dataloader(batch_size = batch_size)

test_ds <- sp500_risk(test_set, n_timesteps)
test_dl <- test_ds %>% dataloader(batch_size = 1)

# model
model = nn_module(

  initialize = function(type, input_size, hidden_size, num_layers = 1, dropout = 0) {

    self$type <- type
    self$num_layers <- num_layers

    self$rnn <- if (self$type == "gru") {
      nn_gru(
        input_size = input_size,
        hidden_size = hidden_size,
        num_layers = num_layers,
        dropout = dropout,
        batch_first = TRUE
      )
    } else {
      nn_lstm(
        input_size = input_size,
        hidden_size = hidden_size,
        num_layers = num_layers,
        dropout = dropout,
        batch_first = TRUE
      )
    }

    self$output <- nn_linear(hidden_size, 1)
  },

  forward = function(input) {

    # list of [output, hidden]
    # we use the output, which is of size (batch_size, n_timesteps, hidden_size)
    x <- self$rnn(input)[[1]]

    # from the output, we only want the final timestep
    # shape now is (batch_size, hidden_size)
    x <- x[ , dim(x)[2], ]

    # feed this to a single output neuron
    # final shape then is (batch_size, 1)
    x %>% self$output()
  }
)


# training RNNs on the GPU currently prints a warning that may clutter
# the console
# see https://github.com/mlverse/torch/issues/461
# alternatively, use
# device <- "cpu"
device <- torch_device(if (cuda_is_available()) "cuda" else "cpu")

net <- model("gru", 1, 32)
net <- net$to(device = device)

# train
optimizer <- optim_adam(net$parameters, lr = 0.001)

num_epochs <- 30

train_batch <- function(b) {

  optimizer$zero_grad()
  output <- net(b$x$to(device = device))
  target <- b$y$to(device = device)

  loss <- nnf_mse_loss(output, target)
  loss$backward()
  optimizer$step()

  loss$item()
}

valid_batch <- function(b) {

  output <- net(b$x$to(device = device))
  target <- b$y$to(device = device)

  loss <- nnf_mse_loss(output, target)
  loss$item()

}

for (epoch in 1:num_epochs) {

  net$train()
  train_loss <- c()

  coro::loop(for (b in train_dl) {
    loss <-train_batch(b)
    train_loss <- c(train_loss, loss)
  })

  cat(sprintf("\nEpoch %d, training: loss: %3.5f \n", epoch, mean(train_loss)))

  net$eval()
  valid_loss <- c()

  coro::loop(for (b in valid_dl) {
    loss <- valid_batch(b)
    valid_loss <- c(valid_loss, loss)
  })

  cat(sprintf("\nEpoch %d, validation: loss: %3.5f \n", epoch, mean(valid_loss)))
}

net$eval()

preds <- rep(NA, n_timesteps)

coro::loop(for (b in test_dl) {
  output <- net(b$x$to(device = device))
  preds <- c(preds, output %>% as.numeric())
})

length(preds)
length(test_dl)

trues <- ifelse(as.vector(test_set) > 0, 1, 0)
resp <- ifelse(preds > 0, 1, 0)

