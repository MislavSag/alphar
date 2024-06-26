library(tiledb)
library(data.table)
library(nanotime)
library(rtsplot)
library(ggplot2)
library(patchwork)
library(PerformanceAnalytics)
library(lubridate)
library(TTR)
library(timechange)
library(AzureStor)
library(runner)
library(onlineBcp)
library(rvest)
library(Rcpp)
library(findata)
library(parallel)



# configure s3
config <- tiledb_config()
config["vfs.s3.aws_access_key_id"] <- Sys.getenv("AWS-ACCESS-KEY")
config["vfs.s3.aws_secret_access_key"] <- Sys.getenv("AWS-SECRET-KEY")
config["vfs.s3.region"] <- Sys.getenv("AWS-REGION")
context_with_config <- tiledb_ctx(config)

# parameters
exuber_window = 600 # 100, 600
universe = "sp500"  # sp100, sp500, all

# import hour ohlcv
arr <- tiledb_array("D:/equity-usa-hour-fmpcloud-adjusted",
                    as.data.frame = TRUE,
                    query_layout = "UNORDERED")
hour_data <- arr[]
tiledb_array_close(arr)
hour_data_dt <- as.data.table(hour_data)
attr(hour_data_dt$time, "tz") <- Sys.getenv("TZ")

# import exuber data
atributes_cols <- c("exuber_100_1_adf_log", "exuber_100_1_sadf_log",
                    "exuber_100_1_gsadf_log", "exuber_100_1_badf_log",
                    "exuber_100_1_bsadf_log", "exuber_600_1_adf_log",
                    "exuber_600_1_sadf_log", "exuber_600_1_gsadf_log",
                    "exuber_600_1_badf_log", "exuber_600_1_bsadf_log")
arr <- tiledb_array("s3://equity-usa-hour-features",
                    as.data.frame = TRUE,
                    attrs = atributes_cols)
system.time(exuber_data_1 <- arr[])
tiledb_array_close(arr)
arr <- tiledb_array("s3://equity-usa-hour-features-temporary",
                    as.data.frame = TRUE,
                    attrs = atributes_cols)
system.time(exuber_data_2 <- arr[])
tiledb_array_close(arr)
exuber_data <- rbind(exuber_data_1, exuber_data_2)
unique(exuber_data$symbol)
exuber_data_dt <- as.data.table(exuber_data)
attr(exuber_data_dt$date, "tz") <- Sys.getenv("TZ")
setnames(exuber_data_dt, "date", "time")

# merge market data and exuber data
exuber_dt <- merge(hour_data_dt, exuber_data_dt, by = c("symbol", "time"),
                   all.x = TRUE, all.y = FALSE)

# choose parameter set
cols_by_parameter <- colnames(exuber_dt)[grep(exuber_window, colnames(exuber_dt))]
cols <- c("symbol", "time", "close", cols_by_parameter)
exuber_dt <- exuber_dt[, ..cols]
colnames(exuber_dt) <- gsub(paste0("exuber_", exuber_window, "_\\d+_"), "", colnames(exuber_dt))

# visualize adf and gsadf corr
# rtsplot(as.xts.data.table(exuber_dt[symbol == "AAPL", .(date, adf)][2000:4000]))
# rtsplot(as.xts.data.table(exuber_dt[symbol == "AAPL", .(date, sadf)][2000:4000]))
# rtsplot(as.xts.data.table(exuber_dt[symbol == "AAPL", .(date, bsadf)][2000:4000]))
# rtsplot(as.xts.data.table(exuber_dt[symbol == "AAPL", .(date, gsadf)][2000:4000]))

# create new variable radf_sum and select variables we need for our analysis
exuber_dt[, radf_sum := adf_log + sadf_log + gsadf_log]
# exuber_dt[, radf_sum := adf_log + sadf_log + gsadf_log]

# choose universe
if (universe == "sp100") {
  sp100 <- read_html("https://en.wikipedia.org/wiki/S%26P_100") |>
    html_elements(x = _, "table") |>
    (`[[`)(3) |>
    html_table(x = _, fill = TRUE) |>
    (`[[`)(1)
  sp100 <- c(sp100)
  exuber_dt_sample <- exuber_dt[symbol %in% sp100]
} else if (universe == "sp500") {
  fmp = FMP$new()
  symbols <- fmp$get_sp500_symbols()
  exuber_dt_sample <- exuber_dt[symbol %in% symbols]
}

# define indicators based on exuber
radf_vars <- colnames(exuber_dt_sample)[4:ncol(exuber_dt_sample)]
indicators_median <- exuber_dt_sample[, lapply(.SD, median, na.rm = TRUE), by = c('time'), .SDcols = radf_vars]
colnames(indicators_median)[2:ncol(indicators_median)] <- paste0("median_", colnames(indicators_median)[2:ncol(indicators_median)])
indicators_sd <- exuber_dt_sample[, lapply(.SD, sd, na.rm = TRUE), by = c('time'), .SDcols = radf_vars]
colnames(indicators_sd)[2:ncol(indicators_sd)] <- paste0("sd_", colnames(indicators_sd)[2:ncol(indicators_sd)])
indicators_mean <- exuber_dt_sample[, lapply(.SD, mean, na.rm = TRUE), by = c('time'), .SDcols = radf_vars]
colnames(indicators_mean)[2:ncol(indicators_mean)] <- paste0("mean_", colnames(indicators_mean)[2:ncol(indicators_mean)])
indicators_sum <- exuber_dt_sample[, lapply(.SD, sum, na.rm = TRUE), by = c('time'), .SDcols = radf_vars]
colnames(indicators_sum)[2:ncol(indicators_sum)] <- paste0("sum_", colnames(indicators_sum)[2:ncol(indicators_sum)])
indicators_q <- exuber_dt_sample[, lapply(.SD, quantile, probs = 0.99, na.rm = TRUE), by = c('time'), .SDcols = radf_vars]
colnames(indicators_q)[2:ncol(indicators_q)] <- paste0("q99_", colnames(indicators_q)[2:ncol(indicators_q)])
indicators_q97 <- exuber_dt_sample[, lapply(.SD, quantile, probs = 0.97, na.rm = TRUE), by = c('time'), .SDcols = radf_vars]
colnames(indicators_q97)[2:ncol(indicators_q97)] <- paste0("q97_", colnames(indicators_q97)[2:ncol(indicators_q97)])
indicators_q95 <- exuber_dt_sample[, lapply(.SD, quantile, probs = 0.95, na.rm = TRUE), by = c('time'), .SDcols = radf_vars]
colnames(indicators_q95)[2:ncol(indicators_q95)] <- paste0("q95_", colnames(indicators_q95)[2:ncol(indicators_q95)])
indicators_skew <- exuber_dt_sample[, lapply(.SD, skewness, na.rm = TRUE), by = c('time'), .SDcols = radf_vars]
colnames(indicators_skew)[2:ncol(indicators_skew)] <- paste0("skew_", colnames(indicators_skew)[2:ncol(indicators_skew)])
indicators_kurt <- exuber_dt_sample[, lapply(.SD, kurtosis, na.rm = TRUE), by = c('time'), .SDcols = radf_vars]
colnames(indicators_kurt)[2:ncol(indicators_kurt)] <- paste0("kurt_", colnames(indicators_kurt)[2:ncol(indicators_kurt)])

# merge indicators
indicators <- Reduce(function(x, y) merge(x, y, by = "time", all.x = TRUE, all.y = FALSE),
                     list(indicators_sd, indicators_median, indicators_sum,
                          indicators_q, indicators_q97, indicators_q95,
                          indicators_skew, indicators_kurt))
setorderv(indicators, c("time"))
indicators <- na.omit(indicators)

# visualize
# library(TTR)
# rtsplot.matplot(as.xts.data.table(indicators[1000:2000, c(1, 7)]))
# rtsplot.lines(SMA(as.xts.data.table(indicators[1000:2000, c(1, 7)])))
# rtsplot.matplot(log(as.xts.data.table(indicators[1000:2000, c(1, 6:7)])))
# rtsplot.matplot(as.xts.data.table(indicators[1000:2000, c(1, 8:13)]))
# cols = rtsplot.colors(5)
# rtsplot(as.xts.data.table(indicators[, c(1:7)]), col = cols)
# rtsplot(as.xts.data.table(indicators[1000:2000, c(1:7)]), type = "l", col = graphics::par("col"))
# rtsplot(as.xts.data.table(exuber_dt[symbol == "AAPL", .(date, sadf)][2000:4000]))
# rtsplot(as.xts.data.table(exuber_dt[symbol == "AAPL", .(date, bsadf)][2000:4000]))
# rtsplot(as.xts.data.table(exuber_dt[symbol == "AAPL", .(date, gsadf)][2000:4000]))
# rtsplot(as.xts.data.table(indicators[, .(time, q99_radf_sum)]))
# rtsplot(as.xts.data.table(indicators[, .(time, q99_adf_log = diff(q99_adf_log))]))
# rtsplot(as.xts.data.table(indicators[, c(1, 7)]), col = cols)
# rtsplot(as.xts.data.table(indicators[28000:nrow(indicators), .(time, q99_radf_sum = diff(q99_radf_sum))]))
# median
rtsplot(as.xts.data.table(indicators[, .(time, median_radf_sum)]))
# q97
rtsplot(as.xts.data.table(indicators[, .(time, q97_radf_sum)]))
# q99
rtsplot(as.xts.data.table(indicators[, .(time, q99_radf_sum)]))
# skew
rtsplot(as.xts.data.table(indicators[, .(time, skew_radf_sum)]))
# rtsplot(as.xts.data.table(indicators[28000:nrow(indicators), .(time, skew_radf_sum = diff(q99_radf_sum))]))
# kurtosis
rtsplot(as.xts.data.table(indicators[, .(time, kurt_sadf_log)]))

# backtest functions
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
params <- merge(data.frame(sma_width=1:2), params, by=NULL)

# thresholds <- c(seq(1, 9, 0.02))
# thresholds <- indicators[ , ..variables][, .(q1 = quantile(.SD), max = max(.SD))]
# sma_window <- c(1:50)
# params <- expand.grid(thresholds, variables, sma_window, stringsAsFactors = FALSE)
# colnames(params) <- c("thresholds", "variables", "sma_window")

# optim data
optimization_data <- merge(hour_data_dt[symbol == "SPY"], indicators,
                           by = "time", all.x = TRUE, all.y = FALSE)
optimization_data <- optimization_data[as.integer(time_clock_at_tz(time,
                                                                   tz = "America/New_York",
                                                                   units = "hours")) %in% 10:16]
setorder(optimization_data, time)
optimization_data[, returns := close / shift(close) - 1]
optimization_data <- optimization_data[, .SD, .SDcols = c(1, 8:ncol(optimization_data))]
optimization_data <- na.omit(optimization_data)

# help vectors
returns <- optimization_data$returns
thresholds <- params[, 3]
vars <- as.vector(params[, 2])
ns <- params[, 1]

# optimizations loop
x <- vapply(1:nrow(params), function(i) backtest_cpp(returns,
                                                     SMA(optimization_data[, get(vars[i])], ns[i]),
                                                     thresholds[i]),
            numeric(1))
returns_strategies <- cbind(params, x)

# summary
optimization_results_level_sd_order <- returns_strategies[order(x, decreasing = TRUE), ]
head(optimization_results_level_sd_order, 30)
tail(optimization_results_level_sd_order)

# optimization summary
dt_ <- as.data.table(optimization_results_level_sd_order)
ggplot(dt_[variables == "sd_radf_sum"], aes(thresholds, sma_window, fill= x)) +
  geom_tile()

# backtest individual
threshold <- 0.8
strategy_returns <- backtest(optimization_data$returns,
                             optimization_data$q97_bsadf_log,
                             threshold,
                             FALSE)
charts.PerformanceSummary(xts(cbind(optimization_data$returns, strategy_returns), order.by = optimization_data$time))
charts.PerformanceSummary(tail(xts(cbind(optimization_data$returns, strategy_returns), order.by = optimization_data$time), 3000))
Performance(xts(strategy_returns, order.by = optimization_data$time))
Performance(xts(optimization_data$returns, order.by = optimization_data$time))

# save to azure
keep_cols <- colnames(optimization_data)[grep("time|sd_radf", colnames(indicators))]
indicators_azure <- optimization_data[, ..keep_cols]
cols <- colnames(indicators_azure)[2:ncol(indicators_azure)]
indicators_azure[, (cols) := lapply(.SD, shift), .SDcols = cols]
indicators_azure <- na.omit(indicators_azure)
indicators_azure[, time := with_tz(time, "America/New_York")]
indicators_azure[, time := as.character(time)]
SNP_KEY = "0M4WRlV0/1b6b3ZpFKJvevg4xbC/gaNBcdtVZW+zOZcRi0ZLfOm1v/j2FZ4v+o8lycJLu1wVE6HT+ASt0DdAPQ=="
SNP_ENDPOINT = "https://snpmarketdata.blob.core.windows.net/"
bl_endp_key <- storage_endpoint(SNP_ENDPOINT, key=SNP_KEY)
cont <- storage_container(bl_endp_key, "qc-backtest")
time_ <- format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
file_name <- paste0("exuber_", time_, ".csv")
storage_write_csv(indicators_azure, cont, file_name, col_names = FALSE)
file_name <- paste0("exuber_raw_", time_, ".csv")
file_name <- "exuber_raw_20221114103015.csv"
# storage_write_csv(exuber_dt, cont, file_name, col_names = FALSE)


# compare my trades and Quantconnect trades
indicators_azure
# head(indicators_azure)




# CHANGES IN THE MEAN -----------------------------------------------------
# prepare data
indicators_mean_change <- merge(hour_data_dt[symbol == "SPY"], indicators,
                                by = "time", all.x = TRUE, all.y = FALSE)
indicators_mean_change <- indicators_mean_change[as.integer(time_clock_at_tz(time,
                                                                             tz = "America/New_York",
                                                                             units = "hours")) %in% 10:16]
setorder(indicators_mean_change, time)
indicators_mean_change[, returns := close / shift(close) - 1]
indicators_mean_change <- indicators_mean_change[, .(time, returns, sd_radf_sum)]
indicators_mean_change <- na.omit(indicators_mean_change)

# detect mean changes
bcp <- online_cp(indicators_mean_change$sd_radf_sum, th_cp = 0.7, theta=0.95)
res <- summary(bcp)
print(res, digits = 4)
plot(res)

# add date of change and mean to the indicators var
indicators_mean_change[res$result$changepoint$location, changepoint := seq_along(res$result$changepoint$location)]
indicators_mean_change[, changepoint := nafill(changepoint, type = "nocb")]
indicators_mean_change[is.na(changepoint), changepoint := length(res$result$changepoint$location) + 1]
indicators_mean_change[, mean := roll::roll_mean(sd_radf_sum, 50000, min_obs = 1), by = changepoint]

# add final mean
start_ <- res$result$segment[, 1]
stop_ <- res$result$segment[, 2]
means_ <- res$result$segment[, 3]
for (i in 1:nrow(res$result$segment)) {
  indicators_mean_change[start_[i]:stop_[i], mean_regime := means_[i]]
}

# inspect
indicators_mean_change[340:350]
indicators_mean_change[785:795]

# backtest
returns <- indicators_mean_change$returns
sides <- vector("integer", length(returns))
indicator <- indicators_mean_change$sd_radf_sum
indicator_mean <- indicators_mean_change$mean
indicator_mean_regime <- indicators_mean_change$mean_regime
regimes <- indicators_mean_change$changepoint
mean_previous_regime <- NA
for (i in seq_along(sides)) {
  if (i %in% c(1, 2) || is.na(indicator[i-1]) || is.na(indicator_mean[i-1])) {
    sides[i] <- NA
  } else if (regimes[i-1] == 1  || is.na(mean_previous_regime)) {
    sides[i] <- 1
  } else if (indicator_mean[i-1] > mean_previous_regime) {
    sides[i] <- 0
  } else {
    sides[i] <- 1
  }
  if (i %in% c(1, 2) || is.na(regimes[i-2]) || regimes[i-1] == 1) {
    next()
  } else if (regimes[i-1] < regimes[i-2] | regimes[i-1] > regimes[i-2]) {
    print(mean_previous_regime)
    mean_previous_regime <- indicator_mean_regime[i-1]
  }
}
sides <- ifelse(is.na(sides), 1, sides)
returns_strategy <- returns * sides
PerformanceAnalytics::Return.cumulative(returns_strategy)
PerformanceAnalytics::charts.PerformanceSummary(xts(cbind(returns, returns_strategy), order.by = indicators_mean_change$time))



# EXPANDING BEST THRESHOLD ------------------------------------------------
# optimization params
thresholds <- c(seq(1, 3, 0.02))
variables <- colnames(indicators)[7]
sma_window <- c(1:10)
params <- expand.grid(thresholds, variables, sma_window, stringsAsFactors = FALSE)
colnames(params) <- c("thresholds", "variables", "sma_window")

# optim data
optimization_data <- merge(hour_data_dt[symbol == "SPY"], indicators,
                           by = "time", all.x = TRUE, all.y = FALSE)
optimization_data <- optimization_data[as.integer(time_clock_at_tz(time,
                                                                   tz = "America/New_York",
                                                                   units = "hours")) %in% 10:16]
setorder(optimization_data, time)
optimization_data[, returns := close / shift(close) - 1]
optimization_data <- optimization_data[, .SD, .SDcols = c(1, 8:ncol(optimization_data))]
optimization_data <- na.omit(optimization_data)
optimization_data <- optimization_data[, .(time, sd_radf_sum, returns)]

# init
returns <- optimization_data$returns
thresholds <- params[, 1]
vars <- params[, 2]
ns <- params[, 3]

# walk forward optimization
# if (nrow(x) < 100) {
#   return(NA)
# }
# cl <- parallel::makeCluster(8L)
# optimization_data <- as.data.frame(optimization_data)
# parallel::clusterExport(cl, c("optimization_data", "thresholds", "vars", "ns", "params", "backtest_cpp"),
#                         envir = environment())
# parallel::clusterCall(cl, function() lapply(c("TTR", "data.table"), require, character.only = TRUE))
best_params_window <- runner(
  x = optimization_data,
  f = function(x) {
    x_ <- vapply(1:nrow(params), function(i) backtest_cpp(x$returns,
                                                          SMA(x[, 2], ns[i]),
                                                          thresholds[i]),
                 numeric(1))
    returns_strategies <- cbind(params, x_)
    return(returns_strategies[which.max(returns_strategies$x), ])
  },
  k = 2000, # 2000/4.602549. 1000/3.20532
  na_pad = TRUE,
  simplify = FALSE
)
# parallel::stopCluster(cl)

# save best params object
file_name <- paste0("exuber_bestparams_2000_",
                    format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S"), ".rds")
file_name <- file.path("D:/features", file_name)
saveRDS(best_params_window, file_name)

# read existing
list.files("D:/features")
best_params_window <- readRDS("D:/features/exuber_bestparams_2000_20221128055057.rds")
length(best_params_window)
best_params_window[[1]]
best_params_window[[1999]]
best_params_window[[2000]]
dim(optimization_data)

# buy using best params
best_prams_cleaned <- lapply(best_params_window, as.data.table)
best_prams_cleaned <- rbindlist(best_prams_cleaned, fill = TRUE)
best_prams_cleaned[, V1 := NULL]
thresholds_best <- best_prams_cleaned$thresholds
radf_values <- optimization_data[, sd_radf_sum]
sma_window_best <- best_prams_cleaned$sma_window
returns_best <- returns[1:length(sma_window_best)]
# expanding
# indicators_sma <- vapply(seq_along(returns_best), function(x) {
#  if (x > 100) {
#    tail(SMA(radf_values[1:x], n = sma_window_best[x]), 1)
#  } else {
#    return(NA)
#  }
# }, numeric(1))
# rolling
indicators_sma <- vapply(seq_along(returns_best), function(x) {
  if (x > 2000) {
    tail(SMA(radf_values[(x - 2000):x], n = sma_window_best[x]), 1)
  } else {
    return(NA)
  }
}, numeric(1))

# backtest using dynamic variables, thresholds and sma
# inputs <- storage_read_csv(cont, "exuber_wf_20221125121609.csv", col_names = FALSE)
# indicators_sma <- inputs$X2
# thresholds_best <- inputs$X3
# merge(optimization_data, indicators_sma)
summary(radf_values)
sides <- vector("integer", length(returns_best))
for (i in seq_along(sides)) {
  if (i %in% c(1) || is.na(indicators_sma[i-1])) {
    sides[i] <- 1
  } else if (indicators_sma[i-1] > thresholds_best[i-1])  {
    sides[i] <- 0
  } else {
    sides[i] <- 1
  }
}
sides <- ifelse(is.na(sides), 1, sides)
returns_strategy <- returns_best * sides
PerformanceAnalytics::Return.cumulative(returns_strategy)
PerformanceAnalytics::Return.cumulative(returns_best)
data_ <- data.table(date = optimization_data$time, benchmark = returns_best, strategy = returns_strategy)
PerformanceAnalytics::charts.PerformanceSummary(as.xts.data.table(data_))
PerformanceAnalytics::charts.PerformanceSummary(tail(as.xts.data.table(data_), 2000))

# save data for QC
qc_data <- cbind.data.frame(time = optimization_data$time,
                            indicator = indicators_sma,
                            threshold = thresholds_best)
qc_data <- as.data.table(qc_data)
cols <- colnames(qc_data)[2:ncol(qc_data)]
qc_data[, (cols) := lapply(.SD, shift), .SDcols = cols]
qc_data <- na.omit(qc_data)
qc_data[, time := with_tz(time, "America/New_York")]
qc_data[, time := as.character(time)]
bl_endp_key <- storage_endpoint(Sys.getenv("BLOB-ENDPOINT-SNP"), Sys.getenv("BLOB-KEY-SNP"))
cont <- storage_container(bl_endp_key, "qc-backtest")
time_ <- format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
file_name <- paste0("exuber_wf_", time_, ".csv")
storage_write_csv(qc_data, cont, file_name, col_names = FALSE)



# ECONOMETRIC TS ----------------------------------------------------------
# optim data
X <- merge(hour_data_dt[symbol == "SPY"], indicators,
           by = "time", all.x = TRUE, all.y = FALSE)
X <- X[as.integer(time_clock_at_tz(time,
                                   tz = "America/New_York",
                                   units = "hours")) %in% 10:16]
setorder(X, time)
X[, returns := close / shift(close) - 1]
X <- X[, .SD, .SDcols = c(1, 8:ncol(X))]
X <- na.omit(X)

# prepare data
X_vars <- as.data.frame(X[, .(returns, sd_radf_sum)])

# descriptive
acf(X$returns)     # short memory
acf(X$sd_radf_sum) # long memory
# X_ <- X[X$sd_radf_sum < 5,]
# x <- shift(X_$sd_radf_sum)
# y <- X_$returns
# m1 <- loess(y ~ x) # local smoothing
# sx <- sort(shift(x),index=TRUE)   # sorting the threshold variable
# par(mfcol=c(1,1))
# ix <- sx$ix # index for orderstatistics
# plot(x, y, xlab='x(t1)', ylab='x(t)')
# lines(x[ix], m1$fitted[ix],col="red")


# frac diff
# library(fracdiff)
# diff_times <- lapply(optimization_data[, 2:(ncol(optimization_data)-1)],
#                      function(x) fdGPH(x, bandw.exp = 0.5)$d)
# diff_times_vec <- unlist(diff_times)
# X <- copy(optimization_data)
# X[ , (names(diff_times_vec)) := lapply(seq_along(.SD), function(x) diffseries(.SD[[x]], diff_times_vec[x])),
#    .SDcols = names(diff_times_vec)]

# VAR
library(vars)
library(tsDyn)

# window_lengths <- c(7 * 22 * 3, 7 * 22 * 6, 7 * 22 * 12, 7 * 22 * 24)
window_lengths <- c(7 * 22 * 2, 7 * 22 * 6)
cl <- makeCluster(8)
clusterExport(cl, "X", envir = environment())
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
# file_name <- paste0("exuber_threshold2_", format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S"), ".rds")
# file_name <- file.path("D:/features", file_name)
# saveRDS(roll_preds, file_name)

# read results
# list.files("D:/features")
# roll_preds <- readRDS(file.path("D:/features", "exuber_threshold2_20221128054703.rds"))

# extract info from object
roll_results <- lapply(roll_preds, function(x) lapply(x, as.data.table))
roll_results <- lapply(roll_results, rbindlist, fill = TRUE)
roll_results[[1]]
tvar_res <- lapply(roll_results, function(x){
  cbind(X[1:nrow(x), .(time, returns, sd_radf_sum)], x)
})

# visualize
ggplot(tvar_res[[1]], aes(time)) +
  geom_line(aes(y = threshold_1), color = "green") +
  geom_line(aes(y = threshold_2), color = "red") +
  geom_line(aes(y = sd_radf_sum))
ggplot(tvar_res[[1]], aes(time)) +
  geom_line(aes(y = bic), color = "green")
ggplot(tvar_res[[1]][time %between% c("2020-01-01", "2022-10-01")], aes(time)) +
  geom_line(aes(y = threshold_1), color = "green") +
  geom_line(aes(y = threshold_2), color = "red") +
  geom_line(aes(y = sd_radf_sum))

# threshold based backtest
tvar_backtest <- function(tvar_res_i) {
  # tvar_res_i <- tvar_res[[2]]
  returns <- tvar_res_i$returns
  threshold_1 <- tvar_res_i$threshold_1
  threshold_2 <- tvar_res_i$threshold_2
  coef_top_1 <- tvar_res_i$sd_radf_sum..1_bup
  coef_top_2 <- tvar_res_i$sd_radf_sum..2_bup
  aic_ <- tvar_res_i$aic
  indicator <- tvar_res_i$sd_radf_sum
  predictions <- tvar_res_i$predictions_1
  sides <- vector("integer", length(predictions))
  sides <- vector("integer", length(returns))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(threshold_2[i-1]) || is.na(threshold_2[i-2])) {
      sides[i] <- NA
    # } else if (indicator[i-1] > threshold_2[i-1]) {
    # } else if (indicator[i-1] > threshold_2[i-1] & coef_top_1[i-1] < 0) {
      # } else if (indicator[i-1] > threshold_2[i-1] & predictions[i-1] < 0.01) {
    # } else if (aic_[i-1] > -4500) {
      sides[i] <- 0
    } else {
      sides[i] <- 1
    }
  }
  sides <- ifelse(is.na(sides), 1, sides)
  returns_strategy <- returns * sides
  returns_strategy
  Return.cumulative(returns_strategy)
}
lapply(tvar_res, function(x) Return.cumulative(tvar_backtest(x)))
window_lengths / 7 / 22
returns_strategy <- tvar_backtest(tvar_res[[2]])
PerformanceAnalytics::Return.cumulative(returns_strategy)
charts.PerformanceSummary(xts(cbind(returns, returns_strategy), order.by = tvar_res[[2]]$time))
charts.PerformanceSummary(xts(tail(cbind(returns, returns_strategy), 2000), order.by = tail(tvar_res[[2]]$time, 2000)))

# save data for QC
qc_data <- cbind.data.frame(time = optimization_data$time,
                            indicator = indicator,
                            threshold = thresholds)
qc_data <- as.data.table(qc_data)
cols <- colnames(qc_data)[2:ncol(qc_data)]
qc_data[, (cols) := lapply(.SD, shift), .SDcols = cols]
qc_data <- na.omit(qc_data)
qc_data[, time := with_tz(time, "America/New_York")]
qc_data[, time := as.character(time)]
bl_endp_key <- storage_endpoint(Sys.getenv("BLOB-ENDPOINT-SNP"), Sys.getenv("BLOB-KEY-SNP"))
cont <- storage_container(bl_endp_key, "qc-backtest")
time_ <- format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
file_name <- paste0("exuber_tvar_", time_, ".csv")
storage_write_csv(qc_data, cont, file_name, col_names = FALSE)
print(file_name)



# OPTIMIZATION RESULTS ----------------------------------------------------
# import results
opt_raw <- fread("https://snpmarketdata.blob.core.windows.net/qc-optimization/determined_blue_hamster.csv")
setorder(opt_raw, -`Sharpe Ratio`)

# create heatmap
ggplot(opt_raw, aes(x = `radf-threshold`, y = `radf-sma-width`, fill = `Sharpe Ratio`)) +
  geom_raster()

# filter low sma
opt_raw <- opt_raw[`radf-sma-width` < 30]

# most frequent threshold in 100 best backtests
opt_raw[1:100, .N, by = `radf-threshold`][order(N, decreasing = TRUE)]

# optimization results for best threshold
g1 <- ggplot(opt_raw[`radf-threshold` == 2.7], aes(x = `radf-sma-width`, y =  `Sharpe Ratio`)) +
  geom_point() +
  # scale_y_continuous(labels = scales::percent_format(accuracy = 1)) +
  ylab("Sharpe Ratio") +
  xlab("SMA width") +
  ggtitle("3.0")
g2 <- ggplot(opt_raw[`radf-threshold` == 3.0], aes(x = `radf-sma-width`, y =  `Sharpe Ratio`)) +
  geom_point() +
  ylab("Sharpe Ratio") +
  xlab("SMA width") +
  ggtitle("2.70")
g1 / g2

# optimization results for best threshold
g1 <- ggplot(opt_raw[`radf-sma-width` == 1], aes(x = `radf-threshold`, y =  `Sharpe Ratio`)) +
  geom_point() +
  # scale_y_continuous(labels = scales::percent_format(accuracy = 1)) +
  ylab("Sharpe Ratio") +
  xlab("Threshold") +
  ggtitle("SMA 1")
g2 <- ggplot(opt_raw[`radf-sma-width` == 2], aes(x = `radf-threshold`, y =  `Sharpe Ratio`)) +
  geom_point() +
  ylab("Threshold") +
  xlab("SMA width") +
  ggtitle("SMa 2")
g3 <- ggplot(opt_raw[`radf-sma-width` == 3], aes(x = `radf-threshold`, y =  `Sharpe Ratio`)) +
  geom_point() +
  ylab("Sharpe Ratio") +
  xlab("Threshold") +
  ggtitle("SMA 3")
g4 <- ggplot(opt_raw[`radf-sma-width` == 4], aes(x = `radf-threshold`, y =  `Sharpe Ratio`)) +
  geom_point() +
  ylab("Sharpe Ratio") +
  xlab("Threshold") +
  ggtitle("SMA 4")
(g1 / g2) | (g3 / g4)

# best threshold
opt_raw[`radf-threshold` == 2.7]
