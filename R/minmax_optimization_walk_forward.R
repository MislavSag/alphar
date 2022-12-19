library(data.table)
library(timechange)
library(roll)
library(tiledb)
library(lubridate)
library(rtsplot)
library(TTR)
library(patchwork)
library(ggplot2)
library(AzureStor)
library(PerformanceAnalytics)
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




# UTILS -------------------------------------------------------------------
# date segments
GFC <- c("2007-01-01", "2010-01-01")
COVID <- c("2020-01-01", "2021-06-01")
AFTER_COVID <- c("2021-06-01", "2022-01-01")
NEW <- c("2022-01-01", as.character(Sys.Date()))


# IMPORT DATA -------------------------------------------------------------
# import market data
# arr <- tiledb_array("/home/matej/Desktop/stock_data/equity-usa-hour-fmpcloud-adjusted", as.data.frame = TRUE)
# system.time(hour_data <- arr[])
# tiledb_array_close(arr)
# hour_data_dt <- as.data.table(hour_data)
# hour_data_dt[, time := as.POSIXct(time, tz = "UTC")]
# 
# # keep only trading hours
# hour_data_dt <- hour_data_dt[as.integer(time_clock_at_tz(time,
#                                                          tz = "America/New_York",
#                                                          units = "hours")) %in% 10:16]
# 
# # clean data
# hour_data_dt[, returns := close / shift(close) - 1, by = "symbol"]
# hour_data_dt <- unique(hour_data_dt, by = c("symbol", "time"))
# hour_data_dt <- na.omit(hour_data_dt)
# 
# # spy data
# spy <- hour_data_dt[symbol == "SPY", .(time, close, returns)]
# spy <- unique(spy, by =  "time")
# 
# # visualize
# rtsplot(as.xts.data.table(spy[, .(time, close)]))
# rtsplot(as.xts.data.table(hour_data_dt[symbol == "AAPL", .(time, close)]))
# 


# MINMAX INDICATORS -------------------------------------------------------

# # read old data
market_data <- fread("/home/matej/Desktop/minmax_data_20221207.csv")
market_data <- as.data.table(market_data)
market_data[, time := as.POSIXct(time, tz = "UTC")]

# keep only trading hours
market_data <- market_data[as.integer(time_clock_at_tz(time,
                                                       tz = "America/New_York",
                                                       units = "hours")) %in% 10:16]

# clean data
market_data[, returns := close / shift(close) - 1, by = "symbol"]
market_data <- unique(market_data, by = c("symbol", "time"))
market_data <- na.omit(market_data)

# spy data
spy <- market_data[symbol == "SPY", .(time, close, returns)]
spy <- unique(spy, by =  "time")

rm(arr)
rm(config)
rm(context_with_config)
rm(hour_data)
rm(hour_data_dt)
gc()


# exrtreme returns
cols <- colnames(market_data)[grep("^p_9", colnames(market_data))]
cols_new <- paste0("above_", cols)
market_data[, (cols_new) := lapply(.SD, function(x) ifelse(returns > shift(x), returns - shift(x), 0)),
            by = .(symbol), .SDcols = cols]
cols <- colnames(market_data)[grep("^p_0", colnames(market_data))]
cols_new <- paste0("below_", cols)
market_data[, (cols_new) := lapply(.SD, function(x) ifelse(returns < shift(x), abs(returns - shift(x)), 0)),
            by = .(symbol), .SDcols = cols]

# crate dummy variables
cols <- colnames(market_data)[grep("^p_9", colnames(market_data))]
cols_new <- paste0("above_dummy_", cols)
market_data[, (cols_new) := lapply(.SD, function(x) ifelse(returns > shift(x), 1, 0)), by = .(symbol), .SDcols = cols]
cols <- colnames(market_data)[grep("^p_0", colnames(market_data))]
cols_new <- paste0("below_dummy_", cols)
market_data[, (cols_new) := lapply(.SD, function(x) ifelse(returns < shift(x), 1, 0)), by = .(symbol), .SDcols = cols]

# get tail risk mesures with sum
cols <- colnames(market_data)[grep("below_p|above_p|dummy", colnames(market_data))]
indicators <- market_data[, lapply(.SD, function(x) sum(x, na.rm = TRUE)), by = .(time), .SDcols = cols]
colnames(indicators) <- c("time", paste0("sum_", cols))
setorder(indicators, time)
above_sum_cols <- colnames(indicators)[grep("above", colnames(indicators))]
below_sum_cols <- colnames(indicators)[grep("below", colnames(indicators))]
excess_sum_cols <- gsub("above", "excess", above_sum_cols)
indicators[, (excess_sum_cols) := indicators[, ..above_sum_cols] - indicators[, ..below_sum_cols]]

# get tail risk mesures with sd
cols <- colnames(market_data)[grep("below_p|above_p|dummy", colnames(market_data))]
indicators_sd <- market_data[, lapply(.SD, function(x) sd(x, na.rm = TRUE)), by = .(time), .SDcols = cols]
colnames(indicators_sd) <- c("time", paste0("sd_", cols))
setorder(indicators_sd, time)
above_sum_cols <- colnames(indicators_sd)[grep("above", colnames(indicators_sd))]
below_sum_cols <- colnames(indicators_sd)[grep("below", colnames(indicators_sd))]
excess_sum_cols <- gsub("above", "excess", above_sum_cols)
indicators_sd[, (excess_sum_cols) := indicators_sd[, ..above_sum_cols] - indicators_sd[, ..below_sum_cols]]

# merge indicators and spy
indicators <- merge(indicators, indicators_sd, by = c("time"), all.x = TRUE, all.y = FALSE)
indicators <- merge(indicators, spy, by = "time")

rm(market_data)
gc()



# OPTIMIZE STRATEGY -------------------------------------------------------
# params for returns
sma_width <- 1:60
threshold <- seq(-0.1, 0, by = 0.002)
vars <- colnames(indicators)[grep("sum_excess_p", colnames(indicators))]
paramset <- expand.grid(sma_width, threshold, vars, stringsAsFactors = FALSE)
colnames(paramset) <- c('sma_width', 'threshold', "vars")



# backtst function
backtest <- function(returns, indicator, threshold, return_cumulative = TRUE) {
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

library(Rcpp)
Rcpp::cppFunction("
  double backtest_cpp(NumericVector returns, NumericVector indicator, double threshold) {
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

backtest_dummy <- function(returns, indicator, threshold, return_cumulative = TRUE) {
  sides <- vector("integer", length(indicator))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(indicator[i-1])) {
      sides[i] <- NA
    } else if (indicator[i-1] > threshold) {
      sides[i] <- 0
    } else if (indicator[i-1] <= threshold) {
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


window_lengths <- c(7 * 22 * 2, 7 * 22 * 6)


# EXPANDING BEST THRESHOLD WALK FORWARD------------------------------------------------
# optimization params
library(runner)
library(tsDyn)
library(parallel)

thresholds <- c(seq(1, 3, 0.02))
variables <- c("sum_excess_p_999_year", "sd_excess_p_999_2year", "sum_excess_p_999_halfyear")
sma_window <- c(1:10)
params <- expand.grid(thresholds, variables, sma_window, stringsAsFactors = FALSE)
colnames(params) <- c("thresholds", "variables", "sma_window")



optimization_data<- as.data.frame(indicators[, .(time, returns, sum_excess_p_999_year, sd_excess_p_999_2year, sum_excess_p_999_halfyear)])
optimization_data <- optimization_data[complete.cases(optimization_data), ]
optimization_data <- na.omit(optimization_data)


# init
returns <- optimization_data$returns
thresholds <- params[, 1]
vars <- params[, 2]
ns <- params[, 3]


# cl <- makeCluster(8)
# clusterExport(cl, varlist=c("optimization_data", "params", "backtest_cpp", "EMA", "thresholds", "ns"), envir = environment())
# clusterEvalQ(cl, {library(tsDyn)})
# 

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
  #cl=cl,
  simplify = FALSE
)
#stopCluster(cl)
gc()

# save best params object
file_name <- paste0("minmax_bestparams_2000_",
                    format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S"), "WALK_F_EMA", ".rds")
file_name <- file.path("/home/matej/Desktop/minmax", file_name)
saveRDS(best_params_window, file_name)

# buy using best params
best_prams_cleaned <- lapply(best_params_window, as.data.table)
best_prams_cleaned <- rbindlist(best_prams_cleaned, fill = TRUE)
best_prams_cleaned[, V1 := NULL]
thresholds_best <- best_prams_cleaned$thresholds
radf_values <- optimization_data[, c("sum_excess_p_999_year")]
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
sides <- vector("integer", length(returns_best))
for (i in seq_along(sides)) {
  if (i %in% c(1) || is.na(indicators_sma[i-1])) {
    sides[i] <- 1
  } else if (indicators_sma[i-1] > thresholds_best[i-1] & indicators_sma[i-1] > 1.8)  {
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

