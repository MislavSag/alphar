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
library(DescTools)



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

# read old data
# market_data <- fread("/home/matej/Desktop/minmax_data_20221207.csv")
market_data <- fread("D:/risks/minmax/minmax_data_20221207.csv")
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


X <- as.data.frame(indicators[, .(returns, sum_excess_p_999_halfyear)])
X <- na.omit(X)
X$sum_excess_p_999_halfyear <- Winsorize(X$sum_excess_p_999_halfyear, probs = c(0.0001, 0.9999))
plot(X$sum_excess_p_999_halfyear, type = "l")

library(runner)
library(tsDyn)
library(parallel)
window_lengths <- c(7 * 22 * 6)

cl <- makeCluster(4)
clusterExport(cl, "X", envir = environment())
clusterEvalQ(cl, {library(tsDyn)})
roll_preds <- lapply(window_lengths, function(w) {
  runner(
    x = X,
    f = function(x) {
      # debug
      # x = X[1:500, ]

      # # TVAR (1)
      tv1 <- tryCatch(
        TVAR(data = x,
             lag = 3,       # Number of lags to include in each regime
             model = "TAR", # Whether the transition variable is taken in levels (TAR) or difference (MTAR)
             nthresh = 2,   # Number of thresholds
             thDelay = 1,   # 'time delay' for the threshold variable
             trim = 0.05,   # trimming parameter indicating the minimal percentage of observations in each regime
             mTh = 2,       # combination of variables with same lag order for the transition variable. Either a single value (indicating which variable to take) or a combination
             plot = FALSE),
        error = function(e) NULL)
      if (is.null(tv1)) {
        tv1_pred_onestep <- NA
      } else {
        test <- tryCatch({
          tv1_pred <- predict(tv1)[, 1]
          names(tv1_pred) <- paste0("predictions_", 1:5)
          data.frame(threshold = tv1$model.specific$Thresh,
                     predictions = data.frame(as.list(tv1_pred)))
        }, error = function(e) NULL)
        if (is.null(test)) {
          y1 <<- x
          y2 <<- tv1
          print(tv1)
          print(x)
          stop("Error")
        }
        return(test)
      }
    },
    k = w,
    lag = 0L,
    cl = cl,
    na_pad = TRUE
  )
})
stopCluster(cl)
gc()

#save results
file_name <- paste0("minmax_optimization", format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S"), ".rds")
file_name <- file.path("/home/matej/Desktop/minmax", file_name)
saveRDS(roll_preds, file_name)

# read results
# list.files("D:/features")
#roll_preds <- readRDS(file.path("D:/features", "exuber_threshold2_20221128054703.rds"))


# extract info from object
roll_results <- lapply(roll_preds, function(x) lapply(x, as.data.table))
roll_results <- lapply(roll_preds, function(l) {
  lapply(l, function(x) {
    if (length(x) > 1) {
      threshold_1 <- x[1, 1]
      threshold_2 <- x[2, 1]
      predictions <- x[1, 2:6]
      return(cbind.data.frame(threshold_1, threshold_2, predictions))
    } else {
      na_df <- cbind.data.frame(t(rep(NA, 7)))
      colnames(na_df) <- c("threshold_1", "threshold_2",
                           "predictions.predictions_1",
                           "predictions.predictions_2",
                           "predictions.predictions_3",
                           "predictions.predictions_4",
                           "predictions.predictions_5")
      return(na_df)
    }
  })
})
roll_results <- lapply(roll_results, rbindlist, fill = TRUE)
tvar_res <- lapply(roll_results, function(x){
  cbind(indicators[1:nrow(x), .(time, returns, sum_excess_p_999_halfyear)], x)
})
tvar_res[[1]]$sum_excess_p_999_halfyear <- Winsorize(tvar_res[[1]]$sum_excess_p_999_halfyear,
                                                     probs = c(0.0001, 0.9999))


# visualize
ggplot(tvar_res[[1]], aes(time)) +
  geom_line(aes(y = threshold_1), color = "green") +
  geom_line(aes(y = threshold_2), color = "red") +
  geom_line(aes(y = sum_excess_p_999_halfyear))
ggplot(tvar_res[[1]][time %between% c("2021-01-01", "2021-10-01")], aes(time)) +
  geom_line(aes(y = threshold_1), color = "green") +
  geom_line(aes(y = threshold_2), color = "red") +
  geom_line(aes(y = sum_excess_p_999_halfyear))
# tvar_res<- as.data.frame(tvar_res)

# threshold based backtest
tvar_backtest <- function(tvar_res) {
  returns <- tvar_res$returns
  threshold_1 <- tvar_res$threshold_1
  threshold_2 <- tvar_res$threshold_2
  indicator <- tvar_res$sum_excess_p_999_halfyear
  predictions <- tvar_res$predictions.predictions_1
  sides <- vector("integer", length(predictions))
  sides <- vector("integer", length(returns))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(threshold_2[i-1]) || is.na(indicator[i-1])) {
      sides[i] <- NA
    } else if (indicator[i-1] < threshold_1[i-1]) {
      # } else if (indicator[i-1] > threshold_2[i-1] & abs(predictions[i-1]) < 0.01) {
      sides[i] <- 1
    } else {
      sides[i] <- 0
    }
  }
  sides <- ifelse(is.na(sides), 1, sides)
  returns_strategy <- returns * sides
  returns_strategy
}
lapply(tvar_res, function(x) Return.cumulative(tvar_backtest(x)))
returns_strategy <- tvar_backtest(tvar_res[[1]])
Return.cumulative(returns_strategy)
returns <- tvar_res[[1]]$returns
charts.PerformanceSummary(xts(cbind(returns, returns_strategy), order.by = tvar_res[[1]]$time))
charts.PerformanceSummary(xts(tail(cbind(returns, returns_strategy), 5000), order.by = tail(tvar_res[[1]]$time, 5000)))

