library(data.table)
# library(timechange)
# library(roll)
# library(tiledb)
# library(lubridate)
# library(rtsplot)
# library(TTR)
# library(patchwork)
# library(ggplot2)
# library(AzureStor)
# library(PerformanceAnalytics)



# UTILS -------------------------------------------------------------------
# date segments
GFC         = c("2007-01-01", "2010-01-01")
COVID       = c("2020-01-01", "2021-06-01")
AFTER_COVID = c("2021-06-01", "2022-01-01")
CORECTION   = c("2022-01-01", "2022-08-01")
NEW         = c("2022-08-01", as.character(Sys.Date()))


# IMPORT DATA -------------------------------------------------------------
# Import

# import market data
arr <- tiledb_array("D:/equity-usa-hour-fmpcloud-adjusted",
                    as.data.frame = TRUE,
                    query_layout = "UNORDERED")
system.time(hour_data <- arr[])
tiledb_array_close(arr)
hour_data_dt <- as.data.table(hour_data)
hour_data_dt[, time := as.POSIXct(time, tz = "UTC")]
setorder(hour_data_dt, symbol, time)

# change timezone and keep only trading hours
hour_data_dt[, time := with_tz(time, tzone = "America/New_York")]
hour_data_dt <- hour_data_dt[as.ITime(time) %between% c(as.ITime("09:30:00"),
                                                        as.ITime("16:00:00"))]

# # keep only trading hours (OLD WAY)
# hour_data_dt <- hour_data_dt[as.integer(time_clock_at_tz(time,
#                                                          tz = "America/New_York",
#                                                          units = "hours")) %in% 10:16]

# clean data
hour_data_dt[, returns := close / shift(close) - 1, by = "symbol"]
hour_data_dt <- unique(hour_data_dt, by = c("symbol", "time"))
hour_data_dt <- na.omit(hour_data_dt)

# spy data
spy <- hour_data_dt[symbol == "SPY", .(time, close, returns)]
spy <- unique(spy, by =  "time")

# visualize
rtsplot(as.xts.data.table(spy[, .(time, close)]))
rtsplot(as.xts.data.table(hour_data_dt[symbol == "AAPL", .(time, close)]))



# MINMAX INDICATORS -------------------------------------------------------
# calculate rolling quantiles
market_data <- copy(hour_data_dt)
market_data[, p_999_4year := roll::roll_quantile(returns, 255*8*4, p = 0.999), by = .(symbol)]
market_data[, p_001_4year := roll::roll_quantile(returns, 255*8*4, p = 0.001), by = .(symbol)]
market_data[, p_999_2year := roll::roll_quantile(returns, 255*8*2, p = 0.999), by = .(symbol)]
market_data[, p_001_2year := roll::roll_quantile(returns, 255*8*2, p = 0.001), by = .(symbol)]
market_data[, p_999_year := roll::roll_quantile(returns, 255*8, p = 0.999), by = .(symbol)]
market_data[, p_001_year := roll::roll_quantile(returns, 255*8, p = 0.001), by = .(symbol)]
market_data[, p_999_halfyear := roll::roll_quantile(returns, 255*4, p = 0.999), by = .(symbol)]
market_data[, p_001_halfyear := roll::roll_quantile(returns, 255*4, p = 0.001), by = .(symbol)]

market_data[, p_99_4year := roll::roll_quantile(returns, 255*8*4, p = 0.99), by = .(symbol)]
market_data[, p_01_4year := roll::roll_quantile(returns, 255*8*4, p = 0.01), by = .(symbol)]
market_data[, p_99_2year := roll::roll_quantile(returns, 255*8*2, p = 0.99), by = .(symbol)]
market_data[, p_01_2year := roll::roll_quantile(returns, 255*8*2, p = 0.01), by = .(symbol)]
market_data[, p_99_year := roll::roll_quantile(returns, 255*8, p = 0.99), by = .(symbol)]
market_data[, p_01_year := roll::roll_quantile(returns, 255*8, p = 0.01), by = .(symbol)]
market_data[, p_99_halfyear := roll::roll_quantile(returns, 255*4, p = 0.99), by = .(symbol)]
market_data[, p_01_halfyear := roll::roll_quantile(returns, 255*4, p = 0.01), by = .(symbol)]

market_data[, p_97_4year := roll::roll_quantile(returns, 255*8*4, p = 0.97), by = .(symbol)]
market_data[, p_03_4year := roll::roll_quantile(returns, 255*8*4, p = 0.03), by = .(symbol)]
market_data[, p_97_2year := roll::roll_quantile(returns, 255*8*2, p = 0.97), by = .(symbol)]
market_data[, p_03_2year := roll::roll_quantile(returns, 255*8*2, p = 0.03), by = .(symbol)]
market_data[, p_97_year := roll::roll_quantile(returns, 255*8, p = 0.97), by = .(symbol)]
market_data[, p_03_year := roll::roll_quantile(returns, 255*8, p = 0.03), by = .(symbol)]
market_data[, p_97_halfyear := roll::roll_quantile(returns, 255*4, p = 0.97), by = .(symbol)]
market_data[, p_03_halfyear := roll::roll_quantile(returns, 255*4, p = 0.03), by = .(symbol)]

market_data[, p_95_4year := roll::roll_quantile(returns, 255*8*4, p = 0.95), by = .(symbol)]
market_data[, p_05_4year := roll::roll_quantile(returns, 255*8*4, p = 0.05), by = .(symbol)]
market_data[, p_95_2year := roll::roll_quantile(returns, 255*8*2, p = 0.95), by = .(symbol)]
market_data[, p_05_2year := roll::roll_quantile(returns, 255*8*2, p = 0.05), by = .(symbol)]
market_data[, p_95_year := roll::roll_quantile(returns, 255*8, p = 0.95), by = .(symbol)]
market_data[, p_05_year := roll::roll_quantile(returns, 255*8, p = 0.05), by = .(symbol)]
market_data[, p_95_halfyear := roll::roll_quantile(returns, 255*4, p = 0.95), by = .(symbol)]
market_data[, p_05_halfyear := roll::roll_quantile(returns, 255*4, p = 0.05), by = .(symbol)]

# save market data
# time_ <- format(Sys.Date(), format = "%Y%m%d")
# file_name <- paste0("D:/risks/minmax/minmax_data_", time_,".csv")
# fwrite(market_data, file_name)
# zip::zipr(zipfile = "D:/risks/minmax/minmax_data_20221207.zip",
#           files = "D:/risks/minmax/minmax_data_20221207.csv",
#           compression_level = 9)

# # read old data
# list.files("D:/risks/minmax")
# market_data <- fread("D:/risks/minmax/minmax_data_20221207.csv")

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

# help function to caluclate diff summy sum
get_diff_dummy = function(col = "p_01_year") {

  # get dt and dt lag
  cols = c("symbol", "time", col)
  dt = dcast(market_data[, ..cols], time ~ symbol, value.var = col)
  setorder(dt, "time")
  dt_lag = dt[, lapply(.SD, function(x) shift(x, 1)), .SDcols = colnames(dt)[2:ncol(dt)]]
  dt_lag = cbind(dt[, time], dt_lag)

  # get dt diff
  dt_diff = dt[, 2:ncol(dt)] - dt_lag[, 2:ncol(dt)]
  dt_diff = cbind(dt[, time], dt_diff)

  # get dt diff dummy
  cols = colnames(dt_diff)[2:ncol(dt_diff)]
  dt_diff_dummy = dt_diff[, lapply(.SD, function(x) ifelse(!is.na(x) & x != 0, 1, 0)), .SDcols = cols]

  # get dt diff dummy sum
  dt_diff_dummy = dt_diff_dummy[, dummy_sum := rowSums(.SD), .SDcols = cols]
  dt_diff_dummy = cbind(time = dt[, time], dt_diff_dummy)
  dt_diff_dummy = dt_diff_dummy[, .(dummy_sum)]
  setnames(dt_diff_dummy, paste0(col, "_", "dummy_sum"))

  return(dt_diff_dummy)
}

# get diff dummy some for all columns
cols <- colnames(market_data)[grep("^p_9|^p_0", colnames(market_data))]
cols_new <- paste0("diff_dummy_", cols)
indicators_diff_dummy = market_data[, lapply(cols, function(x) get_diff_dummy(x))]
time_ = sort(unique(market_data[, time]))
indicators_diff_dummy = cbind(time = time_, indicators_diff_dummy)
setnames(indicators_diff_dummy, c("time", cols_new))

# merge indicators and spy
indicators <- merge(indicators, indicators_sd, by = c("time"), all.x = TRUE, all.y = FALSE)
indicators <- merge(indicators, indicators_diff_dummy, by = c("time"), all.x = TRUE, all.y = FALSE)

# # get spy data
# arr <- tiledb_array("D:/equity-usa-hour-fmpcloud-adjusted",
#                     as.data.frame = TRUE,
#                     query_layout = "UNORDERED",
#                     selected_ranges = list(symbol = cbind("SPY", "SPY")))
# system.time(spy_data <- arr[])
# tiledb_array_close(arr)
# spy_dt <- as.data.table(spy_data)
# spy_dt[, time := as.POSIXct(time, tz = "UTC")]
# setorder(spy_dt, symbol, time)
# spy_dt[, time := with_tz(time, tzone = "America/New_York")]
# spy_dt <- spy_dt[as.ITime(time) %between% c(as.ITime("09:30:00"),
#                                             as.ITime("16:00:00"))]
# spy_dt[, returns := close / shift(close) - 1]
# spy_dt <- na.omit(spy_dt)
# spy <- spy_dt[, .(time, close, returns)]

# merge spy and indicators
indicators <- merge(indicators, spy, by = "time")



# VISUALIZATION -----------------------------------------------------------
#
ggplot(indicators, aes(x = time)) +
  geom_line(aes(y = sum_above_p_999_2year))
ggplot(indicators, aes(x = time)) +
  geom_line(aes(y = sum_below_p_001_2year))

# visualize for sample predictors
sample_ = copy(indicators)
indicators[, .(time, returns, sum_above_p_999_year, sum_below_p_001_year)]
sample_[, buy := ifelse(shift(sum_above_p_999_year, 1, type = "lag") > 0.8, 1, 0)]
sample_[, sell := ifelse(shift(sum_below_p_001_year, 1, type = "lag") > 0.8, 1, 0)]
table(sample_$buy)
v_buy <- sample_[buy == 1, time, ]
v_sell <- sample_[sell == 1, time, ]
ggplot(sample_, aes(x = time)) +
  geom_line(aes(y = close)) +
  geom_vline(xintercept = v_buy, color = "green") +
  geom_vline(xintercept = v_sell, color = "red")
ggplot(sample_[time %between% GFC], aes(x = time)) +
  geom_line(aes(y = close)) +
  geom_vline(xintercept = v_buy, color = "green") +
  geom_vline(xintercept = v_sell, color = "red")
ggplot(sample_[time %between% COVID], aes(x = time)) +
  geom_line(aes(y = close)) +
  geom_vline(xintercept = v_buy, color = "green") +
  geom_vline(xintercept = v_sell, color = "red")
ggplot(sample_[time %between% AFTER_COVID], aes(x = time)) +
  geom_line(aes(y = close)) +
  geom_vline(xintercept = v_buy, color = "green") +
  geom_vline(xintercept = v_sell, color = "red")
ggplot(sample_[time %between% NEW], aes(x = time)) +
  geom_line(aes(y = close)) +
  geom_vline(xintercept = v_buy, color = "green") +
  geom_vline(xintercept = v_sell, color = "red")

# visualize dummy
# dummmy variables
ggplot(indicators, aes(x = time)) +
  geom_line(aes(y = sum_excess_dummy_p_99_year))
ggplot(indicators[as.Date(time) %between% COVID], aes(x = time)) +
  geom_line(aes(y = sum_excess_dummy_p_99_year))
ggplot(indicators[as.Date(time) %between% c("2020-02-01", "2020-03-01")], aes(x = time)) +
  geom_line(aes(y = sum_excess_dummy_p_99_year))
ggplot(indicators[as.Date(time) %between% c("2020-02-01", "2020-03-01")], aes(x = time)) +
  geom_line(aes(y = SMA(sum_excess_dummy_p_99_year, 10)))
ggplot(indicators, aes(x = time)) +
  geom_line(aes(y = sum_below_p_001_2year))


sample_ = copy(indicators)
indicators[, .(time, returns, sum_above_dummy_p_99_2year, sum_below_dummy_p)]
sample_[, buy := ifelse(shift(sum_above_p_999_year, 1, type = "lag") > 0.4, 1, 0)]
sample_[, sell := ifelse(shift(sum_below_p_001_year, 1, type = "lag") > 0.4, 1, 0)]
table(sample_$buy)
v_buy <- sample_[buy == 1, time, ]
v_sell <- sample_[sell == 1, time, ]
ggplot(sample_, aes(x = time)) +
  geom_line(aes(y = close)) +
  geom_vline(xintercept = v_buy, color = "green") +
  geom_vline(xintercept = v_sell, color = "red")
ggplot(sample_[date %between% GFC], aes(x = date)) +
  geom_line(aes(y = close)) +
  geom_vline(xintercept = v_buy, color = "green") +
  geom_vline(xintercept = v_sell, color = "red")

# visualiye diff dummy
ggplot(indicators_diff_dummy, aes(x = time, y =  diff_dummy_p_001_2year)) +
  geom_line()
ggplot(indicators_diff_dummy, aes(x = time, y = SMA(diff_dummy_p_001_2year, 15))) +
  geom_line()
ggplot(indicators_diff_dummy[30000:32000], aes(x = time, y = SMA(diff_dummy_p_001_2year, 15))) +
  geom_line()



# OPTIMIZE STRATEGY -------------------------------------------------------
# params for returns
sma_width <- 1:60
threshold <- seq(-0.1, 0, by = 0.002)
vars <- colnames(indicators)[grep("sum_excess_p", colnames(indicators))]
paramset <- expand.grid(sma_width, threshold, vars, stringsAsFactors = FALSE)
colnames(paramset) <- c('sma_width', 'threshold', "vars")

# params for dummies
threshold <- seq(0, 60, by = 1)
vars <- colnames(indicators)[grep("sum_excess_dumm", colnames(indicators))]
paramset_dummy <- expand.grid(threshold, vars, stringsAsFactors = FALSE)
colnames(paramset_dummy) <- c('threshold', "vars")
# v2
sma_width <- 1:60
threshold <- seq(0, 60, by = 1)
vars <- colnames(indicators)[grep("sum_excess_dumm", colnames(indicators))]
paramset_dummy <- expand.grid(sma_width, threshold, vars, stringsAsFactors = FALSE)
colnames(paramset_dummy) <- c('sma_width', 'threshold', "vars")

# params for dummies sd
sma_width <- 1:60
threshold <- seq(0.001, 0.10, by = 0.001)
vars <- colnames(indicators)[grep("sd_excess_dumm", colnames(indicators))]
paramset_dummy_sd <- expand.grid(sma_width, threshold, vars, stringsAsFactors = FALSE)
colnames(paramset_dummy_sd) <- c("sma_width", 'threshold', "vars")

# params for dummy diff
sma_width <- 1:50
threshold <- 1:200
vars <- colnames(indicators)[grep("diff_dummy_p_0", colnames(indicators))]
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

library(Rcpp)
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

# backtset
# plan(multiprocess(workers = 4L))
cum_returns_f <- function(paramset) {
  cum_returns <- vapply(1:nrow(paramset), function(x) {
    excess_sma <- SMA(indicators[, get(paramset[x, 3])], paramset[x, 1])
    results <- backtest_cpp(indicators$returns, excess_sma, paramset[x, 2])
    return(results)
  }, numeric(1))
  results <- as.data.table(cbind(paramset, cum_returns))
}
cum_returns_dt <- cum_returns_f(paramset)
setorder(cum_returns_dt, cum_returns)

# # backtset rsi
# cum_returns_rsi <- function(paramset) {
#   cum_returns <- vapply(1:nrow(paramset), function(x) {
#     excess_sma <- RSI(indicators[, get(paramset[x, 3])], paramset[x, 1])
#     results <- backtest_dummy(indicators$returns, excess_sma, paramset[x, 2])
#     return(results)
#   }, numeric(1))
#   results <- as.data.table(cbind(paramset, cum_returns))
# }
# cum_returns_rsi_dt <- cum_returns_rsi(paramset_rsi)
# setorder(cum_returns_rsi_dt, cum_returns)

# backtest optimization for dummy indicators
cum_returns_dummy <- function(paramset) {
  cum_returns <- vapply(1:nrow(paramset), function(x) {
    excess_sma <- SMA(indicators[, get(paramset[x, 3])], paramset[x, 1])
    results <- backtest_dummy(indicators$returns, excess_sma, paramset[x, 2])
    return(results)
  }, numeric(1))
  results <- as.data.table(cbind(paramset, cum_returns))
}
cum_returns_dummy_dt <- cum_returns_dummy(paramset_dummy)
setorder(cum_returns_dummy_dt, cum_returns)

# # backtest optimization for dummy sd indicators
# cum_returns_sd <- function(paramset) {
#   cum_returns <- vapply(1:nrow(paramset), function(x) {
#     excess_sma <- SMA(na.omit(abs(indicators[, get(paramset[x, 3])])), paramset[x, 1])
#     results <- backtest_dummy(indicators$returns, excess_sma, paramset[x, 2])
#     return(results)
#   }, numeric(1))
#   results <- as.data.table(cbind(paramset, cum_returns))
# }
# cum_returns_sd_dt <- cum_returns_sd(paramset_dummy_sd)
# setorder(cum_returns_sd_dt, cum_returns)

# backtest diff dummy
cum_returns_f <- function(paramset) {
  cum_returns <- vapply(1:nrow(paramset), function(x) {
    excess_sma <- SMA(indicators[, get(paramset[x, 3])], paramset[x, 1])
    results <- backtest_cpp(indicators$returns, excess_sma, paramset[x, 2])
    return(results)
  }, numeric(1))
  results <- as.data.table(cbind(paramset, cum_returns))
}
cum_returns_dt <- cum_returns_f(paramset)
setorder(cum_returns_dt, cum_returns)


# n best
head(cum_returns_dt, 50)
tail(cum_returns_dt, 100)
# head(cum_returns_rsi_dt, 50)
# tail(cum_returns_rsi_dt, 50)
head(cum_returns_dummy_dt, 50)
tail(cum_returns_dummy_dt, 50)
head(cum_returns_sd_dt, 50)
tail(cum_returns_sd_dt, 50)

# summary statistics
# returns
dcast(cum_returns_dt, vars ~ ., value.var = "cum_returns", fun.aggregate = median)
dcast(cum_returns_dt, vars ~ ., value.var = "cum_returns", fun.aggregate = mean)
dcast(cum_returns_dt, vars ~ ., value.var = "cum_returns", fun.aggregate = quantile, p = 0.10)
dcast(cum_returns_dt, vars ~ ., value.var = "cum_returns", fun.aggregate = quantile, p = 0.90)
dcast(cum_returns_dt, vars ~ ., value.var = "cum_returns", fun.aggregate = quantile, p = 0.80)
skimr::skim(cum_returns_dt)
# dummy
dcast(cum_returns_dummy_dt, vars ~ ., value.var = "cum_returns", fun.aggregate = median)
dcast(cum_returns_dummy_dt, vars ~ ., value.var = "cum_returns", fun.aggregate = mean)
dcast(cum_returns_dummy_dt, vars ~ ., value.var = "cum_returns", fun.aggregate = quantile, p = 0.10)
dcast(cum_returns_dummy_dt, vars ~ ., value.var = "cum_returns", fun.aggregate = quantile, p = 0.90)
dcast(cum_returns_dummy_dt, vars ~ ., value.var = "cum_returns", fun.aggregate = quantile, p = 0.80)
skimr::skim(cum_returns_dummy_dt)
ggplot(cum_returns_dummy_dt, aes(cum_returns)) +
  geom_histogram() +
  geom_vline(xintercept = PerformanceAnalytics::Return.cumulative(indicators$returns), color = "red")
ggplot(cum_returns_dummy_dt[grepl("excess_dummy_p_97", vars)], aes(cum_returns)) +
  geom_histogram() +
  geom_vline(xintercept = PerformanceAnalytics::Return.cumulative(indicators$returns), color = "red") +
  facet_grid(cols = vars(vars))

# plots
# returns
ggplot(cum_returns_dt, aes(cum_returns)) +
  geom_histogram() +
  geom_vline(xintercept = PerformanceAnalytics::Return.cumulative(indicators$returns), color = "red")
ggplot(cum_returns_dt[sma_width %in% 1:10 & grepl("diff_dummy_p_03", vars)], aes(cum_returns)) +
  geom_histogram() +
  geom_vline(xintercept = PerformanceAnalytics::Return.cumulative(indicators$returns), color = "red") +
  facet_grid(cols = vars(vars))
ggplot(cum_returns_dt, aes(x = sma_width, y = threshold, fill = cum_returns)) +
  geom_tile()
ggplot(cum_returns_dt[sma_width %in% 1:10 & grepl("diff_dummy_p_03", vars)],
       aes(x = sma_width, y = threshold, fill = cum_returns)) +
  geom_tile() +
  facet_grid(cols = vars(vars))
ggplot(cum_returns_dt[sma_width %in% 1:10 & grepl("diff_dummy_p_01", vars)],
       aes(x = sma_width, y = threshold, fill = cum_returns)) +
  geom_tile() +
  facet_grid(cols = vars(vars))
ggplot(cum_returns_dt[sma_width %in% 1:10 & grepl("excess_p_99_", vars)],
       aes(x = sma_width, y = threshold, fill = cum_returns)) +
  geom_tile() +
  facet_grid(cols = vars(vars))
ggplot(cum_returns_dt[sma_width %in% 1:10 & grepl("excess_p_999", vars)],
       aes(x = sma_width, y = threshold, fill = cum_returns)) +
  geom_tile() +
  facet_grid(cols = vars(vars))
ggplot(cum_returns_dt, aes(x = vars, y = sma_width, fill = cum_returns)) +
  geom_tile() +
  theme(text = element_text(size=12),
        axis.text.x = element_text(angle=90, hjust=1))
ggplot(cum_returns_dt[sma_width %in% 1:10 & threshold %between% c(-0.01, -0.001)],
       aes(x = sma_width, y = threshold, fill = cum_returns)) +
  geom_tile()
ggplot(cum_returns_dt[sma_width %in% 1:10 & threshold %between% c(-0.01, -0.001) & vars == "sum_excess_p_97_halfyear"],
       aes(x = sma_width, y = threshold, fill = cum_returns)) +
  geom_tile()

# best vriable
best_var <- "diff_dummy_p_03_year"
ggplot(cum_returns_dt[vars == best_var], aes(cum_returns)) +
  geom_histogram() +
  geom_vline(xintercept = PerformanceAnalytics::Return.cumulative(indicators$returns), color = "red")
ggplot(cum_returns_dt[vars == best_var],
       aes(x = sma_width, y = threshold, fill = cum_returns)) +
  geom_tile() +
  facet_grid(cols = vars(vars))

# best var and sma width
best_var <- "diff_dummy_p_03_year"
sma_width_best <- 6
ggplot(cum_returns_dt[vars == best_var & sma_width == sma_width_best], aes(cum_returns)) +
  geom_histogram() +
  geom_vline(xintercept = PerformanceAnalytics::Return.cumulative(indicators$returns), color = "red")
ggplot(cum_returns_dt[vars == best_var],
       aes(x = sma_width, y = threshold, fill = cum_returns)) +
  geom_tile() +
  facet_grid(cols = vars(vars))

# plots for dummies
ggplot(cum_returns_dummy, aes(cum_returns)) +
  geom_histogram() +
  geom_vline(xintercept = PerformanceAnalytics::Return.cumulative(indicators$returns), color = "red")

# best backtest
sma_width_param <- 6
threshold_param <- 20
vars_param <- "diff_dummy_p_03_year"
excess_sma <- SMA(indicators[, get(vars_param)], sma_width_param)
strategy_returns <- backtest(indicators$returns, excess_sma, threshold_param, return_cumulative = FALSE)
charts.PerformanceSummary(xts(cbind(indicators$returns, strategy_returns), order.by = indicators$time))
charts.PerformanceSummary(xts(cbind(indicators$returns[30000:nrow(indicators)], strategy_returns[30000:nrow(indicators)]),
                              order.by = indicators$date[30000:nrow(indicators)]))

# add negatibe trend condition
backtest <- function(returns, indicator, threshold, return_cumulative = TRUE) {
  sides <- vector("integer", length(indicator))
  for (i in seq_along(sides)) {
    # print(i)
    if (i %in% c(1:35) || is.na(indicator[i-1]) || is.na(returns[i-35])) {
      sides[i] <- NA
    } else if (indicator[i-1] > threshold & (returns[i-1] > 0)) { # & ((returns[i-1] / returns[i-35] - 1) < 0)
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
sma_width_param <- 6
threshold_param <- 10
vars_param <- "diff_dummy_p_03_year"
excess_sma <- SMA(indicators[, get(vars_param)], sma_width_param)
strategy_returns <- backtest(indicators$returns, excess_sma, threshold_param, return_cumulative = FALSE)
charts.PerformanceSummary(xts(cbind(indicators$returns, strategy_returns), order.by = indicators$time))
charts.PerformanceSummary(xts(cbind(indicators$returns[30000:nrow(indicators)], strategy_returns[30000:nrow(indicators)]),
                              order.by = indicators$date[30000:nrow(indicators)]))


# best backtest for dummy
threshold_param <- 3
vars_param <- "sum_excess_dummy_p_97_year"
strategy_returns <- backtest_dummy(indicators$returns, indicators[, get(vars_param)], threshold_param, return_cumulative = FALSE)
charts.PerformanceSummary(xts(cbind(indicators$returns, strategy_returns), order.by = indicators$time))


# best backtest for dummy sd
sma_width_param <- 24
threshold <- 0.05
vars_param <- "sum_excess_dummy_p_97_year"
excess_sma <- SMA(na.omit(abs(indicators[, get(paramset[x, 3])])), paramset[x, 1])
strategy_returns <- backtest_dummy(indicators$returns, excess_sma, threshold_param, return_cumulative = FALSE)
charts.PerformanceSummary(xts(cbind(indicators$returns, strategy_returns), order.by = indicators$datetime))


# save best strategy to azure
keep_cols <- colnames(indicators)[grep("datetime|sum_excess_p", colnames(indicators))]
indicators_azure <- indicators[, ..keep_cols]
cols <- colnames(indicators_azure)[2:ncol(indicators_azure)]
indicators_azure[, (cols) := lapply(.SD, shift), .SDcols = cols]
indicators_azure <- na.omit(indicators_azure)
file_name <- "D:/risks/minmax/minmax.csv"
fwrite(indicators_azure, file_name, col.names = FALSE, dateTimeAs = "write.csv")
bl_endp_key <- storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), key=Sys.getenv("BLOB-KEY"))
cont <- storage_container(bl_endp_key, "qc-backtest")
storage_upload(cont, file_name, basename(file_name))
# https://contentiobatch.blob.core.windows.net/qc-backtest/minmax.csv?sp=r&st=2022-01-01T11:15:54Z&se=2023-01-01T19:15:54Z&sv=2020-08-04&sr=b&sig=I0Llnk3ELMOJ7%2FJ2i2VzQOxCFEOTbmFaEHzXnzLJ7ZQ%3D

# save best strategy to azure
keep_cols <- colnames(indicators)[grep("time|sum_excess_dummy", colnames(indicators))]
indicators_azure <- indicators[, ..keep_cols]
cols <- colnames(indicators_azure)[2:ncol(indicators_azure)]
indicators_azure[, (cols) := lapply(.SD, shift), .SDcols = cols]
indicators_azure <- na.omit(indicators_azure)
indicators_azure[, time := with_tz(time, "America/New_York")]
file_name <- "D:/risks/minmax/minmax_dummy.csv"
fwrite(indicators_azure, file_name, col.names = FALSE, dateTimeAs = "write.csv")
SNP_KEY = "0M4WRlV0/1b6b3ZpFKJvevg4xbC/gaNBcdtVZW+zOZcRi0ZLfOm1v/j2FZ4v+o8lycJLu1wVE6HT+ASt0DdAPQ=="
SNP_ENDPOINT = "https://snpmarketdata.blob.core.windows.net/"
bl_endp_key <- storage_endpoint(SNP_ENDPOINT, key=SNP_KEY)
cont <- storage_container(bl_endp_key, "qc-backtest")
storage_upload(cont, file_name, basename(file_name))



# IMPORTANT FEATURES ------------------------------------------------------
# extract important features with gausscov package
library(gausscov)

# define feature matrix
cols_keep <- c(colnames(indicators)[2:(ncol(indicators)-3)], "returns")
X <- as.matrix(na.omit(indicators[, ..cols_keep]))
dim(X)

# f1st
f1st_fi_ <- f1st(X[, ncol(X)], X[, -ncol(X)], kmn = 20, sub = TRUE)
cov_index_f1st_ <- colnames(X[, -ncol(X)])[f1st_fi_[[1]][, 1]]

# f3st_1
f3st_1_ <- f3st(X[, ncol(X)], X[, -ncol(X)], kmn = 20, m = 1)
cov_index_f3st_1 <- unique(as.integer(f3st_1_[[1]][1, ]))[-1]
cov_index_f3st_1 <- cov_index_f3st_1[cov_index_f3st_1 != 0]
cov_index_f3st_1 <- colnames(X[, -ncol(X)])[cov_index_f3st_1]

# f3st_1 m=2
f3st_2_ <- f3st(X[, ncol(X)], X[, -ncol(X)], kmn = 20, m = 2)
cov_index_f3st_2 <- unique(as.integer(f3st_2_[[1]][1, ]))[-1]
cov_index_f3st_2 <- cov_index_f3st_2[cov_index_f3st_2 != 0]
cov_index_f3st_2 <- colnames(X[, -ncol(X)])[cov_index_f3st_2]

# f3st_1 m=3
# f3st_3_ <- f3st(X[, ncol(X)], X[, -ncol(X)], kmn = 20, m = 3)
# cov_index_f3st_3_ <- unique(as.integer(f3st_3_[[1]][1, ]))[-1]
# cov_index_f3st_3 <- cov_index_f3st_3[cov_index_f3st_3 != 0]
# cov_index_f3st_3 <- colnames(X[, -ncol(X)])[cov_index_f3st_3]

# best variables
predictors <- cov_index_f3st_2[1:5]

# correlation of best predictors
plot(na.omit(indicators[, ..predictors][, 1][[1]]))
plot(na.omit(indicators[, ..predictors][, 2][[1]]))
plot(na.omit(indicators[, ..predictors][, 3][[1]]))
plot(na.omit(indicators[, ..predictors][, 4][[1]]))
plot(na.omit(indicators[, ..predictors][, 5][[1]]))
cor(as.matrix(indicators[, ..predictors]))

# VAR
library(vars)
keep_cols_var <- c("time", "returns", predictors)
X <- indicators[, ..keep_cols_var]
X <- na.omit(X)

# predictions for every period
library(runner)
roll_var <- runner(
  x = X,
  f = function(x) {
    res <- VAR(as.xts.data.table(x), p = 2, type = "both")
    p <- predict(res, n.ahead = 4, ci = 0.95)
    p <- p$fcst$returns
    data.frame(first = p[1, 1], median = median(p[, 1]), mean = mean(p[, 1]), sd = sd(p[, 1]), min = min(p[, 1]))
  },
  k = 2000,
  lag = 0L,
  na_pad = TRUE
)
predictions_var <- lapply(roll_var, as.data.table)
predictions_var <- rbindlist(predictions_var, fill = TRUE)
predictions_var[, V1 := NULL]
predictions_var <- cbind(time = X[, time], predictions_var)
predictions_var <- merge(spy, predictions_var, by = "time", all.x = TRUE, all.y = FALSE)
predictions_var <- na.omit(predictions_var)
tail(predictions_var, 10)

# backtest apply
Return.cumulative(predictions_var$returns)
backtest(predictions_var$returns, predictions_var$first, -0.0001)
backtest(predictions_var$returns, predictions_var$median, -0.0001)
backtest(predictions_var$returns, predictions_var$mean, -0.0001)
backtest(predictions_var$returns, predictions_var$min, -0.004)
backtest_dummy(predictions_var$returns, predictions_var$sd, 0.001)
x <- backtest(predictions_var$returns, predictions_var$first, -0.006, FALSE)
charts.PerformanceSummary(as.xts(cbind(predictions_var$returns, x), order.by = predictions_var$time))
x <- backtest_dummy(predictions_var$returns, predictions_var$sd, 0.001, FALSE)
charts.PerformanceSummary(as.xts(cbind(predictions_var$returns, x), order.by = predictions_var$time))



# BigVAR
library(BigVAR)
data(Y)
# Create a Basic VAR-L (Lasso Penalty) with maximum lag order p=4, 10 gridpoints with lambda optimized according to rolling validation of 1-step ahead MSFE
mod1 <- constructModel(Y,p=4,"Basic",gran=c(150,10),RVAR=FALSE,h=1,cv="Rolling",MN=FALSE,verbose=FALSE,IC=TRUE)
results=cv.BigVAR(mod1)
results
nrow(Y)
predict(results,n.ahead=1)

model <- constructModel(as.xts.data.table(X[1:100, ]),p=4,"Basic",gran=c(150,10),RVAR=FALSE,h=1,cv="Rolling",MN=FALSE,verbose=FALSE,IC=TRUE)
results=cv.BigVAR(model)
predict(results,n.ahead=1)

# predictions for every period
x <- X[1:500]
roll_var <- runner(
  x = X,
  f = function(x) {
    model <- constructModel(as.matrix(x[, -1]), p = 8, "Basic",gran=c(150,10),RVAR=FALSE,h=1,cv="Rolling",MN=FALSE,verbose=FALSE,IC=TRUE)
    model_results <- cv.BigVAR(model)
    p <- predict(model_results, n.ahead=1)
    data.frame(first = p[1])
  },
  k = 5000,
  lag = 0L,
  na_pad = TRUE
)
predictions_var <- lapply(roll_var, as.data.table)
predictions_var <- rbindlist(predictions_var, fill = TRUE)
predictions_var[, V1 := NULL]
predictions_var <- cbind(datetime = X[, datetime], predictions_var)
predictions_var <- merge(spy, predictions_var, by = "datetime", all.x = TRUE, all.y = FALSE)
predictions_var <- na.omit(predictions_var)
tail(predictions_var, 10)




# ML ----------------------------------------------------------------------

#
clf_data <- copy(indicators)
plot(clf_data$close)

# create target var
clf_data[, bin := shift(close, 8 * 1, type = "lead") / shift(close, 1, type = "lead") - 1]
clf_data[, bin := as.factor(ifelse(bin > 0, 1L, 0L))]
table(clf_data$bin)

# remove missing values
clf_data <- na.omit(clf_data)

# filter raws
clf_data[, sd_filter := QuantTools::roll_sd_filter(close, 8 * 5 * 22, 2, 1)]
table(clf_data$sd_filter)
clf_data <- clf_data[sd_filter == FALSE]

# select features vector
feature_cols <- setdiff(colnames(clf_data), c("datetime", "close", "returns", "bin", "sd_filter"))
clf_data <- clf_data[, .SD, .SDcols = c(feature_cols, "bin")]

# define task
task = as_task_classif(clf_data, target = "bin", id = "minmax")

# rpart tree
library(rpart.plot)
learner = lrn("classif.rpart", maxdepth = 4, predict_type = "prob")
learner$train(task)
predictins = learner$predict(task)
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
  terminator = trm("evals", n_evals = 25),
  fselector = fs("random_search")
)
at_rpart = AutoFSelector$new(
  learner = lrn("classif.rpart"),
  resampling = rsmp("holdout"),
  measure = msr("classif.ce"),
  terminator = trm("evals", n_evals = 25),
  fselector = fs("sequential")
)
at_rpart_genetic = AutoFSelector$new(
  learner = lrn("classif.rpart"),
  resampling = rsmp("holdout"),
  measure = msr("classif.ce"),
  terminator = trm("evals", n_evals = 25),
  fselector = fs("genetic_search")
)
grid = benchmark_grid(
  task = task,
  learner = list(at_rf, at_rpart, at_rpart_genetic),
  resampling = rsmp("cv", folds = 5)
)
bmr_fs = benchmark(grid, store_models = TRUE)
bmr_fs$aggregate(msrs(c("classif.ce", "time_train")))

# exract features
important_features <- lapply(as.data.table(bmr_fs)$learner, function(x) unlist(x$fselect_result$features))
important_features <- as.data.table(table(unlist(important_features)))
important_features <- important_features[order(N, decreasing = TRUE), ]
important_features_n <- head(important_features[, V1], 20) # top n features selected through ML
important_features_most_important <- important_features[N == max(N, na.rm = TRUE), V1] # most importnatn feature(s)
features_short <- unique(c(important_features_most_important, important_features_n))
features_long <- unique(c(important_features_most_important, important_features_n))
features_long <- features_long[!grepl("lm_", features_long)]



# TORCH -------------------------------------------------------------------
# load packages
library(torch)
torch::cuda_is_available()

# parameters
n_timesteps = 8 * 5
n_forecast = 1
batch_size = 64

# define train, validation and test set
colnames(indicators)
cols <- c("datetime", "returns", colnames(indicators)[grep("sum_below_p|sd_below_p", colnames(indicators))])
X <- indicators[, ..cols]
X <- na.omit(X)
X <- X[10:nrow(X), ]
X_train <- as.matrix(X[1:as.integer((nrow(X) * 0.7)), .SD, .SDcols = !c("datetime")])
X_validation <- as.matrix(X[(nrow(X_train)+1):as.integer((nrow(X) * 0.85)), .SD, .SDcols = !c("datetime") ])
X_test <- as.matrix(X[(as.integer((nrow(X) * 0.85))+1):nrow(X), .SD, .SDcols = !c("datetime") ])
X_test_dates <- X[(as.integer((nrow(X) * 0.85))+1 + (n_timesteps + 1)):nrow(X), .SD, .SDcols = c("datetime") ]

# util values for scaling IGNORE FOR NOW
return_mean <- mean(X_train[, 1])
return_sd <- sd(X_train[, 1])

# torch dataset class
pra_dataset <- dataset(
  name = "pra_dataset",

  initialize = function(x, n_timesteps, n_forecast, sample_frac = 1) {
    self$n_timesteps <- n_timesteps
    self$n_forecast <- n_forecast
    self$x <- torch_tensor(x)

    n <- nrow(self$x) - self$n_timesteps - self$n_forecast + 1

    # it seems to me this is relevant only if we use sample of train sample
    self$starts <- sort(sample.int(
      n = n,
      size = n * sample_frac
    ))
  },

  .getitem = function(i) {

    start <- self$starts[i]
    end <- start + self$n_timesteps - 1
    pred_length <- self$n_forecast

    list(
      x = self$x[start:end,],
      y = self$x[(end + 1):(end + pred_length), 1] # target column is in first column in the taable
    )
  },

  .length = function() {
    length(self$starts)
  }
)

# define train, validation and tset sets for the model
train_ds <- pra_dataset(X_train, n_timesteps, n_forecast, sample_frac = 1)
train_dl <- train_ds %>% dataloader(batch_size = batch_size, shuffle = TRUE)
iter <- train_dl$.iter()
b <- iter$.next()
dim(b$x)
dim(b$y)

valid_ds <- pra_dataset(X_validation, n_timesteps, n_forecast, sample_frac = 1)
valid_dl <- valid_ds %>% dataloader(batch_size = batch_size)

test_ds <- pra_dataset(X_test, n_timesteps, n_forecast)
test_dl <- test_ds %>% dataloader(batch_size = 1)

# model
model <- nn_module(

  initialize = function(type, input_size, hidden_size, linear_size, output_size,
                        num_layers = 1, dropout = 0, linear_dropout = 0) {

    self$type <- type
    self$num_layers <- num_layers
    self$linear_dropout <- linear_dropout

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

    self$mlp <- nn_sequential(
      nn_linear(hidden_size, linear_size),
      nn_relu(),
      nn_dropout(linear_dropout),
      nn_linear(linear_size, output_size)
    )

    # self$output <- nn_linear(hidden_size, 1)
  },

  forward = function(x) {

    x <- self$rnn(x)
    x[[1]][ ,-1, ..] %>%
      self$mlp

    # x <- self$rnn(x)[[1]]
    # x <- x[ , -1, ..]
    # x %>% self$output()
  }
)

# model instantiation
net <- model(
  "lstm", input_size = ncol(X_train), num_layers = 1, hidden_size = 64, linear_size = 512,
  output_size = n_forecast, linear_dropout = 0, dropout = 0
)
device <- torch_device(if (cuda_is_available()) "cuda" else "cpu")
net <- net$to(device = device)

# training optimizers and losses
optimizer <- optim_adam(net$parameters, lr = 0.001)
num_epochs <- 10

train_batch <- function(b) {

  optimizer$zero_grad() # If we perform optimization in a loop, we need to make sure to call optimizer$zero_grad() on every step, as otherwise gradients would be accumulated
  output <- net(b$x$to(device = device))
  target <- b$y$to(device = device)

  loss <- nnf_mse_loss(output, target)
  loss$backward() # backward pass calculates the gradients, but does not update the parameters
  optimizer$step() # optimizer actually performs the updates

  loss$item()
}

valid_batch <- function(b) {

  output <- net(b$x$to(device = device))
  target <- b$y$to(device = device)

  loss <- nnf_mse_loss(output, target)
  loss$item()
}

# training loop
for (epoch in 1:num_epochs) {

  net$train()
  train_loss <- c()

  coro::loop(for (b in train_dl) {
    print(b)
    loss <- train_batch(b)
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

# evaluation
net$eval()
test_preds <- vector(mode = "list", length = length(test_dl))

i <- 1

coro::loop(for (b in test_dl) {

  input <- b$x
  output <- net(input$to(device = device))
  output <- output$to(device = "cpu")
  preds <- as.numeric(output)

  test_preds[[i]] <- preds
  i <<- i + 1
})

# plot test predictipon
predictions <- data.frame(predictions = unlist(test_preds))
predictions$sign <- ifelse(predictions >= 0, 1, 0)
predictions <- cbind(returns = X_test[(n_timesteps + 1):nrow(X_test), "returns"], predictions)
predictions$returns_strategy <- predictions$returns * predictions$sign
predictions <- predictions[, c("returns", "returns_strategy")]
head(predictions)
X_test_predictions <- xts(predictions[-nrow(predictions), ], order.by = X_test_dates[[1]])
PerformanceAnalytics::Return.cumulative(X_test_predictions)
PerformanceAnalytics::charts.PerformanceSummary(X_test_predictions)



# REG AUTOML --------------------------------------------------------------
# library(mlr3verse)
#
# # prepare data
# colnames(indicators)
# cols <- c("datetime", "returns", colnames(indicators)[grep("sum_below_p|sd_below_p", colnames(indicators))])
# X <- indicators[, ..cols]
# X <- na.omit(X)
# X <- X[10:nrow(X), ]
#
# # define task
# task = as_task_regr(x = X, target = "returns", id = "task_reg")
#



# TEST API ----------------------------------------------------------------
library(httr)
library(jsonlite)

y1_train <- rnorm(500)
y2_train <- rnorm(500)
y1_test <- rnorm(500)
y2_test <- rnorm(500)
p <- POST("http://127.0.0.1:8000/pairs",
          body = list(y1_train = toJSON(y1_train),
                      y2_train = toJSON(y2_train),
                      y1_test = toJSON(y1_test),
                      y2_test = toJSON(y2_test)))
p
x <- content(p)
x

