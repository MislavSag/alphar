library(data.table)
library(AzureStor)
library(ggplot2)
library(QuantTools)
library(future.apply)
library(PerformanceAnalytics)
library(xts)
library(fmpcloudr)
library(readr)



# get data from azure
ENDPOINT = storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), key=Sys.getenv("BLOB-KEY"))
CONT = storage_container(ENDPOINT, "equity-usa-hour-fmpcloud-adjusted")
CONTMIN = storage_container(ENDPOINT, "equity-usa-minute-fmpcloud")
fmpcloudr::fmpc_set_token(Sys.getenv("APIKEY-FMPCLOUD"))



# CROSS SECTION -----------------------------------------------------------

# import all data from Azure storage
azure_blobs <- list_blobs(CONT)
# symbols <- toupper(gsub("\\.csv", "", azure_blobs$name))
# files_from_symbols <- paste0(tolower(symbols), ".csv")
market_data_list <- lapply(azure_blobs$name, function(x) {
  print(x)
  y <- tryCatch(storage_read_csv2(CONT, x), error = function(e) NA)
  if (is.null(y) | all(is.na(y))) return(NULL)
  y <- cbind(symbol = x, y)
  return(y)
})
market_data <- rbindlist(market_data_list)
market_data[, symbol := toupper(gsub("\\.csv", "", symbol))]
market_data[, returns := close / shift(close) - 1]
market_data <- na.omit(market_data)
market_data$datetime <- as.POSIXct(as.numeric(market_data$datetime),
                                   origin=as.POSIXct("1970-01-01", tz="EST"),
                                   tz="EST")

# dist change function
get_dist_change <- function(data, size_train = 8 * 5, size_test = 8 * 50, gap = 0) {

  # init values
  date_seq <- sort(unique(data$datetime))
  returns_dt <- data[, .(datetime, returns)]
  returns <- returns_dt$returns

  # get dist change
  dist_results <- future_lapply(seq_along(date_seq), function(i){

    # next if there is no enough data
    if (i < (size_train + gap + size_test)) {
      return(NULL)
    }

    # sampling data
    x <- returns_dt[datetime %in% c(date_seq[(i-(size_train + size_test + gap)):(i-size_train-gap - 1)]), returns]
    y <- returns_dt[datetime %in% c(date_seq[(i-size_train):(i-1)]), returns]

    # compute ks test
    ks <- ks.test(x, y)

    # merge
    statistics_results <- data.table(datetime = date_seq[i], p_values = ks$p.value, statistic = ks$statistic)
    return(statistics_results)
  })

  # results
  statistics <- rbindlist(dist_results)
  if (length(statistics) == 0) return(NULL)
  setorder(statistics, datetime)

  # merge with data
  statistics_data <- merge(data, statistics, by = "datetime", all.x = TRUE, all.y = FALSE)
  statistics_data <- na.omit(statistics_data, cols = "p_values")
  return(statistics_data)
}


# dist change function
get_dist_change_fast <- function(data, size_train = 8 * 5, size_test = 8 * 50, gap = 0) {

  # init values
  date_seq <- sort(unique(data$datetime))
  returns_dt <- data[, .(datetime, returns)]
  returns <- returns_dt$returns
  data_length <- size_train + gap + size_test

  # get dist change
  dist_results <- future_lapply(seq_along(date_seq), function(i){

    # next if there is no enough data
    if (i < (data_length)) {
      return(NULL)
    }

    # sampling data
    # x <- returns_dt[datetime %in% c(date_seq[(i-(size_train + size_test + gap)):(i-size_train-gap - 1)]), returns]
    # y <- returns_dt[datetime %in% c(date_seq[(i-size_train):(i-1)]), returns]
    x <- returns[(i-(data_length)):(i-size_train-gap - 1)]
    y <- returns[(i-size_train):(i-1)]

    # compute ks test
    ks <- ks.test(x, y)

    # merge
    statistics_results <- data.table(datetime = date_seq[i], p_values = ks$p.value, statistic = ks$statistic)
    return(statistics_results)
  })

  # results
  statistics <- rbindlist(dist_results)
  if (length(statistics) == 0) return(NULL)
  setorder(statistics, datetime)

  # merge with data
  statistics_data <- merge(data, statistics, by = "datetime", all.x = TRUE, all.y = FALSE)
  statistics_data <- na.omit(statistics_data, cols = "p_values")
  return(statistics_data)
}




# SPY ---------------------------------------------------------------------

# get dist change for all symbols
symbols <- unique(market_data$symbol)
dist_changes <- lapply(symbols, function(s) {
  print(s)
  get_dist_change(market_data[symbol == s])
})
dist_changes_dt <- rbindlist(dist_changes)



# CROSS SECTION -----------------------------------------------------------

# get dist change for all symbols
# plan(multisession(workers = 8L))
symbols <- unique(market_data$symbol)
dist_changes <- lapply(symbols, function(s) {
  print(s)
  get_dist_change(market_data[symbol == s])
})
dist_changes_dt <- rbindlist(dist_changes)

# aggregate across ddatetime
indicators_median <- dist_changes_dt[, .(median = median(statistic, na.rm = TRUE),
                                         sd = sd(statistic, na.rm = TRUE),
                                         mean = mean(statistic, na.rm = TRUE),
                                         sum = sum(statistic, na.rm = TRUE)), by = c('datetime')]

# inspect results
results_data <- merge(market_data[symbol == "SPY"], indicators_median, by = "datetime", all.x = TRUE, all.y = FALSE)
ggplot(results_data, aes(x = datetime)) +
  geom_line(aes(y = sum, color = "red")) +
  geom_line(aes(y = close / 1000))
ggplot(results_data[as.Date(datetime) %between% c("2007-01-01", "2009-12-30")], aes(x = datetime)) +
  geom_line(aes(y = sum, color = "red")) +
  geom_line(aes(y = close))
ggplot(results_data[as.Date(datetime) %between% c("2020-02-01", "2020-05-01")], aes(x = datetime)) +
  geom_line(aes(y = sum, color = "red")) +
  geom_line(aes(y = close))
ggplot(results_data[as.Date(datetime) %between% c("2005-07-01", "2006-01-01")], aes(x = datetime)) +
  geom_line(aes(y = sum, color = "red")) +
  geom_line(aes(y = close))

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

# backtest
backtest_data <- na.omit(results_data)
x <- backtest(backtest_data$returns, backtest_data$sum, 90, FALSE)
x <- cbind(xts(x, order.by = backtest_data$datetime), backtest_data$returns)
colnames(x) <- c("strategy", "benchmark")
charts.PerformanceSummary(x)


# # calculate ks test
# sample_size <- 300
# returns <- market_data$returns
# datetime <- market_data$datetime
# p_values <- c()
# statistic <- c()
# time <- c()
# for (i in seq_along(returns)) {
#
#   # next if there is no enough data
#   if (i < (sample_size * 2)) {
#     next()
#   }
#
#   # sampling data
#   x <- returns[(i - (sample_size * 2)):(i - sample_size - 1)]
#   y <- returns[(i - sample_size):(i - 1)]
#   # hist(x)
#   # hist(y)
#
#   # compute ks test
#   ks <- ks.test(x, y)
#   p_values <- c(p_values, ks$p.value)
#   statistic <- c(statistic, ks$statistic)
#   time <- c(time, datetime[i])
# }
#
# # inspect results
# results <- data.table(datetime = as.POSIXct(time, origin = "1970-10-01", tz = "EST"), p_values, statistic)
# backtest_data <- merge(market_data, results, by = "datetime", all.x = TRUE, all.y = FALSE)
# ggplot(backtest_data, aes(x = datetime)) +
# #  geom_line(aes(y = p_values)) +
#   geom_line(aes(y = statistic)) +
#   # geom_line(aes(y = sma(statistic, 8), color = "red")) +
#   geom_line(aes(y = close / 1000))
#
# ggplot(backtest_data[as.Date(datetime) %between% c("2020-01-01", "2020-12-30")], aes(x = datetime)) +
#   #  geom_line(aes(y = p_values)) +
#   geom_line(aes(y = statistic), color = "red") +
#   # geom_line(aes(y = sma(statistic, 8), color = "red")) +
#   geom_line(aes(y = close / 1000))
#
#
# ggplot(backtest_data[as.Date(datetime) %between% c("2007-06-01", "2008-12-30")], aes(x = datetime)) +
#   #  geom_line(aes(y = p_values)) +
#   geom_line(aes(y = statistic), color = "red") +
#   # geom_line(aes(y = sma(statistic, 8), color = "red")) +
#   geom_line(aes(y = close / 1000))
#
#


# SPY MINUTE DATA ---------------------------------------------------------

# minute SPY data
spy <- storage_read_csv(CONTMIN, "SPY.csv")
spy <- as.data.table(spy)

# change timezone
spy[, datetime := as.POSIXct(as.numeric(formated), origin = as.POSIXct("1970-01-01", tz = "EST"), tz = "EST")]
spy[, formated := NULL]
spy[, t := NULL]

# keep only trading hours
spy <- spy[format.POSIXct(datetime, format = "%H:%M:%S") %between% c("09:30:00", "16:30:00")]

# calculate returns
spy[, returns := c / shift(c) - 1]
spy[, sd := roll::roll_sd(returns, 60 * 8)]
spy[, sd_returns := returns / sd]

# remove missing vlaues
spy <- na.omit(spy)

# define parameters
train_set_sizes <- c(60 * 4, 60 * 8 * 5)
test_set_sizes <- c(60 * 8 * 5, 60 * 8 * 10)
gap_set_sizes <- c(0, 60 * 8 * 5)
params <- expand.grid(train_set_sizes, test_set_sizes, gap_set_sizes, stringsAsFactors = FALSE)
colnames(params) <- c("train", "test", "gap")
# params <- params[1:2, ] # for test

# ged dist change acroos horizonts
plan(multisession(workers = 2L))

# OLD SLOQ WAY
# start_time <- Sys.time()
# dist_changes_spy <- lapply(1:nrow(params), function(i) {
#   print(i)
#   get_dist_change(spy[1:10000, .(datetime, c, returns)], params[i, "train"], params[i, "test"], params[i, "gap"])
# })
# dist_changes_spy_dt <- rbindlist(dist_changes_spy)
# end_time <- Sys.time()
# end_time - start_time
 # NEW FAST WAY
start_time <- Sys.time()
dist_changes_spy_fast_l <- lapply(1:nrow(params), function(i) {
  print(i)
  x <- get_dist_change_fast(spy[, .(datetime, c, returns)], params[i, "train"], params[i, "test"], params[i, "gap"])
})
dist_changes_spy_dt_fast <- rbindlist(dist_changes_spy_fast_l)
end_time <- Sys.time()
end_time - start_time

# all(dist_changes_spy_dt == dist_changes_spy_dt_fast)

# # save all statistics
dist_changes_spy_fast_l_newcols <- lapply(seq_along(dist_changes_spy_fast_l), function(i) {
  colnames(dist_changes_spy_fast_l[[i]])[c(2:5)] <- paste0(colnames(dist_changes_spy_fast_l[[i]])[c(2:5)], "_", i)
  dist_changes_spy_fast_l[[i]]
})
indicators <- Reduce(function(x, y) merge(x, y, by = "datetime", all.x = TRUE, all.y = FALSE), dist_changes_spy_fast_l_newcols)
cols <- colnames(indicators)[c(1:3, seq(5, ncol(indicators), 4))]
indicators <- indicators[, ..cols]
indicators <- na.omit(indicators)

# aggregate across ddatetime
indicators_stats <- dist_changes_spy_dt_fast[, .(median = median(statistic, na.rm = TRUE),
                                                 sd = sd(statistic, na.rm = TRUE),
                                                 mean = mean(statistic, na.rm = TRUE),
                                                 sum = sum(statistic, na.rm = TRUE)), by = c('datetime')]
indicators <- merge(indicators, indicators_stats, by = "datetime", all.x = TRUE, all.y = FALSE)

# inspect results
# results_data <- merge(spy[, .(datetime)], indicators, by = "datetime", all.x = TRUE, all.y = FALSE)
ggplot(indicators, aes(x = datetime)) +
  geom_line(aes(y = sum, color = "red")) +
  geom_line(aes(y = close / 1000))
ggplot(indicators[as.Date(datetime) %between% c("2007-01-01", "2009-12-30")], aes(x = datetime)) +
  geom_line(aes(y = sum, color = "red")) +
  geom_line(aes(y = close / 1000))
ggplot(indicators[as.Date(datetime) %between% c("2020-02-01", "2020-05-01")], aes(x = datetime)) +
  geom_line(aes(y = sum, color = "red")) +
  geom_line(aes(y = close / 1000))
ggplot(indicators[as.Date(datetime) %between% c("2005-07-01", "2006-01-01")], aes(x = datetime)) +
  geom_line(aes(y = sum, color = "red")) +
  geom_line(aes(y = close / 1000))

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

# summary
summary(indicators$statistic_1)
summary(indicators$median)
summary(indicators$sd)
summary(indicators$sum)

# backtest
backtest_data <- na.omit(results_data)
backtest(indicators$returns_1, indicators$sum, 2)
backtest(indicators$returns_1, indicators$sd, 0.04)

x <- backtest(indicators$returns_1, indicators$sd, 0.09, FALSE)
x <- cbind(xts(x, order.by = indicators$datetime), indicators$returns)
colnames(x) <- c("strategy", "benchmark")
charts.PerformanceSummary(x, )


# VAR
library(vars)
library(runner)
library(parallel)
keep_cols_var <- colnames(indicators)[4:11]
keep_cols_var <- c("datetime", "returns_1", keep_cols_var)
X <- indicators[, ..keep_cols_var]
X <- na.omit(X)
# X <- X[datetime %between% c("2015-01-01", "2020-02-26")]
# X <- as.xts.data.table(X)
# res <- VAR(as.xts.data.table(X[1:1000]), p = 8, type = "both")
# preds <- predict(res, n.ahead = 8, ci = 0.95)
# p <- preds$fcst$returns


# predictions for every period
# cl <- makeCluster(4)
# clusterExport(cl, "X", envir = environment())
roll_var <- runner(
  x = X,
  f = function(x) {
    res <- VAR(as.xts.data.table(x), p = 8, type = "both")
    p <- predict(res, n.ahead = 8, ci = 0.95)
    p <- p$fcst$returns
    data.frame(first = p[1, 1], median = median(p[, 1]), mean = mean(p[, 1]), sd = sd(p[, 1]), min = min(p[, 1]))
  },
  k = 1000,
  lag = 0L,
  na_pad = TRUE
  # cl = cl
)
predictions_var <- lapply(roll_var, as.data.table)
predictions_var <- rbindlist(predictions_var, fill = TRUE)
predictions_var[, V1 := NULL]
predictions_var <- cbind(datetime = X[, datetime], predictions_var)
predictions_var <- merge(spy, predictions_var, by = "datetime", all.x = TRUE, all.y = FALSE)
predictions_var <- na.omit(predictions_var)
tail(predictions_var, 10)

# backtest apply
Return.cumulative(predictions_var$returns)
backtest(predictions_var$returns, predictions_var$first, -0.005)
backtest(predictions_var$returns, predictions_var$median, -0.003)
backtest(predictions_var$returns, predictions_var$mean, -0.005)
backtest(predictions_var$returns, predictions_var$min, -0.04)
backtest_dummy(predictions_var$returns, predictions_var$sd, 0.002)
x <- backtest(predictions_var$returns, predictions_var$first, -0.006, FALSE)
charts.PerformanceSummary(as.xts(cbind(predictions_var$returns, x), order.by = predictions_var$datetime))
x <- backtest_dummy(predictions_var$returns, predictions_var$sd, 0.002, FALSE)
charts.PerformanceSummary(as.xts(cbind(predictions_var$returns, x), order.by = predictions_var$datetime))

