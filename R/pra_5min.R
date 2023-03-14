# Title:  PRA min
# Author: Mislav Sagovac
# Description: PRA strategy on  min frequency

# packages
library(tiledb)
library(data.table)
library(checkmate)
library(nanotime)
library(findata)
library(lubridate)
library(QuantTools)
library(ggplot2)
library(patchwork)
library(rextendr)
library(PerformanceAnalytics)



# SET UP ------------------------------------------------------------------
# check if we have all necessary env variables
assert_choice("AWS-ACCESS-KEY", names(Sys.getenv()))
assert_choice("AWS-SECRET-KEY", names(Sys.getenv()))
assert_choice("AWS-REGION", names(Sys.getenv()))
assert_choice("BLOB-ENDPOINT", names(Sys.getenv()))
assert_choice("BLOB-KEY", names(Sys.getenv()))
assert_choice("APIKEY-FMPCLOUD", names(Sys.getenv()))
assert_choice("FRED-KEY", names(Sys.getenv()))

# set credentials
config <- tiledb_config()
config["vfs.s3.aws_access_key_id"] <- Sys.getenv("AWS-ACCESS-KEY")
config["vfs.s3.aws_secret_access_key"] <- Sys.getenv("AWS-SECRET-KEY")
config["vfs.s3.region"] <- Sys.getenv("AWS-REGION")
context_with_config <- tiledb_ctx(config)

# parameters
windows = c(90, 90 * 5, 90 * 22, 90 * 22 * 6, 90 * 22 * 12, 90 * 22 * 24)
uri = "s3://predictors-pra5min" # if NULL, it calculates PRA values.



# UNIVERSE ----------------------------------------------------------------
# sp500 universe
fmp = FMP$new()
symbols <- fmp$get_sp500_symbols()



# PRA -------------------------------------------------------------
# calculate PRA
cols <- paste0("pr_", windows)
for (s in symbols) {

  # debug
  print(s)

  # import existing data
  if (tiledb_object_type("s3://predictors-pra5min") == "ARRAY") {
    system.time({
      arr <- tiledb_array(uri,
                          as.data.frame = TRUE,
                          query_layout = "UNORDERED",
                          attrs = c("close"),
                          selected_ranges = list(symbol = cbind(s, s)))
      x <- arr[]
    })
    tiledb_array_close(arr)

    # TODO: UPDATE IF GOOD
    if (nrow(x) > 100) {
      next
    }
  }

  # import minute data
  system.time({
    arr <- tiledb_array("D:/equity-usa-minute-fmpcloud-adjusted",
                        as.data.frame = TRUE,
                        query_layout = "UNORDERED",
                        attrs = c("close"),
                        selected_ranges = list(symbol = cbind(s, s)))
    x <- arr[]
  })
  tiledb_array_close(arr)
  if (nrow(x) == 0) {
    next
  }

  # clean table
  x <- as.data.table(x)
  attr(x$time, "tz") <- Sys.getenv("TZ")
  x[, time := with_tz(time, "America/New_York")]

  # keep trading hours
  x <- x[as.ITime(time) %between% c(as.ITime("09:30:00"), as.ITime("15:59:00"))]
  x <- unique(x, cols = c("symbol", "time"))
  setorderv(x, c('symbol', 'time'))

  # create 5 min data
  x_ <- copy(x)
  x_[, time := as.nanotime(time)]
  x_5 <- x_[, .(close = tail(close, 1)),
            by = .(symbol,
                   time = nano_ceiling(time, as.nanoduration("00:05:00")))]
  x_5[, time := as.POSIXct(time, tz = "UTC")]
  x_5[, time := with_tz(time, "America/New_York")]

  # check N
  if (nrow(x_5) < max(windows)) {
    next
  }

  # calculate PRApra_save
  system.time(x_5[, (cols) := lapply(windows, function(w) roll_percent_rank(close, w))])

  # save to tiledb
  x_save <- copy(x_5)
  x_save[, time := with_tz(time, "UTC")]
  if (tiledb_object_type(uri) != "ARRAY") {
    fromDataFrame(
      obj = as.data.frame(x_save),
      uri = uri,
      col_index = c("symbol", "time"),
      sparse = TRUE,
      tile_domain=list(time=c(as.POSIXct("1970-01-01 00:00:00", tz = "UTC"),
                              as.POSIXct("2099-12-31 23:59:59", tz = "UTC"))),
      allows_dups = FALSE
    )
  } else {
    arr <- tiledb_array(uri)
    arr[] <- as.data.frame(x_save)
    tiledb_array_close(arr)
  }
}

# import pra indicators
arr <- tiledb_array(uri, as.data.frame = TRUE, query_layout = "UNORDERED")
system.time({pra <- arr[]})
tiledb_array_close(arr)

# free resources
gc()

# convert to data.table
pra <- as.data.table(pra)



# DUMMY -------------------------------------------------------------------
# dummy
cols_above_999 <- paste0("pr_above_dummy_", windows)
pra[, (cols_above_999) := lapply(.SD, function(x) ifelse(x > 0.999, 1, 0)), .SDcols = cols]
cols_below_001 <- paste0("pr_below_dummy_", windows)
pra[, (cols_below_001) := lapply(.SD, function(x) ifelse(x < 0.001, 1, 0)), .SDcols = cols]
cols_net_1 <- paste0("pr_below_dummy_net_", windows)
pra[, (cols_net_1) := pra[, ..cols_above_999] - pra[, ..cols_below_001]]

cols_above_99 <- paste0("pr_above_dummy_99_", windows)
pra[, (cols_above_99) := lapply(.SD, function(x) ifelse(x > 0.99, 1, 0)), .SDcols = cols]
cols_below_01 <- paste0("pr_below_dummy_01_", windows)
pra[, (cols_below_01) := lapply(.SD, function(x) ifelse(x < 0.01, 1, 0)), .SDcols = cols]
cols_net_2 <- paste0("pr_below_dummy_net_0199", windows)
pra[, (cols_net_2) := pra[, ..cols_above_99] - pra[, ..cols_below_01]]

cols_above_97 <- paste0("pr_above_dummy_97_", windows)
pra[, (cols_above_97) := lapply(.SD, function(x) ifelse(x > 0.97, 1, 0)), .SDcols = cols]
cols_below_03 <- paste0("pr_below_dummy_03_", windows)
pra[, (cols_below_03) := lapply(.SD, function(x) ifelse(x < 0.03, 1, 0)), .SDcols = cols]
cols_net_3 <- paste0("pr_below_dummy_net_0397", windows)
pra[, (cols_net_3) := pra[, ..cols_above_97] - pra[, ..cols_below_03]]

cols_above_95 <- paste0("pr_above_dummy_95_", windows)
pra[, (cols_above_95) := lapply(.SD, function(x) ifelse(x > 0.95, 1, 0)), .SDcols = cols]
cols_below_05 <- paste0("pr_below_dummy_05_", windows)
pra[, (cols_below_05) := lapply(.SD, function(x) ifelse(x < 0.05, 1, 0)), .SDcols = cols]
cols_net_4 <- paste0("pr_below_dummy_net_0595", windows)
pra[, (cols_net_4) := pra[, ..cols_above_95] - pra[, ..cols_below_05]]


# get risk measures
indicators <- pra[symbol != "SPY", lapply(.SD, sum, na.rm = TRUE),
                  .SDcols = c(colnames(pra)[grep("pr_\\d+", colnames(pra))],
                              cols_above_999, cols_above_99, cols_below_001, cols_below_01,
                              cols_above_97, cols_below_03, cols_above_95, cols_below_05,
                              cols_net_1, cols_net_2, cols_net_3, cols_net_4),
                  by = .(time)]
indicators <- unique(indicators, by = c("time"))
indicators[, time := force_tz(time, "America/New_York")]
setorder(indicators, "time")

# import spy data
# TODO: make function that imports minute data and automaticly upsample in required period
arr <- tiledb_array("D:/equity-usa-minute-fmpcloud-adjusted",
                    as.data.frame = TRUE,
                    query_layout = "UNORDERED",
                    attrs = c("close"),
                    selected_ranges = list(symbol = cbind("SPY", "SPY")))
spy <- arr[]
spy <- as.data.table(spy)
attr(spy$time, "tz") <- Sys.getenv("TZ")
spy[, time := with_tz(time, "America/New_York")]
spy <- spy[as.ITime(time) %between% c(as.ITime("09:30:00"), as.ITime("15:59:00"))]
spy <- unique(spy, cols = c("symbol", "time"))
setorderv(spy, c('symbol', 'time'))

# create 5 min SPY data
spy[, time := as.nanotime(time)]
spy_5 <- spy[, .(close = tail(close, 1)),
             by = .(time = nano_ceiling(time, as.nanoduration("00:05:00")))]
spy_5[, time := as.POSIXct(time, tz = "UTC")]
spy_5[, time := with_tz(time, "America/New_York")]

# merge spy and pra
backtest_data <- spy_5[indicators, on = 'time']
backtest_data <- na.omit(backtest_data)
setorder(backtest_data, "time")
backtest_data[, returns := close / shift(close) - 1]
backtest_data <- na.omit(backtest_data)



# VISUALIZATION -----------------------------------------------------------
# date segments
GFC <- c("2007-01-01", "2010-01-01")
AFTERGFCBULL <- c("2010-01-01", "2015-01-01")
COVID <- c("2020-01-01", "2021-06-01")
AFTER_COVID <- c("2021-06-01", "2022-01-01")
NEW <- c("2022-01-01", "2022-03-15")

# check individual stocks
sample_ <- pra[symbol == "AAPL"]
v_buy <- sample_[pr_above_dummy_95_1980 == 1, time, ]
v_sell <- sample_[pr_below_dummy_05_1980 == 1, time, ]
ggplot(sample_, aes(x = time)) +
  geom_line(aes(y = close)) +
  geom_vline(xintercept = v_buy, color = "green") +
  geom_vline(xintercept = v_sell, color = "red")
ggplot(sample_[time %between% GFC], aes(x = time)) +
  geom_line(aes(y = close)) +
  geom_vline(xintercept = v_buy, color = "green") +
  geom_vline(xintercept = v_sell, color = "red")
ggplot(sample_[datetime %between% AFTERGFCBULL], aes(x = datetime)) +
  geom_line(aes(y = close)) +
  geom_vline(xintercept = v_buy, color = "green") +
  geom_vline(xintercept = v_sell, color = "red")
ggplot(sample_[datetime %between% COVID], aes(x = datetime)) +
  geom_line(aes(y = close)) +
  geom_vline(xintercept = v_buy, color = "green") +
  geom_vline(xintercept = v_sell, color = "red")
ggplot(sample_[datetime %between% AFTER_COVID], aes(x = datetime)) +
  geom_line(aes(y = close)) +
  geom_vline(xintercept = v_buy, color = "green") +
  geom_vline(xintercept = v_sell, color = "red")
ggplot(sample_[datetime %between% NEW], aes(x = datetime)) +
  geom_line(aes(y = close)) +
  geom_vline(xintercept = v_buy, color = "green") +
  geom_vline(xintercept = v_sell, color = "red")

# plots sum for backtest data
g1 <- ggplot(backtest_data, aes(x = time)) +
  geom_line(aes(y = pr_below_dummy_90))
g2 <- ggplot(backtest_data, aes(x = time)) +
  geom_line(aes(y = pr_below_dummy_450))
g3 <- ggplot(backtest_data, aes(x = time)) +
  geom_line(aes(y = pr_below_dummy_1980))
g4 <- ggplot(backtest_data, aes(x = time)) +
  geom_line(aes(y = pr_below_dummy_11880))
( g1 | g2 ) / ( g3 | g4 )

# plots sum 95 / 05
g1 <- ggplot(backtest_data, aes(x = time)) +
  geom_line(aes(y = pr_below_dummy_05_90)) +
  geom_line(aes(y = pr_above_dummy_95_90), color ="red")
g2 <- ggplot(backtest_data, aes(x = time)) +
  geom_line(aes(y = pr_below_dummy_05_450)) +
  geom_line(aes(y = pr_above_dummy_95_450), color ="red")
g3 <- ggplot(backtest_data, aes(x = time)) +
  geom_line(aes(y = pr_below_dummy_05_1980)) +
  geom_line(aes(y = pr_above_dummy_95_1980), color ="red")
g4 <- ggplot(backtest_data, aes(x = time)) +
  geom_line(aes(y = pr_below_dummy_05_11880)) +
  geom_line(aes(y = pr_above_dummy_95_11880), color ="red")
( g1 | g2 ) / ( g3 | g4 )

# net
g1 <- ggplot(backtest_data, aes(x = time)) +
  geom_line(aes(y = pr_below_dummy_net_90))
g2 <- ggplot(backtest_data, aes(x = time)) +
  geom_line(aes(y = pr_below_dummy_net_450))
g3 <- ggplot(backtest_data, aes(x = time)) +
  geom_line(aes(y = pr_below_dummy_net_1980))
g4 <- ggplot(backtest_data, aes(x = time)) +
  geom_line(aes(y = pr_below_dummy_net_11880))
( g1 | g2 ) / ( g3 | g4 )



# BACKTEST ----------------------------------------------------------------
# backtest function
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

# optimizations loop
thresholds <- c(seq(0, 100, 1))
colnames(backtest_data)
variables <- colnames(indicators)[grep("below_dummy_net", colnames(indicators))]
params <- expand.grid(thresholds, variables, stringsAsFactors = FALSE)
returns_strategies <- list()

returns_ <- backtest_data$returns
vars_ <- params$Var2
x <- vapply(1:nrow(params), function(i) backtest_cpp(returns_,
                                                     backtest_data[, get(vars_[i])],
                                                     # SMA(backtest_data[, get(params[i, 2])], 4),
                                                     params[i, 1]),
            numeric(1))
optim_results <- cbind(params, cum_return = x)
setorder(optim_results, cum_return)

# inspect
tail(optim_results[order(optim_results$cum_return), ], 50)
ggplot(optim_results[grep("dummy_\\d+$", optim_results$Var2),], aes(x = cum_return)) + geom_histogram() +
  geom_vline(xintercept = 4.1, color = "red") + facet_grid(. ~ Var2)
ggplot(optim_results[grep("01", optim_results$Var2),], aes(x = cum_return)) + geom_histogram() +
  geom_vline(xintercept = 4.1, color = "red") + facet_grid(. ~ Var2)
ggplot(optim_results[grep("03", optim_results$Var2),], aes(x = cum_return)) + geom_histogram() +
  geom_vline(xintercept = 4.1, color = "red") + facet_grid(. ~ Var2)
ggplot(optim_results[grep("03", optim_results$Var2),], aes(x = cum_return)) + geom_histogram() +
  geom_vline(xintercept = 4.1, color = "red") + facet_grid(. ~ Var2)
summary_results <- as.data.table(optim_results)
summary_results[, median(cum_return), by = Var2]

# for net
unique(optim_results$Var2)
ggplot(optim_results[grep("net_40|net_176|net_528|net_1056|net_2112|net_4224", optim_results$Var2),], aes(x = cum_return)) +
  geom_histogram() +
  geom_vline(xintercept = 4.1, color = "red") + facet_grid(. ~ Var2)
ggplot(optim_results[grep("net_01|net_40|net_176|net_528|net_1056|net_2112|net_4224", optim_results$Var2),], aes(x = cum_return)) +
  geom_histogram() +
  geom_vline(xintercept = 4.1, color = "red") + facet_grid(. ~ Var2)
ggplot(optim_results[grep("net_03|net_05", optim_results$Var2),], aes(x = cum_return)) + geom_histogram() +
  geom_vline(xintercept = 4.1, color = "red") + facet_grid(. ~ Var2)


# backtest individual
strategy_returns <- backtest(returns_, backtest_data$pr_below_dummy_90, 71, return_cumulative = FALSE)
charts.PerformanceSummary(xts(cbind(returns_, strategy_returns), order.by = backtest_data$time))
charts.PerformanceSummary(xts(cbind(backtest_data$returns, strategy_returns)[30000:length(strategy_returns), ],
                              order.by = backtest_data$datetime[30000:length(strategy_returns)]))
Performance(xts(strategy_returns, order.by = backtest_data$datetime))
Performance(xts(backtest_data$returns, order.by = backtest_data$datetime))
