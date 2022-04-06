library(data.table)
library(AzureStor)
library(roll)
library(TTR)
library(future.apply)
library(ggplot2)
library(PerformanceAnalytics)
library(runner)
library(cpm)
library(QuantTools)
require(finfeatures, lib.loc = "C:/Users/Mislav/Documents/GitHub/finfeatures/renv/library/R-4.1/x86_64-w64-mingw32")




# SET UP ------------------------------------------------------------------

# get data from azure
ENDPOINT = storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), key=Sys.getenv("BLOB-KEY"))
CONT = storage_container(ENDPOINT, "equity-usa-hour-fmpcloud-adjusted")
CONTMIN = storage_container(ENDPOINT, "equity-usa-minute-fmpcloud")
fmpcloudr::fmpc_set_token(Sys.getenv("APIKEY-FMPCLOUD"))


# IMPORT DATA -------------------------------------------------------------

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




# CALCULATE INDICATORS ----------------------------------------------------

# calculate bbands for all periods and stocks
window_sizes <- c(8 * 5, 8 * 5 * 4, 8 * 5 * 4 * 3, 8 * 5 * 4 * 6, 8 * 5 * 4 * 12, 8 * 5 * 4 * 24, 8 * 5 * 4 * 48)
sds <- c(1, 2, 3)
params <- expand.grid(window_sizes, sds, stringsAsFactors = FALSE)
# params <- params[1:5, ]
for (i in 1:nrow(params)) {

  # sample params
  param_ <- params[i, ]
  print(param_)

  # calculate bbands
  bbands_cols <- paste0("bbands_", c("upper", "lower", "sma"), "_", paste0(param_, collapse = "_"))
  market_data[, (bbands_cols) := close - bbands(close, param_$Var1, param_$Var2), by = .(symbol)]
}

# calculate aggregate indicators for every period
# get tail risk mesures with sum
cols <- colnames(market_data)[grep("bbands", colnames(market_data))]
indicators <- market_data[, lapply(.SD, function(x) sum(x, na.rm = TRUE)), by = .(datetime), .SDcols = cols]
colnames(indicators) <- c("datetime", paste0("sum_", cols))
setorder(indicators, datetime)

# get tail risk mesures with sd
indicators_sd <- market_data[, lapply(.SD, function(x) sd(x, na.rm = TRUE)), by = .(datetime), .SDcols = cols]
colnames(indicators_sd) <- c("datetime", paste0("sd_", cols))
setorder(indicators_sd, datetime)

# merge indicators
indicators <- merge(indicators, indicators_sd, by = c("datetime"), all.x = TRUE, all.y = FALSE)

# merge spy
spy <- market_data[symbol == "SPY", .(datetime, returns)]
indicators <- merge(indicators, spy, by = "datetime")



# DESCRIPTIVE -------------------------------------------------------------

# plots
ggplot(indicators, aes(x = datetime, y = sd_bbands_upper_1920_3)) + geom_line()
ggplot(indicators, aes(x = datetime, y = sd_bbands_upper_1920_2)) + geom_line()
ggplot(indicators, aes(x = datetime, y = sd_bbands_upper_1920_1)) + geom_line()

ggplot(indicators, aes(x = datetime, y = sma(sd_bbands_upper_1920_3, 8))) + geom_line()
ggplot(indicators, aes(x = datetime, y = sma(sd_bbands_upper_1920_2, 8))) + geom_line()
ggplot(indicators, aes(x = datetime, y = sma(sd_bbands_upper_1920_1, 8))) + geom_line()

ggplot(indicators, aes(x = datetime, y = sum_bbands_upper_1920_3)) + geom_line()
ggplot(indicators, aes(x = datetime, y = sum_bbands_upper_1920_2)) + geom_line()
ggplot(indicators, aes(x = datetime, y = sum_bbands_upper_1920_1)) + geom_line()

ggplot(indicators, aes(x = datetime, y = sma(sum_bbands_upper_1920_3, 8))) + geom_line()
ggplot(indicators, aes(x = datetime, y = sma(sum_bbands_upper_1920_2, 8))) + geom_line()
ggplot(indicators, aes(x = datetime, y = sma(sum_bbands_upper_1920_1, 8))) + geom_line()




# OPTIMIZATION ------------------------------------------------------------

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

# optimization loop
p <- c(0.999, 0.99, 0.95, 0.9, 0.85, 0.8)
roll_width <- c(8 * 22 * seq(1, 12, 1), 8 * 22 * 12 * seq(2, 6, 1))
vars_ <- colnames(indicators)[2:(ncol(indicators)-1)]
params <- expand.grid(p, roll_width, vars_, stringsAsFactors = FALSE)
# plan(multicore(workers = 8))
returns_strategies <- list()

# optimizations
opt_results <- future_lapply(1:nrow(params), function(i) {

  # devugging
  # i = 1052
  print(i)

  # param row
  param_ <- params[i, ]

  # sample
  var_ <- param_[, 3]
  cols <- c("datetime", "returns", var_)
  sample_ <- indicators[, ..cols]

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
optimization_results_down <- rbindlist(opt_results)
setorderv(optimization_results_down, "backtets")
tail(optimization_results_down, 100)

# individual backtests
p <- 0.99
window_size <- 1232
var_ <- "sd_bbands_lower_3840_2"
cols <- c("datetime", "returns", var_)
sample_ <- indicators[, ..cols]
cols_new <- paste0('p_', var_)
sample_[, (cols_new) := lapply(.SD, function(x) roll::roll_quantile(x, window_size, p = p)),
        .SDcols = var_]
backtets <- backtest_percentiles(sample_$returns, sample_[, ..var_][[1]], sample_$p_sd_bbands_lower_3840_2, FALSE)
charts.PerformanceSummary(xts(cbind(sample_$returns, backtets), order.by = sample_$datetime))
