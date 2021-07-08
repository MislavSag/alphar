library(data.table)
library(leanr)
library(PerformanceAnalytics)
library(TTR)
library(fasttime)
library(lubridate)
library(ggplot2)
library(stringr)


# parameters
backtest_path <- "D:/risks/risk-factors/hour"

# import spy radf data
market_data <- import_lean('D:/market_data/equity/usa/hour/trades_adjusted')
market_data[, returns := close / shift(close) - 1]
spy <- market_data[symbol == "SPY"]
bactest_files <- list.files(backtest_path, full.names = TRUE)
bactest_files <- unlist(lapply(bactest_files, function(x) list.files(x, full.names = TRUE)))
bactest_files <- bactest_files[grepl("EWMA_plain_100_150", bactest_files)]
backtest_data <- lapply(bactest_files, fread)
names(backtest_data) <- gsub(".*/|\\.csv", "", bactest_files)
backtest_data <- rbindlist(backtest_data, idcol = TRUE)
backtest_data[, datetime := as.POSIXct(datetime, tz = "EST")]
backtest_data[, symbol := gsub("_.*", "", .id)]
backtest_data <- merge(backtest_data, market_data[, .(symbol, datetime, returns)], by = c("symbol", "datetime"), all.x = TRUE, all.y = FALSE)
setorderv(backtest_data, c(".id", "datetime"))
backtest_data[, .id := str_extract(.id, "_.*")]
# backtest_data <- na.omit(backtest_data[, .(.id, datetime, returns, adf, sadf, gsadf, badf, bsadf)])

# define indicators
main_vars <- colnames(backtest_data)[4:(ncol(backtest_data)-2)]
indicators_median <- backtest_data[, lapply(.SD, median, na.rm = TRUE), by = c('.id', 'datetime'), .SDcols = main_vars]
colnames(indicators_median)[3:ncol(indicators_median)] <- paste0("median_", colnames(indicators_median)[3:ncol(indicators_median)])
indicators_sd <- backtest_data[, lapply(.SD, sd, na.rm = TRUE), by = c('.id', 'datetime'), .SDcols = main_vars]
colnames(indicators_sd)[3:ncol(indicators_sd)] <- paste0("sd_", colnames(indicators_sd)[3:ncol(indicators_sd)])
indicators_mean <- backtest_data[, lapply(.SD, mean, na.rm = TRUE), by = c('.id', 'datetime'), .SDcols = main_vars]
colnames(indicators_mean)[3:ncol(indicators_mean)] <- paste0("mean_", colnames(indicators_mean)[3:ncol(indicators_mean)])
indicators_sum <- backtest_data[, lapply(.SD, sum, na.rm = TRUE), by = c('.id', 'datetime'), .SDcols = main_vars]
colnames(indicators_sum)[3:ncol(indicators_sum)] <- paste0("sum_", colnames(indicators_sum)[3:ncol(indicators_sum)])

# merge indicators
indicators <- merge(indicators_sd, indicators_median, by = c(".id", "datetime"))
indicators <- merge(indicators, indicators_mean, by = c(".id", "datetime"))
indicators <- merge(indicators, indicators_sum, by = c(".id", "datetime"))
setorderv(backtest_data, c(".id", "datetime"))
indicators <- na.omit(indicators[, ])

# merge spy and indicators
spy <- spy[, .(datetime, close, returns)]
spy <- indicators[spy, on = "datetime"]
spy <- na.omit(spy)
setorderv(spy, c(".id", "datetime"))

# plots
ggplot(spy, aes(x = datetime)) +
  geom_line(aes(y = mean_es_std)) +
  geom_line(aes(y = median_es_std), color = "red")
# geom_line(aes(y = es_month), color = "blue")

# optimization params
thresholds <- seq(0, 0.02, 0.001)
variables <- colnames(spy)[3:(ncol(spy)-2)]
params <- expand.grid(thresholds, variables, stringsAsFactors = FALSE)

# backtest function
backtest <- function(returns, indicator, threshold, return_cumulative = TRUE) {
  sides <- vector("integer", length(indicator))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(indicator[i])) {
      sides[i] <- NA
    } else if (indicator[i] > threshold) {
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

# optimizations loop
returns_strategies <- list()
unique_ids <- unique(backtest_data$.id)
for (j in seq_along(unique_ids)) {
  print(unique_ids[j])
  sample_ <- spy[.id == unique_ids[j]]
  x <- vapply(1:nrow(params), function(i) backtest(sample_$returns,
                                                   sample_[, get(params[i, 2])], # sample_[, get(params[i, 2])]
                                                   params[i, 1]),
              numeric(1))
  returns_strategies[[j]] <- cbind(radf_id = unique_ids[j], params, x)
}
optimization_results <- rbindlist(returns_strategies)
head(optimization_results[order(optimization_results$x, decreasing = TRUE), ], 50)
tail(optimization_results[order(optimization_results$x, decreasing = TRUE), ], 20)

# optimization summary
opt_summary <- optimization_results[, median(backtets), by = .(id)]
opt_summary[order(V1, decreasing = TRUE)]
optimization_results[Var2 == "bsadf" & Var1 > 0.8]
ggplot(optimization_results, aes(Var1, Var2, fill= backtets)) +
  geom_tile()
ggplot(optimization_results, aes(Var2, id, fill= backtets)) +
  geom_tile()
ggplot(optimization_results, aes(Var1, id, fill= backtets)) +
  geom_tile()

# backtest individual
sample_ <- spy[.id == "_950_EWMA_plain_100_150"] #  & datetime %between% c('2020-01-01', '2021-01-01')]
strategy_returns <- backtest(sample_$returns, sample_$median_es_std, 0.005, return_cumulative = FALSE)
charts.PerformanceSummary(xts(cbind(sample_$returns, strategy_returns), order.by = sample_$datetime))
