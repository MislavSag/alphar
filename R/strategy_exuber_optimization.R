library(data.table)
library(leanr)
library(PerformanceAnalytics)
library(TTR)
library(fasttime)
library(lubridate)
library(ggplot2)


# parameters
radf_data_path <- "D:/risks/radf-minute"

# import spy market_data
if (grepl("hour", radf_data_path)) {
  spy <- import_lean('D:/market_data/equity/usa/hour/trades_adjusted', "spy")
  spy[, returns := close / shift(close) - 1]
} else if (grepl("minute", radf_data_path)) {
  spy <- get_market_equities_minutes('D:/market_data/equity/usa/minute', "spy")
  spy[, time := format.POSIXct(datetime, "%H:%M:%S")]
  spy <- spy[time %between% c("08:00:00", "16:00:00")]
}

# import radf data
radf_data_files <- list.files(radf_data_path, full.names = TRUE)
radf_data <- lapply(radf_data_files, function(x) {
  spy_file <- list.files(x, pattern = "SPY", full.names = TRUE)
  if (length(spy_file) != 0) {
    y <- fread(spy_file)
    y[, id := gsub(".*/|\\.csv", "", x)]
  } else {
   y <- NULL
  }
  y
})
radf_data <- rbindlist(radf_data)
radf_data <- merge(radf_data, spy[, .(datetime, close)], by = "datetime", all.x = TRUE, all.y = FALSE)
setorderv(radf_data, c("id", "datetime"))
radf_data[, returns := close / shift(close) - 1, by = .(id)]
setorderv(radf_data, c("id", "datetime"))
radf_data <- na.omit(radf_data[, .(id, datetime, returns, adf, sadf, gsadf, badf, bsadf)])

# optimization
thresholds <- seq(0, 2, 0.1)
variables <- colnames(radf_data)[4:(ncol(radf_data))]
params <- expand.grid(thresholds, variables, stringsAsFactors = FALSE)
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
  print(table(sides))
  returns_strategy <- returns * sides
  if (return_cumulative) {
    return(PerformanceAnalytics::Return.cumulative(returns_strategy))
  } else {
    return(returns_strategy)
  }
}

# optimizations loop
returns_strategies <- list()
unique_ids <- unique(radf_data$id)
for (j in seq_along(unique_ids)) {
  print(unique_ids[j])
  sample_ <- radf_data[id == unique_ids[j]]
  x <- vapply(1:nrow(params), function(i) backtest(sample_$returns,
                                                   sample_[, get(params[i, 2])], # sample_[, get(params[i, 2])]
                                                   params[i, 1]),
              numeric(1))
  returns_strategies[[j]] <- cbind(radf_id = unique_ids[j], params, x)
}
optimization_results <- rbindlist(returns_strategies)
head(optimization_results[order(optimization_results$x, decreasing = TRUE), ], 20)

# optimization summary
opt_summary <- optimization_results[, median(x), by = .(Var2)]
opt_summary[order(V1, decreasing = TRUE)]
optimization_results[Var2 == "bsadf" & Var1 > 0.8]
ggplot(optimization_results, aes(Var1, Var2, fill= x)) +
  geom_tile()
ggplot(optimization_results, aes(radf_id, Var2, fill= x)) +
  geom_tile()
ggplot(optimization_results, aes(radf_id, Var1, fill= x)) +
  geom_tile()

# backtest individual
backtest_data <- radf_data[id == "0-400-1"]
strategy_returns <- backtest(backtest_data$returns, backtest_data$sadf, 1.2, return_cumulative = FALSE)
charts.PerformanceSummary(xts(cbind(backtest_data$returns, strategy_returns), order.by = backtest_data$datetime))
