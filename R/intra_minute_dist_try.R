library(data.table)
library(tiledb)
library(lubridate)
library(PerformanceAnalytics)


# minute SPY data
arr <- tiledb_array("D:/equity-usa-minute-fmpcloud-adjusted",
                    as.data.frame = TRUE,
                    selected_ranges = list(symbol = cbind("SPY", "SPY")))
system.time(spy <- arr[])
tiledb_array_close(arr)
attr(spy$time, "tz") <- Sys.getenv("TZ")
spy <- as.data.table(spy)
spy[, time := with_tz(time, tzone = "America/New_York")]

# keep only trading hours
spy <- spy[format.POSIXct(time, format = "%H:%M:%S") %between% c("09:30:00", "16:30:00")]

# calculate returns
spy[, returns := close / shift(close) - 1]

# remove missing vlaues
spy <- na.omit(spy)

# prepare data for HistDAWass package
spy[, hour := format.POSIXct(time, format = "%Y-%m-%d %H")]
DT <- spy[, .(close = tail(close, 1), p01 = close < quantile(close, probs = 0.01)), by = hour]
DT[, hour := as.POSIXct(hour, format = "%Y-%m-%d %H", tz = "America/New_York")]

# inspection
summary(DT$above_close)
plot(DT$above_close)

# check extemes
extremes_dt <- copy(DT)
extremes_dt <- extremes_dt[, returns := shift(close, n = 1L, type = "lead") / close - 1]
extremes_dt <- extremes_dt[,]
nrow(extremes_dt) / nrow(DT)
nrow(extremes_dt[returns > 0]) / nrow(extremes_dt)
PerformanceAnalytics::Return.cumulative(na.omit(extremes_dt$returns))

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

# fast backtest
backtest <- function(returns, indicator, threshold, return_cumulative = TRUE) {
  sides <- vector("integer", length(returns))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(indicator[i-1])) {
      sides[i] <- NA
    } else if (indicator[i-1] > threshold[1] | indicator[i-1] < threshold) {
      sides[i] <- 1
    } else {
      sides[i] <- 0
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
backtest_dt <- copy(DT)
backtest_dt[, returns := close / shift(close) - 1]
backtest(backtest_dt$returns, backtest_dt$above_close, 55)
res <- backtest(backtest_dt$returns, backtest_dt$above_close, 55, FALSE)
charts.PerformanceSummary(as.xts.data.table(cbind(backtest_dt[, .(hour, returns)], res)))
charts.PerformanceSummary(as.xts.data.table(cbind(clusters_data[4000:nrow(clusters_data), .(date, returns)], res[4000:nrow(clusters_data)])))
Performance(as.xts.data.table(na.omit(cbind(backtest_dt[, .(hour, returns)]))))
Performance(as.xts.data.table(na.omit(cbind(backtest_dt[, .(hour)], res))))
