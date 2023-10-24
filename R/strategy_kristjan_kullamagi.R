library(data.table)
library(TTR)
library(PerformanceAnalytics)


# backtest performance
Performance <- function(x) {
  cumRetx = Return.cumulative(x)
  annRetx = Return.annualized(x, scale=252)
  sharpex = SharpeRatio.annualized(x, scale=252)
  # winpctx = length(x[x > 0])/length(x[x != 0])
  # annSDx = sd.annualized(x, scale=252)

  # DDs <- findDrawdowns(x)
  # maxDDx = min(DDs$return)
  # maxLx = max(DDs$length)

  # Perf = c(cumRetx, annRetx, sharpex, winpctx, annSDx, maxDDx, maxLx)
  Perf = c(cumRetx, annRetx, sharpex)
  names(Perf) = c("Cumulative Return", "Annual Return", "Sharpe")
  return(Perf)
}

# import daily data
dt = fread("F:/lean_root/data/all_stocks_daily.csv")

# this want be necessary after update
setnames(dt, c("date", "open", "high", "low", "close", "volume", "close_adj", "symbol"))

# remove duplicates
dt = unique(dt, by = c("symbol", "date"))

# remove negative prices
dt = dt[close > 0 & close_adj > 0]

# add variables
dt[, dollar_volume := volume * close]

# adjust all columns
dt[, ratio := close_adj / close]
dt[, `:=`(
  open_adj = open * ratio,
  high_adj = high * ratio,
  low_adj = low * ratio
)]
dt[symbol == "aapl"]

# keep only adjusted columns
dt = dt[, .(symbol, date, open = open_adj, high = high_adj, low = low_adj,
            close = close_adj, volume, dollar_volume)]

# keep only  SP500
obs = dt[, .N, by = symbol]
symbols_to_keep = obs[N > 100]
dt = dt[symbol %chin% symbols_to_keep[, symbol]]

# Calculate moving averages
dt[, MA10 := SMA(close, 10), by = symbol]
dt[, MA20 := SMA(close, 20), by = symbol]

# Signal for breakout
dt[, breakout := ifelse(high > shift(high) & low > shift(low, 1), 1, 0), by = "symbol"]

# Entry and exit signals
dt[, signal := 0]
dt[breakout == 1, signal := 1]
dt[close < MA10, signal := 0]
dt[close < MA20, signal := 0]

# Calculate returns
dt[, returns := close / shift(close) - 1, by = "symbol"]

# sample
s = "spy"
strategy = dt[symbol == s]
strategy[, strategy := returns * shift(signal)]
strategy = as.xts.data.table(na.omit(strategy[, .(date, strategy)]))
benchmark = na.omit(as.xts.data.table(dt[symbol == s, .(date, returns)]))
data_ = cbind(benchmark, strategy)
names(data_) = c("benchmark", "strategy")
charts.PerformanceSummary(data_, plot.engine = "ggplot2")
Performance(data_[, 1])
Performance(data_[, 2])

