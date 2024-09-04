library(data.table)
library(TTR)
library(ggplot2)
library(PerformanceAnalytics)


# # SET UP ------------------------------------------------------------------
# # global vars
# PATH = "F:/data/equity/us"


# PRICE DATA --------------------------------------------------------------
# Import QC daily data
prices = fread("F:/lean/data/stocks_daily.csv")
setnames(prices, gsub(" ", "_", c(tolower(colnames(prices)))))

# Remove duplicates
prices = unique(prices, by = c("symbol", "date"))

# Remove duplicates - there are same for different symbols (eg. phun and phun.1)
dups = prices[, .(symbol , n = .N),
              by = .(date, open, high, low, close, volume, adj_close,
                     symbol_first = substr(symbol, 1, 1))]
dups = dups[n > 1]
dups[, symbol_short := gsub("\\.\\d$", "", symbol)]
symbols_remove = dups[, .(symbol, n = .N),
                      by = .(date, open, high, low, close, volume, adj_close,
                             symbol_short)]
symbols_remove[n >= 2, unique(symbol)]
symbols_remove = symbols_remove[n >= 2, unique(symbol)]
symbols_remove = symbols_remove[grepl("\\.", symbols_remove)]
prices = prices[symbol %notin% symbols_remove]

# Adjust all columns
prices[, adj_rate := adj_close / close]
prices[, let(
  open = open*adj_rate,
  high = high*adj_rate,
  low = low*adj_rate
)]
setnames(prices, "close", "close_raw")
setnames(prices, "adj_close", "close")
prices[, let(adj_rate = NULL)]
setcolorder(prices, c("symbol", "date", "open", "high", "low", "close", "volume"))

# Remove observations where open, high, low, close columns are below 1e-008
# This step is opional, we need it if we will use finfeatures package
prices = prices[open > 1e-008 & high > 1e-008 & low > 1e-008 & close > 1e-008]

# Remove missing values
prices = na.omit(prices)

# Keep only symbol with at least 1 year of data
# This step is optional
symbol_keep = prices[, .N, symbol][N >= 252, symbol]
prices = prices[symbol %chin% symbol_keep]

# keep 100 most liquid at every date
# 200 by volume => cca 20 mil rows
# 100 by volume => cca 13 mil rows
# 50 by volume => cca 8 mil rows
# prices[, dollar_volume := close * volume]
# setorder(prices, date, -dollar_volume)
# liquid_symbols = prices[, .(symbol = first(symbol, 100)), by = date]
# liquid_symbols = liquid_symbols[, unique(symbol)]
# sprintf("We keep %f percent of data",
#         length(liquid_symbols) / prices[, length(unique(symbol))] * 100)
# prices = prices[symbol %chin% liquid_symbols]
# prices[, dollar_volume := NULL]

# Sort
setorder(prices, symbol, date)

# free memory
gc()


# PREPARE PRICES DATA -----------------------------------------------------
# calculate retursn
prices[, returns := close / shift(close) - 1, by = .(symbol)]


# ETF DATA ----------------------------------------------------------------
# Import ETF constituents data
etf_files = list.files("F:/data/equity/us/etf_constituents",
                       full.names = TRUE,
                       pattern = "csv$")
etf_constituents = lapply(etf_files, function(x) {
  cbind(symbol = basename(x), fread(x, colClasses = c("numeric" = "weight")))
})
etf_nrows = vapply(etf_constituents, function(x) nrow(x), FUN.VALUE = integer(1))
etf_constituents = etf_constituents[etf_nrows > 2]
index_remove = vapply(
  etf_constituents,
  function(x) x[, all(is.na(weight) | is.na(last_update))],
  FUN.VALUE = logical(1))
etf_constituents = etf_constituents[!index_remove]
etf_constituents = lapply(etf_constituents, function(x) {
  x[, let(
    weight = as.numeric(weight),
    market_value = as.numeric(market_value)
  )]
})
etf_constituents = rbindlist(etf_constituents)
etf_constituents[, symbol := gsub("\\.csv", "", symbol)]

# Remove any missing values
etf_constituents = na.omit(etf_constituents)

# Check 1 ETF
etf_constituents[, unique(symbol)]
spy = etf_constituents[symbol == "spy"]


# ANALYSE ONE SYMBOL ------------------------------------------------------
# Pick symbol
symbol_ = "V"

# Aggregate by date
etf_symbol = etf_constituents[ticker == symbol_]
etf_symbol_dt = etf_symbol[, .(
  shares_held = sum(shares_held),
  weight = sum(weight),
  market_value = sum(market_value)
), by = date]
setorder(etf_symbol_dt, date)

# Plot vars
plot(as.xts.data.table(etf_symbol_dt[, .(date, weight)]), type = "l")
plot(as.xts.data.table(etf_symbol_dt[, .(date, market_value)]), type = "l")
plot(as.xts.data.table(etf_symbol_dt[, .(date, shares_held / 1000)]), type = "l")
plot(as.xts.data.table(etf_symbol_dt[weight < quantile(weight, p = 0.85),
                                     .(date, weight = SMA(weight, 22))]),
     type = "l")
plot(as.xts.data.table(etf_symbol_dt[market_value < quantile(market_value, p = 0.85),
                                     .(date, weight = SMA(market_value, 22))]),
     type = "l")
plot(as.xts.data.table(etf_symbol_dt[shares_held < quantile(shares_held, p = 0.85),
                                     .(date, shares_held = SMA(shares_held / 1000, 22))]),
     type = "l")


# SMA cross strategy
etf_symbol_dt[, sma_short := SMA(market_value, 50)]
etf_symbol_dt[, sma_long := SMA(market_value, 200)]
etf_symbol_dt[, signal := sma_short > sma_long]
plot(as.xts.data.table(etf_symbol_dt[, .(date, sma_short, sma_long)]))
backtest_dt = prices[symbol == tolower(symbol_)][etf_symbol_dt, on = "date"]
ggplot(na.omit(backtest_dt), aes(x = date, y = close)) +
  geom_line() +
  geom_point(aes(color = signal)) +
  theme_minimal()
backtest_dt[, Return.cumulative(returns), by = signal]
performance_dt = na.omit(backtest_dt)
performance_dt[, strategy := shift(signal) * returns]
performance_dt = as.xts.data.table(performance_dt[, .(date, strategy, benchmark = returns)])
charts.PerformanceSummary(performance_dt)
etf_symbol_dt[, let(sma_short = NULL, sma_long = NULL, signal = NULL)]

#
etf_symbol_dt

