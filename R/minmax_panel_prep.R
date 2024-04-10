library(data.table)
library(roll)
library(arrow)
library(lubridate)
library(ggplot2)


# PRICE DATA --------------------------------------------------------------
# There duplicates that are hard to identify because they have different symbols.
# For example symbols lmb and lmb.1 have same data (same date and same OHLCV)
# We should remove one of thoe symbols. We will remove the one with the number.
# We use daily dat here because is consumes lots of RAM to use hour data directly.
get_symbols_to_remove = function() {
  prices = fread("F:/lean/data/stocks_daily.csv")
  setnames(prices, gsub(" ", "_", c(tolower(colnames(prices)))))
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
  return(symbols_remove)
}
symbols_remove = get_symbols_to_remove()

# Import QC hourly data
prices = fread("F:/lean/data/stocks_hour.csv")

# Fix column names
setnames(prices, gsub(" ", "_", c(tolower(colnames(prices)))))

# Remove duplicates by symbol and date
prices = unique(prices, by = c("symbol", "date"))

# Remove duplicates we calculated above
prices = prices[symbol %notin% symbols_remove]

# Adjust all columns for splits and dividends
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
prices = prices[open > 1e-008 & high > 1e-008 & low > 1e-008 & close > 1e-008]

# Remove missing values
prices = na.omit(prices)

# Check timezone - for Quantconenct data should br NY tz, but R automaticly
# converts it to UTC
prices[, attr(date, "tz")]
prices[symbol == "aapl", unique(date)]
prices[, date := force_tz(date, "America/New_York")]

# Keep only symbol with at least 2 years of data
symbol_keep = prices[, .N, symbol][N >= 2 * 252 * 7, symbol]
prices = prices[symbol %chin% symbol_keep]

# Sort
setorder(prices, symbol, date)

# Keep SPY data
spy = prices[symbol == "spy"]

# free memory
gc()


# FILTERING ---------------------------------------------------------------
# Filter by monthly dollar volume
# keep 100 most liquid at every date
# 200 by volume => cca xx mil rows
# 100 by volume => cca 13 mil rows
# 50 by volume => cca xx mil rows
prices[, dollar_volume := close * volume]
setorder(prices, date, -dollar_volume)
liquid_symbols = prices[, .(symbol = first(symbol, 100)),
                        by = .(ym = yearmon(date))]
liquid_symbols = liquid_symbols[, unique(symbol)]
sprintf("We keep %f percent of data",
        length(liquid_symbols) / prices[, length(unique(symbol))] * 100)
prices = prices[symbol %chin% liquid_symbols]
prices[, dollar_volume := NULL]

# free memory
gc()


# MINMAX INDICATORS -------------------------------------------------------
# Calculate returns
setorder(prices, symbol, date)
prices[, returns := close / shift(close) - 1, by = "symbol"]

# Keep only columns we need
prices = prices[, .(symbol, date, close, close_raw, volume, returns)]

# free memory
gc()

# calculate rolling quantiles
prices[, p_999_4year    := roll::roll_quantile(returns, 255*7*4, p = 0.999), by = .(symbol)]
prices[, p_001_4year    := roll::roll_quantile(returns, 255*7*4, p = 0.001), by = .(symbol)]
prices[, p_999_2year    := roll::roll_quantile(returns, 255*7*2, p = 0.999), by = .(symbol)]
prices[, p_001_2year    := roll::roll_quantile(returns, 255*7*2, p = 0.001), by = .(symbol)]
prices[, p_999_year     := roll::roll_quantile(returns, 255*7, p = 0.999), by = .(symbol)]
prices[, p_001_year     := roll::roll_quantile(returns, 255*7, p = 0.001), by = .(symbol)]
prices[, p_999_halfyear := roll::roll_quantile(returns, 255*4, p = 0.999), by = .(symbol)]
prices[, p_001_halfyear := roll::roll_quantile(returns, 255*4, p = 0.001), by = .(symbol)]

prices[, p_99_4year    := roll::roll_quantile(returns, 255*7*4, p = 0.99), by = .(symbol)]
prices[, p_01_4year    := roll::roll_quantile(returns, 255*7*4, p = 0.01), by = .(symbol)]
prices[, p_99_2year    := roll::roll_quantile(returns, 255*7*2, p = 0.99), by = .(symbol)]
prices[, p_01_2year    := roll::roll_quantile(returns, 255*7*2, p = 0.01), by = .(symbol)]
prices[, p_99_year     := roll::roll_quantile(returns, 255*7, p = 0.99), by = .(symbol)]
prices[, p_01_year     := roll::roll_quantile(returns, 255*7, p = 0.01), by = .(symbol)]
prices[, p_99_halfyear := roll::roll_quantile(returns, 255*4, p = 0.99), by = .(symbol)]
prices[, p_01_halfyear := roll::roll_quantile(returns, 255*4, p = 0.01), by = .(symbol)]

prices[, p_97_4year    := roll::roll_quantile(returns, 255*7*4, p = 0.97), by = .(symbol)]
prices[, p_03_4year    := roll::roll_quantile(returns, 255*7*4, p = 0.03), by = .(symbol)]
prices[, p_97_2year    := roll::roll_quantile(returns, 255*7*2, p = 0.97), by = .(symbol)]
prices[, p_03_2year    := roll::roll_quantile(returns, 255*7*2, p = 0.03), by = .(symbol)]
prices[, p_97_year     := roll::roll_quantile(returns, 255*7, p = 0.97), by = .(symbol)]
prices[, p_03_year     := roll::roll_quantile(returns, 255*7, p = 0.03), by = .(symbol)]
prices[, p_97_halfyear := roll::roll_quantile(returns, 255*4, p = 0.97), by = .(symbol)]
prices[, p_03_halfyear := roll::roll_quantile(returns, 255*4, p = 0.03), by = .(symbol)]

prices[, p_95_4year    := roll::roll_quantile(returns, 255*7*4, p = 0.95), by = .(symbol)]
prices[, p_05_4year    := roll::roll_quantile(returns, 255*7*4, p = 0.05), by = .(symbol)]
prices[, p_95_2year    := roll::roll_quantile(returns, 255*7*2, p = 0.95), by = .(symbol)]
prices[, p_05_2year    := roll::roll_quantile(returns, 255*7*2, p = 0.05), by = .(symbol)]
prices[, p_95_year     := roll::roll_quantile(returns, 255*7, p = 0.95), by = .(symbol)]
prices[, p_05_year     := roll::roll_quantile(returns, 255*7, p = 0.05), by = .(symbol)]
prices[, p_95_halfyear := roll::roll_quantile(returns, 255*4, p = 0.95), by = .(symbol)]
prices[, p_05_halfyear := roll::roll_quantile(returns, 255*4, p = 0.05), by = .(symbol)]

# save market data
time_ = format(Sys.Date(), format = "%Y%m%d")
file_name = file.path("F:/predictors/minmax", paste0(time_,".csv"))
fwrite(prices, file_name)
