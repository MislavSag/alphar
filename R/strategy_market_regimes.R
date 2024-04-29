library(data.table)
library(fs)
library(arrow)
library(finfeatures)
library(lubridate)
library(HistDAWass)
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
# 100 by volume => cca 48 mil rows
# 50 by volume  => cca xx mil rows
prices[, dollar_volume := close * volume]
setorder(prices, date, -dollar_volume)
liquid_symbols = prices[, .(symbol = first(symbol, 100)),
                        by = .(ym = yearmon(date))]
liquid_symbols = liquid_symbols[, unique(symbol)]
sprintf("We keep %f percent of data",
        length(liquid_symbols) / prices[, length(unique(symbol))] * 100)
prices = prices[symbol %chin% liquid_symbols]
prices[, dollar_volume := NULL]

# Order again
setorder(prices, symbol, date)

# free memory
gc()




# WASSERSTEIN DISTANCE ----------------------------------------------------
# Create returns column
prices[, returns := close / shift(close) - 1, by = symbol]
prices = na.omit(prices)

# prepare data for HistDAWass package
system.time({
  dist_l = prices[date > as.IDate("2018-01-01"),
                  .(distributions = list(data2hist(returns))),
                  by = c("date")]
})

# Create MatH object
mat = MatH(
  x = dist_l$distributions,
  nrows = nrow(dist_l),
  ncols = 1,
  rownames = unique(dist_l$date),
  varnames = "returns"
)
plot(mat[1100:1101], type="DENS")

# Rolling cluster predictions
estimated_clusters = lapply(1:length(mat@M), function(i) { # nrow(mat@M)
  if (i < 462) {
    return(NA_integer_)
  } else {
    return(WH_kmeans(mat[1:i], k = 3, rep = 10, simplify = TRUE, qua = 40))
  }
})

# Get clean tables with ids
estimated_clusters_dt = lapply(estimated_clusters, function(x) {
  if (all(is.na(x))) {
    return(NULL)
  } else {
    cbind.data.frame(
      date = with_tz(last(as.POSIXct(as.integer(names(x$solution$IDX)))),
                     tzone = "America/New_York"),
      idx  = last(x$solution$IDX)
    )
  }
})
estimated_clusters_dt = rbindlist(estimated_clusters_dt, fill = TRUE)

# Merge with SPY
backtest_dt = estimated_clusters_dt[spy, on = "date"]
backtest_dt = na.omit(backtest_dt, cols = "idx")
backtest_dt[, returns := close / shift(close) - 1]
backtest_dt = na.omit(backtest_dt)

# Plot SPY close with different color for ecery cluster
ggplot(data = backtest_dt,
       aes(x = date, y = close, group = 1, color = factor(idx))) +
  geom_line() + # Draw linesgeom_line(aes(color = insurance)) + geom_point() +
  # scale_color_manual(values = c("red", "blue", "green")) + # Manually set colors for different idx values
  theme_minimal() + # Use a minimal theme for the plot
  # labs(title = "SPY Close Prices by idx", x = "Date", y = "Close Price", color = "Idx") +
  theme(legend.position = "bottom") # Adjust legend position

# Returns across clusters
backtest_dt[, mean(returns), by = idx]
backtest_dt[, mean(returns)]
backtest_dt[, idx_lag := shift(idx)]
backtest_dt = na.omit(backtest_dt)
backtest_dt[, mean(returns), by = idx_lag]

