library(arrow)
library(dplyr)
library(data.table)
library(lubridate)
library(ggplot2)
library(PerformanceAnalytics)


# Import data
prices = open_dataset("F:/lean/data/all_stocks_daily") |>
  filter(Symbol %in% c("tfi", "tfi.1")) |>
  collect()
setDT(prices)
setnames(prices, gsub(" ", "_", tolower(colnames(prices))))

# Plot
dt_ = as.xts.data.table(dcast(prices[, .(symbol, date, adj_close)], date ~ symbol, value.var = "adj_close"))
plot(dt_)
plot(na.omit(dt_[, "tfi"]))
plot(na.omit(dt_[, "tfi"]["2022-03-15/"]))

# Filter until Damjan found an error
prices = prices[symbol == "tfi"][date > as.Date("2022-03-15")]

# Set keys
setkey(prices, "symbol")
setorder(prices, symbol, date)

# Remove duplicates
prices = unique(prices, by = c("symbol", "date"))

# Adjust prices for splits/dividends
prices[, adj_rate := adj_close / close]
prices[, `:=`(
  open = open * adj_rate,
  high = high * adj_rate,
  low = low * adj_rate
)]
setnames(prices, "close", "close_raw")
setnames(prices, "adj_close", "close")
prices[, adj_rate := NULL]
setcolorder(prices, c("symbol", "date", "open", "high", "low", "close", "volume"))

# Calculate returns
prices[, returns := close / shift(close, 1) - 1, by = symbol]

# Remove missing values
prices = na.omit(prices)

# Create year week column
prices[, year_week := ceiling_date(date, unit = "week")]
pricesw = prices[, .(
  open = data.table::first(open),
  close = data.table::last(close),
  volume = sum(volume, na.rm = TRUE)
), by = .(symbol, year_week)]

# Create predictors
# pricesw[, mom4 := close / shift(close, 50) - 1, by = symbol]
pricesw[, mom4 := shift(close, 1) / shift(close, 4) - 1, by = symbol]

# Create target variable
pricesw[, target := close / open]
pricesw[, target := shift(target, 1, type = "lead") - 1, by = symbol]
pricesw = na.omit(pricesw)

# Some summary statistics
hist(pricesw$target, breaks = 100)
plot(pricesw[, close])
cor(pricesw[, .(mom4, target)])

# Regression
lm(target ~ mom4, data = pricesw[, .(mom4, target)])
ggplot(pricesw[, .(mom4, target)], aes(x = mom4, y = target)) +
  geom_point() +
  geom_smooth(method = "gam", color = "blue") +
  geom_smooth(method = "loess", color = "red") +
  geom_smooth(method = "lm", color = "green") +
  labs(title = "Regression of Target on Momentum (4 weeks)",
       x = "Momentum (4 weeks)",
       y = "Target")

# Bins
pricesw[, .(ret = mean(target)), by = mom4 > 0] |>
  ggplot(aes(x = mom4, y = ret)) +
    geom_bar(stat = "identity")
pricesw[, .(ret = median(target)), by = mom4 > 0] |>
  ggplot(aes(x = mom4, y = ret)) +
  geom_bar(stat = "identity")

# Simple backtest
pricesw[, signal := ifelse(mom4 > 0, 1, 0)]
pricesw[, strategy := signal * target]
portfolios = as.xts.data.table(pricesw[, .(year_week, strategy, target)])
charts.PerformanceSummary(portfolios)
