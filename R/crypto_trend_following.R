library(data.table)
library(fs)
library(arrow)
library(ggplot2)


# setup
PATH = "F:/data/crypto/binance"

# import futures data
dir_ls(path(PATH))
read_ = function(x, clean = TRUE) {
  dt_ = read_parquet(path(PATH, paste0(x, ".parquet")))
  if (clean) {
    dt_[, let(
      open_time = as.POSIXct(as.numeric(open_time / 1000)),
      close_time = as.POSIXct(as.numeric(close_time / 1000))
    )]
  }
  dt_= unique(dt_)
  return(dt_)
}
prices = read_("spot_klines_1h")

# Remove duplicates
prices = unique(prices, by = c("symbol", "open_time"))

# Sort
setorder(prices, symbol, open_time)

# Calculate mmomentum predictors
prices[, mom20 := close / shift(close, 20 * 24) - 1, by = symbol]

# Create target variable
prices[, target_1 := shift(close, -24, type = "shift") /close - 1, by = symbol]

# Remove missing targets and mom
prices = na.omit(prices, cols = c("target_1", "mom20"))

# Remove overlapping target observations (basically make data daily)
prices[, time := data.table::as.ITime(open_time)]
prices = prices[time == as.ITime("00:00:00")]

# Scaterplots mom vs target
prices[grep("^BTC", symbol), unique(symbol)]
tickers = c("BTCUSDT", "ETHUSDT", "XRPUSDT", "DOGEUSDT", "DOTUSDT", "UNIUSDT")
# tickers = c("BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "XRPUSDT", "DOGEUSDT", "DOTUSDT", "UNIUSDT", "LINKUSDT", "LTCUSDT")
prices[symbol %in% tickers] |>
  # group_by(ticker) |>
  ggplot(aes(x=mom20, y=target_1)) +
  geom_point() +
  geom_smooth(method = 'lm') +
  facet_wrap(~symbol) +
  ggtitle('Next day log returns vs 20 day momentum')

# Trend
prices[, trend := mom20 > 0]
prices[, .(mean_ret = mean(target_1, na.rm = TRUE)), by = .(symbol, trend)][symbol %in% tickers] |>
  ggplot(aes(x=trend, y = mean_ret, fill = trend)) +
  geom_bar(stat = 'identity') +
  facet_wrap(~symbol) +
  ggtitle('Mean next day returns when 20 day trend is positive (green) or negative (red)')

# Distribution of returns
prices[symbol %in% tickers] |>
  ggplot(aes(x = target_1, color = trend)) +
  geom_density() +
  facet_wrap( ~ symbol) +
  ggtitle('Total returns trading when 20 day trend is positive (green) or negative (red)')

# Sort our trend observations into 5 buckets
prices[, trend_quintile := cut(mom20,
                               breaks = quantile(mom20, probs = 0:5/5),
                               labels = 1:5,
                               right = FALSE)]
prices[, .(mean_ret_q = mean(target_1, na.rm = TRUE)), by = .(symbol, trend_quintile)][
  symbol %in% tickers] |>
  ggplot(aes(x=trend_quintile, y = mean_ret_q, fill = trend_quintile)) +
  geom_bar(stat = 'identity') +
  facet_wrap(~symbol) +
  ggtitle('Mean next day log returns by 20 day momentum quintile')

# What about medians?
prices[, .(median_ret_q = median(target_1, na.rm = TRUE)), by = .(symbol, trend_quintile)][
  symbol %in% tickers] |>
  ggplot(aes(x=trend_quintile, y = median_ret_q, fill = trend_quintile)) +
  geom_bar(stat = 'identity') +
  facet_wrap(~symbol) +
  ggtitle('Median next day log returns by 20 day momentum quintile')

# Let's do a quick, costless, simulation of a strategy that just flips long or
# short based on the sign of the 20 day momentum factor.
trend_strat = prices[, let(
  longonly = target_1,
  momo_long = trend * target_1,
  momo_ls = fcase(trend == TRUE, target_1,
                  trend == FALSE, -target_1)
), by = symbol]
trend_strat = trend_strat[, .(symbol, open_time, trend, longonly, momo_long, momo_ls)]
trend_strat = melt(trend_strat,
                   id.vars = c("symbol", "open_time", "trend"),
                   variable.name = "strategy",
                   value.name = "returns")
trend_strat[symbol %in% tickers]
setorder(trend_strat, symbol, open_time)
trend_strat[, .(open_time, cumreturns = cumsum(returns)), by = .(symbol, strategy)][symbol %in% tickers] |>
  ggplot(aes(x=open_time, y=cumreturns, color = strategy)) +
  geom_line() +
  facet_wrap(~symbol) +
  ggtitle('Rolling 12 month returns - Timing Strategy (Green) vs Equivalent Long Exposure')

# Rolling returns
na.omit(trend_strat[, .(open_time, rolling_returns = frollsum(returns, 365, na.rm = TRUE)),
            by = .(symbol, strategy)])[symbol %in% tickers] |>
  ggplot(aes(x=open_time, y=rolling_returns, color = strategy)) +
  geom_line() +
  facet_wrap(~symbol) +
  ggtitle('Simple Binary Trend Strategy')

# Plot out feature
prices[symbol %in% tickers] |>
  ggplot(aes(x=open_time, y=mom20)) +
  geom_line() +
  facet_wrap(~symbol)

# I'd like to scale it into a workable range, whilst still respecting the sign of the feature.
prices[, roll_sd := roll::roll_sd(mom20, 20), by = symbol]
prices[, scaled_trend := mom20 / roll_sd]
prices[symbol %in% tickers] |>
  ggplot(aes(x = open_time, y = scaled_trend)) +
  geom_line() +
  facet_wrap(~symbol)

# Let's see if we can use the scaled feature to predict the target
prices[, weight := 0.75 * pmax(pmin(scaled_trend, 2), -2)]
na.omit(prices)[symbol %in% tickers] |>
  ggplot(aes(x = open_time, y = weight)) +
  geom_line() +
  facet_wrap(~symbol)

