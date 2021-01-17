library(data.table) # fundamental
library(BatchGetSymbols)
library(tsibble)
library(quantmod)
library(TTR)
library(ggplot2)



# IMPORT DATA -------------------------------------------------------------

# get daily data for sp500
sp500 <- GetSP500Stocks()
prices <- BatchGetSymbols(sp500$Tickers, first.date = '1991-01-01')

# extract prices and meta
meta <- prices$df.control
prices <- prices$df.tickers
getSymbols(c("^VIX"),from = as.Date("1991-01-01"))
spy <- BatchGetSymbols('SPY', first.date = '1991-01-01')$df.tickers



# PREPARE DATA ------------------------------------------------------------

# covert to tsibble
prices_tsbl <- as_tsibble(prices, key = ticker, index = ref.date, regular = FALSE)

# number of observations above threshold
prices_tsbl <- prices_tsbl %>%
  group_by_key() %>%
  mutate(q_lower = roll::roll_quantile(ret.adjusted.prices, 255*4, p = 0.001, complete_obs = FALSE, min_obs = 200)) %>%
  mutate(q_upper = roll::roll_quantile(ret.adjusted.prices, 255*4, p = 0.999, complete_obs = FALSE, min_obs = 200)) %>%
  ungroup()
prices_tsbl <- prices_tsbl %>%
  mutate(below = ifelse(ret.adjusted.prices < q_lower, ret.adjusted.prices, 0)) %>%
  mutate(above = ifelse(ret.adjusted.prices > q_upper, ret.adjusted.prices, 0)) %>%
  mutate(excess_diff = above + below)
n_excess <- prices_tsbl %>%
  index_by() %>%
  summarise(below_excess = sum(below, na.rm = TRUE),
            above_excess = sum(above, na.rm = TRUE),
            excess_diff_sum = sum(excess_diff, na.rm = TRUE)) %>%
  mutate(excess_diff_std = roll::roll_sd(excess_diff_sum, 30),
         below_excess_std = roll::roll_sd(below_excess, 30),
         above_excess_std = roll::roll_sd(above_excess, 20))
spy_xts <- xts::xts(spy[, c('price.close', 'ret.adjusted.prices')], order.by = spy$ref.date)
spy_xts <- na.omit(merge(spy_xts, xts::xts(n_excess[, 2:ncol(n_excess)], n_excess$ref.date)))

# plots
plot(n_excess$below_excess, type = 'l')
ggplot(as.data.table(spy_xts), aes(x = index, y = price.close, color = below_excess_std > 0.2)) +
  geom_line(size = 1)

# merge vix and above threshold data
spy_vix_xts <- xts::xts(n_excess[, c('below_excess', 'below_excess_std', 'above_excess', 'excess_diff_sum')] * 10,
                        order.by = n_excess$ref.date)
spy_vix_xts <- merge(VIX$VIX.Close, spy_vix_xts)
spy_vix_xts <- na.omit(spy_vix_xts)
plot(spy_vix_xts[, c('VIX.Close', 'below_excess', 'above_excess', 'excess_diff_sum')])
plot(spy_vix_xts[4300:4700, c('VIX.Close', 'below_excess')])
plot(spy_vix_xts[4300:4700, c('VIX.Close', 'below_excess_std')])
plot(spy_vix_xts[, c('VIX.Close', 'below_excess_std')])


# BACKTEST ----------------------------------------------------------------

# trading rule
indicator <- as.vector(zoo::coredata(spy_xts$below_excess_std))
indicator_short <- SMA(as.vector(zoo::coredata(spy_xts$excess_diff_sum)), 40)
indicator_long <- SMA(as.vector(zoo::coredata(spy_xts$excess_diff_sum)), 200)
indicator_quantile <- roll::roll_quantile(indicator_short,
                                          width = 255*5,
                                          p = 0.95,
                                          min_obs = 100)
indicator_quantile_b <- roll::roll_quantile(indicator_short,
                                          width = 255*5,
                                          p = 0.05,
                                          min_obs = 100)

side <- vector(mode = 'integer', length = length(indicator))
for (i in 1:length(indicator)) {
  if (i == 1 || is.na(indicator[i-1])) {
    side[i] <- NA
  } else if (indicator[i-1] > 0.8) {  #  | indicator_short[i-1] < indicator_quantile_b[i-1]
    side[i] <- 0
  } else {
    side[i] <- 1
  }
}
table(side, useNA = 'ifany')
returns_strategy <- xts::xts(spy_xts$ret.adjusted.prices * side, order.by = zoo::index(spy_xts))
perf <- na.omit(merge(spy_xts[, c('ret.adjusted.prices')], returns_strategy = returns_strategy))
head(perf)
PerformanceAnalytics::charts.PerformanceSummary(perf)



# CORRELATION -------------------------------------------------------------

# head(prices_tsbl)
#
# library(corrr)
#
# data_corrr <- prices_tsbl[, c('ticker', "ref.date", "ret.adjusted.prices")]
# data_corrr <- tidyr::pivot_wider(data_corrr, id_cols = ref.date, names_from = ticker, values_from = ret.adjusted.prices)
# data_corrr_lastnyears <- data_corrr[(nrow(data_corrr) - (255 * 5)):nrow(data_corrr), ]
# data_corrr_lastnyears <- na.omit(data_corrr_lastnyears[, 2:ncol(data_corrr_lastnyears)])
# d <- correlate(data_corrr_lastnyears, quiet = TRUE)
#
# min_col <- which.min(apply(shave(d)[, 2:ncol(d)],2,min, na.rm = TRUE))
#
# d %>%
#   select(1, min_col + 1) %>%
#   arrange(.[[2]])
#   min(.[, 2], na.rm = TRUE)
#
#
#
# d %>%
#   select(1, (which.min(apply(d[, 2:ncol(d)],MARGIN=2,min, na.rm = TRUE)))) %>%
#   slice(which.min(unlist(d[, which.min(apply(d[, 2:ncol(d)],MARGIN=2,min, na.rm = TRUE))])))
#
# d %>%
#   # rearrange(absolute = FALSE) %>%
#   shave() %>%
#   slice(which.min(.))




