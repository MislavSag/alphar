library(finutils)
library(fastverse)
library(arrow)
library(PerformanceAnalytics)
library(ggplot2)


# Import daily OHLCV data
prices = qc_daily(
  file_path = "F:/lean/data/stocks_daily.csv",
  min_obs = 252,
  price_threshold = 0.000001,
  add_dv_rank = TRUE
)
symbols_all = prices[, unique(symbol)]
symbols_all = toupper(gsub("\\..*", "", symbols_all))

# Import market cap data
mcap = read_parquet("F:/data/equity/us/fundamentals/market_cap.parquet")
mcapm = mcap[symbol %in% symbols_all,
             .(market_cap = sum(marketCap, na.rm = TRUE)),
             by = .(symbol, month = data.table::yearmon(date))]

# Downsample
pricesm = prices[, .(
  open = data.table::first(open),
  close = data.table::last(close),
  volume = sum(volume),
  dollar_vol_rank = mean(dollar_vol_rank),
  close_raw = data.table::last(close_raw)
  ),
by = .(symbol, month = data.table::yearmon(date))]
pricesm[, target := close / open - 1]
pricesm[, target := shift(target, 1, type = "lead")]
pricesm = na.omit(pricesm)

# Summary
pricesm[, summary(close_raw)]
pricesm[, quantile(close_raw, probs = c(0.001, 0.999))]

# Plots
ggplot(pricesm, aes(close_raw)) +
  geom_histogram()

# Ranking
pricesm = pricesm[close_raw < 1000]
pricesm[, closer := frankv(close, order = -1L), by = month]
pricesm[, close_rawr := frankv(close_raw, order = -1L), by = month]
pricesm[month == 2020][order(closer)]
pricesm[month == 2020][order(close_rawr)]

# Backtest
n = 30
var = "close"
back = pricesm[x < n, env = list(x = var)]
back[, weight := 1 / nrow(.SD), by = month]
portfolio = back[, .(ret = sum(weight * target)), by = month]
setorder(portfolio, month)
portfolio_xts = as.xts.data.table(portfolio[, .(zoo::as.Date.yearmon(month), ret)])
charts.PerformanceSummary(portfolio_xts)
charts.PerformanceSummary(portfolio_xts[portfolio_xts$ret < 1]) # Remove outlier
spy_xts = pricesm[symbol == "spy",
                  .(zoo::as.Date.yearmon(month), spy = target)]
spy_xts = as.xts.data.table(spy_xts)
portfolio_stats(portfolio_xts, market = spy_xts)

# Backtest with market cap
n = 30
var = "close_rawr"
months_ = pricesm[month >= 2000, sort(unique(month))]
returns_l = list()
for (i in seq_along(months_)) {
  # m = months_[1]
  m = months_[i]
  print(m)

  # Exlude n%biggest firms
  dt_ = pricesm[, .(symbol = toupper(gsub("\\..*", "", symbol)), month, target, close_raw)] |>
    _[mcapm[month == m], on = c("symbol", "month")]
  dt_ = na.omit(dt_)

  # Rank by marketcap
  dt_[, market_cap_rank := frankv(market_cap, order = -1L), by = month]
  dt_ = dt_[market_cap_rank > (nrow(dt_) * (3/4))]

  #
  dt_[, close_rawr := frankv(close_raw, order = -1L), by = month]
  dt_ = dt_[x < n, env = list(x = var)]
  dt_[, weight := 1 / nrow(.SD), by = month]

  # Calculate portoflio returns
  portfolio = dt_[, .(ret = sum(weight * target)), by = month]
  setorder(portfolio, month)
  returns_l[[i]] = as.xts.data.table(portfolio[, .(zoo::as.Date.yearmon(month), ret)])
}
portfolio_mcap_xts = rbindlist(lapply(returns_l, as.data.table))
charts.PerformanceSummary(portfolio_mcap_xts)

