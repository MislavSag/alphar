library(data.table)
library(finutils)
library(roll)
library(RollingWindow)
library(ggplot2)
library(quantmod)
library(PerformanceAnalytics)


# Risk free rate
getSymbols("TB3MS", src = "FRED")
TB3MS = as.data.table(TB3MS)
TB3MS = TB3MS[, .(rf = data.table::last(TB3MS) / 100), by = .(month = data.table::yearmon(index))]

# Load daily data
prices = qc_daily(
  file_path = "F:/lean/data/stocks_daily.csv",
  market_cap_fmp_file = "F:/data/equity/us/fundamentals/market_cap.parquet",
  min_obs = 252,
  duplicates = "fast",
  market_symbol = "spy",
  profiles_fmp = TRUE,
  fmp_api_key = "6c390b6279b8a67cc7f63cbecbd69430" # Sys.getenv("APIKEY-FMPCLOUD")
)
prices[, beta := RollingBeta(market_ret, returns, window = 252), by = symbol] # , expanding = TRUE

# Downsample to monthly data
pricesm  = prices[, .(
  open   = data.table::first(open),
  close  = data.table::last(close),
  volume = sum(volume, na.rm = TRUE),
  raw    = data.table::last(close_raw),
  mcap   = data.table::last(marketCap),
  isetf  = data.table::last(isEtf),
  isfund = data.table::last(isFund),
  beta   = data.table::last(beta)
), by = .(symbol, month = data.table::yearmon(date))]

# Create target returns
pricesm[, target := close / open - 1]
pricesm[, target := shift(target, type = "lead", n = 1), by = symbol]
pricesm = na.omit(pricesm, cols = "target")

# Check target returns
pricesm[, any(target > 100)]

# Extract SPY for market (excess) returns
spy = pricesm[symbol == "spy", .(month, mkt_excess = target)]

# Mean and median returns by deciles
pricesm[, .(symbol, month, beta, target)] |>
  na.omit() |>
  _[,.(ret = mean(target, na.rm = TRUE)), by = .(ntile = dplyr::ntile(beta, 10))] |>
  _[order(ntile)] |>
  ggplot(data = _, aes(x = as.factor(ntile), y = ret)) +
    geom_col() +
    labs(x = "Beta Decile", y = "Average Return") +
    theme_minimal()
pricesm[, .(symbol, month, beta, target)] |>
  na.omit() |>
  _[,.(ret = median(target, na.rm = TRUE)), by = .(ntile = dplyr::ntile(beta, 10))] |>
  _[order(ntile)] |>
  ggplot(data = _, aes(x = as.factor(ntile), y = ret)) +
  geom_col() +
  labs(x = "Beta Decile", y = "Average Return") +
  theme_minimal()

# Mean and median returns by deciles for non-etf and non-funds
pricesm[isetf == FALSE & isfund == FALSE, .(symbol, month, beta, target)] |>
  na.omit() |>
  _[,.(ret = mean(target, na.rm = TRUE)), by = .(ntile = dplyr::ntile(beta, 10))] |>
  _[order(ntile)] |>
  ggplot(data = _, aes(x = as.factor(ntile), y = ret)) +
  geom_col() +
  labs(x = "Beta Decile", y = "Average Return") +
  theme_minimal()
pricesm[isetf == FALSE & isfund == FALSE, .(symbol, month, beta, target)] |>
  na.omit() |>
  _[,.(ret = median(target, na.rm = TRUE)), by = .(ntile = dplyr::ntile(beta, 10))] |>
  _[order(ntile)] |>
  ggplot(data = _, aes(x = as.factor(ntile), y = ret)) +
  geom_col() +
  labs(x = "Beta Decile", y = "Average Return") +
  theme_minimal()

# Mean weighted returns by deciles
pricesm[, .(symbol, month, beta, target, mcap)] |>
  na.omit() |>
  _[,.(ret = weighted.mean(target, w = mcap)), by = .(ntile = dplyr::ntile(beta, 10))] |>
  _[order(ntile)] |>
  ggplot(data = _, aes(x = as.factor(ntile), y = ret)) +
  geom_col() +
  labs(x = "Beta Decile", y = "Average Value Weighted Return") +
  theme_minimal()
pricesm[isetf == FALSE & isfund == FALSE, .(symbol, month, beta, target, mcap)] |>
  na.omit() |>
  _[,.(ret = weighted.mean(target, w = mcap)), by = .(ntile = dplyr::ntile(beta, 10))] |>
  _[order(ntile)] |>
  ggplot(data = _, aes(x = as.factor(ntile), y = ret)) +
  geom_col() +
  labs(x = "Beta Decile", y = "Average Value Weighted Return") +
  theme_minimal()

# Remove penny stocks
pricesm[isetf == FALSE & isfund == FALSE & raw > 1 & volume > 100000, .(symbol, month, beta, target)] |>
  na.omit() |>
  _[,.(ret = mean(target, na.rm = TRUE)), by = .(ntile = dplyr::ntile(beta, 10))] |>
  _[order(ntile)] |>
  ggplot(data = _, aes(x = as.factor(ntile), y = ret)) +
  geom_col() +
  labs(x = "Beta Decile", y = "Average Return") +
  theme_minimal()
pricesm[isetf == FALSE & isfund == FALSE & raw > 1 & volume > 100000, .(symbol, month, beta, target, mcap)] |>
  na.omit() |>
  _[,.(ret = weighted.mean(target, w = mcap)), by = .(ntile = dplyr::ntile(beta, 10))] |>
  _[order(ntile)] |>
  ggplot(data = _, aes(x = as.factor(ntile), y = ret)) +
  geom_col() +
  labs(x = "Beta Decile", y = "Average Return") +
  theme_minimal()

# Performance evaluation
beta_portfolios = pricesm[isetf == FALSE & isfund == FALSE & target < 1000,
                          .(symbol, month, beta, target, mcap)] |>
  na.omit() |>
  TB3MS[i = _, on = "month"] |>
  _[,.(ret_excess = mean(target, na.rm = TRUE)), by = .(month, ntile = dplyr::ntile(beta, 10))] |>
  # _[,.(ret_excess = mean(target - rf, na.rm = TRUE)), by = .(month, ntile = dplyr::ntile(beta, 10))] |>
  # _[,.(ret_excess = weighted.mean(target, w = mcap, na.rm = TRUE)), by = .(month, ntile = dplyr::ntile(beta, 10))] |>
  # _[,.(ret_excess = weighted.mean(target - rf, w = mcap, na.rm = TRUE)), by = .(month, ntile = dplyr::ntile(beta, 10))] |>
  _[order(ntile, month)] |>
  _[spy, on = "month"] |>
  na.omit()

beta_portfolios_summary = beta_portfolios[, broom::tidy(lm(ret_excess ~ 1 + mkt_excess, data = .SD)), by = .(ntile)]
beta_portfolios_summary = beta_portfolios_summary[, .(ntile, term, estimate)]
beta_portfolios_summary = dcast(beta_portfolios_summary, ntile ~ term, value.var = "estimate")
setnames(beta_portfolios_summary, c("ntile", "alpha", "beta"))

library(scales)
beta_portfolios_summary |>
  ggplot(aes(x = ntile, y = alpha, fill = ntile)) +
  geom_bar(stat = "identity") +
  labs(
    title = "CAPM alphas of beta-sorted portfolios",
    x = "Portfolio",
    y = "CAPM alpha",
    fill = "Portfolio"
  ) +
  scale_y_continuous(labels = percent) +
  theme(legend.position = "None")

#
sml_capm = pricesm[isetf == FALSE & isfund == FALSE & target < 1000,
                   .(symbol, month, beta, target, mcap)] |>
  na.omit() |>
  # TB3MS[i = _, on = "month"] |>
  _[,.(beta, ret_excess = mean(target, na.rm = TRUE)), by = .(month, ntile = dplyr::ntile(beta, 10))] |>
  _[order(ntile, month)] |>
  _[spy, on = "month"] |>
  na.omit() |>
  lm(ret_excess ~ 1 + beta, data = _)
sml_capm$coefficients

beta_portfolios_summary |>
  ggplot(aes(
    x = beta,
    y = ret_excess,
    color = portfolio
  )) +
  geom_point() +
  geom_abline(
    intercept = 0,
    slope =  beta_portfolios[, mean(mkt_excess)],
    # slope = mean(factors_ff3_monthly$mkt_excess),
    linetype = "solid"
  ) +
  geom_abline(
    intercept = sml_capm[1],
    slope = sml_capm[2],
    linetype = "dashed"
  ) +
  scale_y_continuous(
    labels = percent,
    limit = c(0, beta_portfolios[, mean(mkt_excess)] * 2)
  ) +
  scale_x_continuous(limits = c(0, 2)) +
  labs(
    x = "Beta", y = "Excess return", color = "Portfolio",
    title = "Average portfolio excess returns and average beta estimates"
  )

# Portfolio sort strategy
portfolio = pricesm[isetf == FALSE & isfund == FALSE & target < 1000,
                    .(symbol, month, beta, target, mcap)]
portfolio = na.omit(portfolio)
setorder(portfolio, month, beta)
portfolio = portfolio[, head(.SD, 20), by = month]
portfolio[, weight:= 1 / length(beta), by = month]
portfolio = portfolio[, .(ret = sum(target * weight)), by = month]
setorder(portfolio, month)
portfplio_xts = as.xts.data.table(portfolio[, .(date = zoo::as.Date.yearmon(month), ret)])
charts.PerformanceSummary(portfplio_xts)
SharpeRatio.annualized(portfplio_xts)


