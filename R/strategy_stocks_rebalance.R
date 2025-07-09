library(fastverse)
library(finutils)
library(PerformanceAnalytics)


# DATA --------------------------------------------------------------------
# Import daily data
prices = qc_daily(
  file_path = "F:/lean/data/stocks_daily.csv",
  min_obs = 252,
  price_threshold = 0.0000001,
  market_symbol = "spy",
  add_dv_rank = TRUE
)

# Checks
key(prices)

# Define days of month
setorder(prices, symbol, date)
month_date = unique(prices[, .(month, date)])
setorder(month_date, date)
month_date[, day_of_month := 1:.N, by = .(month)]
prices = month_date[, .(date, day_of_month)][prices, on = c("date")]
prices[, day_of_month := as.factor(day_of_month)]
prices[day_of_month == 23, day_of_month := 22] # 23 day to 22 day
prices[day_of_month == 22, day_of_month := 21] # not sure about this but lets fo with it

# Downsample to monthly data
prices[, month := yearmon(date)]
pricesm = prices[, .(
  date = data.table::last(date),
  open = data.table::first(open),
  close = data.table::last(close),
  ret_1_15_current = .SD[day_of_month == 15, close] / data.table::first(open) - 1,
  ret_16_21_current = data.table::last(close) / .SD[day_of_month == 16, open] - 1,
  ret_1_2 = .SD[day_of_month == 2, close] / data.table::first(open) - 1,
  volume = sum(volume, na.rm = TRUE),
  close_raw = data.table::last(close_raw)
), by = .(symbol, month)]
pricesm[, ret_1_2_next := shift(ret_1_2, 1, type = "lead"), by = symbol]

# Check data
pricesm[symbol == "aapl"]
prices[symbol == "aapl"]
prices[symbol == "aapl"][date %between% c(as.Date("1998-01-01"), as.Date("1998-01-6"))]


# PREDICTORS --------------------------------------------------------------
# Calculate momemntum t-12 to t-2
pricesm[, mom_12_1 := shift(close, 1) / shift(close, 12) - 1, by = symbol]
pricesm[, mom_12_1_decile := dplyr::ntile(mom_12_1, 20), by = month]
pricesm[, ret_1_15_current := dplyr::ntile(ret_1_15_current, 20), by = month]
pricesm[, vol_bin := dplyr::ntile(volume, 20), by = month]

pricesm[vol_bin == 1]


# ALPHA -------------------------------------------------------------------
# Returns of mean reversion momentum strategy
pricesm[mom_12_1_decile == 20 & ret_1_15_current < 15, mean(ret_16_21_current, na.rm = TRUE)]
pricesm[mom_12_1_decile == 20 & ret_1_15_current < 15, median(ret_16_21_current, na.rm = TRUE)]
pricesm[mom_12_1_decile == 20 & ret_1_15_current < 15 & vol_bin > 11,
        mean(ret_16_21_current, na.rm = TRUE)]
pricesm[mom_12_1_decile == 20 & ret_1_15_current < 15 & vol_bin > 11,
        median(ret_16_21_current, na.rm = TRUE)]

# Backtest
back = pricesm[mom_12_1_decile %in% 18:20 & ret_1_15_current < 10]
back[month > 2022]
# back = back[vol_bin < 10]
back = back[, .(symbol, month, ret_16_21_current)]
back = na.omit(back)
back[, weight := 1 / .N, by = month]
portfolio = back[, .(ret = sum(weight * ret_16_21_current)), by = month]
setorder(portfolio, month)
portfolio_xts = as.xts.data.table(portfolio[, .(date = zoo::as.Date.yearmon(month), ret)])
portfolio_xts = portfolio_xts[portfolio_xts < 1]
charts.PerformanceSummary(portfolio_xts)
finutils::portfolio_stats(portfolio_xts)
SharpeRatio.annualized(portfolio_xts, scale = 12)

#
