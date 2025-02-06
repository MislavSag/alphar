library(fastverse)
library(DT)
library(tinyplot)
library(finutils)
library(PerformanceAnalytics)
library(ggplot2)
library(AzureStor)


# OPTIONS -----------------------------------------------------------------
# Get all paths for 2022 and 2023
paths = paste0("/home/sn/data/option/", 2020:2024)
files = unlist(lapply(paths, list.files, full.names = TRUE))

# Filtering process in the paper:
# For both implied volatility measures we use ATM straddle implied volatilities
# computed as the average volatility of the put and call options closest to
# at-the-money which survive the following filters:
# (1) The underlying equity has a closing price of at least $10;
# (2) The option price must not violate arbitrage conditions;
# (3) The option must have a non-zero bid;
# (4) The absolute value of the delta must be between 0.35 and 0.65; and
# (5) The implied volatility must be between 3% and 200%.
filter_options = function(dt) {
  # apply filter 1
  dt = dt[stkPx > 10]
  # apply filter 2
  dt = dt[abs(cBidPx - cAskPx) > 0]
  dt = dt[abs(pBidPx - pAskPx) > 0]
  # apply filter 3
  dt = dt[cBidPx > 0]
  dt = dt[pBidPx > 0]
  # apply filter 4
  dt = dt[abs(delta) > 0.35 & abs(delta) < 0.65]
  # apply filter 5
  dt = dt[cBidIv > 0.3 & cBidIv < 2]
  dt = dt[pBidIv > 0.3 & pBidIv < 2]

  return(dt)
}

# Read sample
dt = lapply(files, function(x) {
  print(x)
  x = fread(x, na.strings="NaN", fill=TRUE, sep=",")
  x[, delta := as.numeric(delta)]
  filter_options(x)
})
dt = rbindlist(dt)

# Order
setorder(dt, ticker, trade_date)

# Data types
dt[, let(
  expirDate = as.IDate(as.Date(expirDate, format = "%m/%d/%Y")),
  trade_date = as.IDate(as.Date(trade_date, format = "%m/%d/%Y"))
)]


# PRICES ------------------------------------------------------------------
# Import data
prices = qc_daily(
  file_path = "/home/sn/lean/data/stocks_daily.csv",
  price_threshold = 1e-8,
  min_obs = 252,
  market_symbol = NULL,
  symbols = NULL
)

# Be aware the key is set
key(prices)

# Fix symbol column
prices[, symbol_ajd := gsub("\\..*", "", toupper(symbol))]
max_dates = prices[, .(max_date = max(date)), by = .(symbol_ajd, symbol)]
one_symbol_per_ajd = max_dates[ , .SD[which.max(max_date)], by = symbol_ajd]
prices = prices[one_symbol_per_ajd, on = .(symbol_ajd, symbol), nomatch = 0]
prices[, symbol := gsub("\\..*", "", toupper(symbol))]
prices[, symbol_ajd := NULL]
ggplot(prices[symbol == "MSFT", .(symbol, date, close)], aes(x = date, y = close, color = symbol)) +
  geom_line()

# Calculate returns
prices[, returns := close / shift(close) - 1, by = symbol]

# Rolling 12-month realized volatility (252 trading days)
prices[, RVLT := sqrt(frollsum(returns^2, n = 252)), by = symbol]

# Create rank by volume for every date
prices[, dollar_vol_rank := frankv(close_raw * volume, order = -1L), by = date]
prices[date == sample(prices[, unique(date)], 1)][order(dollar_vol_rank)][1:20]

# Keep only symbols we have in optoins data
prices = prices[symbol %in% dt[, unique(ticker)]]


# ATM OPTIONS -------------------------------------------------------------
# Identify ATM Options
dt[, atm_diff := abs(strike - stkPx)]
atm_options = dt[, .SD[which.min(atm_diff)], by = .(ticker, trade_date, expirDate)]
head(atm_options)
atm_options[, , by = ]


# Calculate ATM Straddle Implied Volatility
atm_options[, atm_iv := (cMidIv + pMidIv) / 2]

# Define $IV_{1M}$ and $IV_{LT}$
# Calculate days to expiration
atm_options[, days_to_expiry := yte * 365]
short_term_iv = atm_options[
  days_to_expiry > 0 & days_to_expiry <= 60,  # Short-term filter
  .SD[which.min(abs(days_to_expiry - 30))],  # Closest to 30 days
  by = .(ticker, trade_date)  # Group by ticker and trade_date
]
long_term_iv = atm_options[
  days_to_expiry > 120 & days_to_expiry <= 220,  # Short-term filter
  .SD[which.min(days_to_expiry)],  # Shortest maturity ≥ 180 days
  by = .(ticker, trade_date)  # Group by ticker and trade_date
]

# Merge short-term and long-term IVs
iv_data = merge(short_term_iv, long_term_iv, by = c("ticker", "trade_date"),
                suffixes = c("_short", "_long"))

# Keep only necessary columns for further analysis of data
iv_data_simplified = iv_data[, .(
  ticker,
  trade_date,
  stkPx_short,
  IV1M = atm_iv_short,    # Rename short-term IV
  IVLT = atm_iv_long      # Rename long-term IV
)]

# Calculate SLOPE
iv_data_simplified[, SLOPE := (IVLT - IV1M) / IVLT]

# Set key to make merge faster
setkey(iv_data_simplified, ticker)
setorder(iv_data_simplified, ticker, trade_date)

# Merge realized volatility
iv_data_simplified = merge(
  iv_data_simplified,
  prices[, .(symbol, date, RVLT, close, open, dollar_vol_rank)],
  by.x = c("ticker", "trade_date"),
  by.y = c("symbol", "date"),
  all.x = TRUE,
  all.y = FALSE
)

# Order
setorder(iv_data_simplified, ticker, trade_date)

# Calculate IVRV slope
iv_data_simplified[, IVRV_SLOPE := (RVLT - IV1M) / RVLT]


# TRADING -----------------------------------------------------------------
# Inspect
atm_options[ticker == "AAPL" & trade_date == as.Date("2022-01-10")]

# Try for one expiration
atm_options_30d = atm_options[
  days_to_expiry > 0 & days_to_expiry <= 60,  # Short-term filter
  .SD[which.min(abs(days_to_expiry - 30))],  # Closest to 30 days
  by = .(ticker, trade_date)  # Group by ticker and trade_date
]
setorder(atm_options_30d, ticker, trade_date)

# Create columns for mid-call and mid-put
atm_options_30d[, mid_call := (cBidPx + cAskPx)/2 ]
atm_options_30d[, mid_put  := (pBidPx + pAskPx)/2 ]

# Sum to get total straddle cost
atm_options_30d[, straddle_price := mid_call + mid_put ]
atm_options_30d[, straddle_price_ask := cAskPx + pAskPx ]
atm_options_30d[, straddle_price_bid := cBidPx + pBidPx ]
atm_options_30d[, mean(straddle_price_ask - straddle_price)]
atm_options_30d[, mean((straddle_price_ask - straddle_price) / straddle_price_ask)]
atm_options_30d[, mean((straddle_price_bid - straddle_price) / straddle_price_bid)]
atm_options_30d[, median((straddle_price_ask - straddle_price) / straddle_price_ask)]
atm_options_30d[, median((straddle_price_bid - straddle_price) / straddle_price_bid)]

# Sort
setorder(atm_options_30d, ticker, trade_date)

# Plot Straddles
ggplot(atm_options_30d[ticker == "AAPL"], aes(x = straddle_price)) +
  geom_histogram()
ggplot(atm_options_30d[ticker == "V"], aes(x = straddle_price)) +
  geom_histogram()
ggplot(atm_options_30d[ticker == "SPY"], aes(x = straddle_price)) +
  geom_histogram()

# Inspect after strange results in QC. Let's see the vars again
# dt[, .SD[which.min(atm_diff)], by = .(ticker, trade_date, expirDate)]
dt[ticker == "AAPL"][trade_date == as.Date("2020-01-08")][]
atm_options[ticker == "AAPL"][trade_date == as.Date("2020-01-08")]
atm_options[ticker == "AAPL"][trade_date == as.Date("2020-01-07")]
atm_options_30d[ticker == "AAPL"]
# atm_options_30d[ticker == "AAPL"][days_to_expiry ]

# Merge atm data with iv_data_simplified data
back = merge(atm_options_30d[, .(ticker, trade_date, straddle_price,
                                 straddle_price_ask, straddle_price_bid, days_to_expiry)],
             iv_data_simplified,
             by = c("ticker", "trade_date"),
             all.x = TRUE,
             all.y = FALSE)
back = back[days_to_expiry %between% c(15, 45)]
back = back[data.table::wday(trade_date) == 3]
back = na.omit(back)
setorder(back, ticker, trade_date)
back = back[dollar_vol_rank < 150]
### IMPORTANT
# back[, wret := shift(straddle_price, 1, type = "lead") / straddle_price - 1, by = ticker]
back[, wret := shift(straddle_price_bid, 1, type = "lead") / straddle_price_ask - 1, by = ticker]
# back[, wret := shift(straddle_price_bid, 1, type = "lead") / straddle_price - 1, by = ticker]
### IMPORTANT

back = na.omit(back)
back[, q := dplyr::ntile(SLOPE, 10), by = trade_date]
back[, .(SLOPE = mean(SLOPE)), by = q][order(q)] |>
  ggplot(data=_, aes(x = q, y = SLOPE)) +
  geom_bar(stat = "identity") +
  scale_x_continuous(breaks = 1:10)
back_q = back[q == 1]
back_q[, weights := 1 / .N, by = trade_date]
back_q[, mean(wret, na.rm = TRUE)]
setorder(back_q, trade_date)
portfolio = back_q[, .(ret = sum(weights * (wret)-0.003)), by = trade_date]
setorder(portfolio, trade_date)
charts.PerformanceSummary(as.xts.data.table(portfolio))
charts.PerformanceSummary(as.xts.data.table(portfolio)["2020"])

# Plot returns across bins
back[, .(ret = mean(wret)), by = q][order(q)] |>
  ggplot(aes(q, ret)) +
  geom_bar(stat = "identity")
back[, .(ret = median(wret)), by = q][order(q)] |>
  ggplot(aes(q, ret)) +
  geom_bar(stat = "identity")

# Save to QC
qc_data = back_q[, .(date = trade_date, ticker)]
setorder(qc_data, date)
qc_data = qc_data[, .(symbol = paste0(ticker, collapse = "|")), by = date]
qc_data[, date := as.character(date)]
qc_data[, date := paste0(date, " 15:00:00")]
blob_key = Sys.getenv("BLOB-KEY-SNP")
endpoint = "https://snpmarketdata.blob.core.windows.net/"
BLOBENDPOINT = storage_endpoint(endpoint, key=blob_key)
cont = storage_container(BLOBENDPOINT, "qc-backtest")
delete_storage_file(cont, "sss2.csv", confirm = FALSE)
storage_write_csv(qc_data, cont, "sss2.csv")

# Inspect differences between QC and local
back_q[trade_date == as.Date("2023-01-01")]
head(back_q[trade_date > as.Date("2023-01-01")], 40)
back_q[trade_date > as.Date("2023-01-01")][ticker == "DVN"]

########### LONGER MATURITIES
# Try for one expiration
atm_options_30d = atm_options[
  days_to_expiry > 80 & days_to_expiry <= 160,  # Short-term filter
  .SD[which.min(abs(days_to_expiry - 120))],  # Closest to 30 days
  by = .(ticker, trade_date)  # Group by ticker and trade_date
]
setorder(atm_options_30d, ticker, trade_date)

# Create columns for mid-call and mid-put
atm_options_30d[, mid_call := (cBidPx + cAskPx)/2 ]
atm_options_30d[, mid_put  := (pBidPx + pAskPx)/2 ]

# Sum to get total straddle cost
atm_options_30d[, straddle_price := mid_call + mid_put ]
atm_options_30d[, straddle_price_ask := cAskPx + pAskPx ]
atm_options_30d[, straddle_price_bid := cBidPx + pBidPx ]
atm_options_30d[, mean(straddle_price_ask - straddle_price)]
atm_options_30d[, mean((straddle_price_ask - straddle_price) / straddle_price_ask)]
atm_options_30d[, mean((straddle_price_bid - straddle_price) / straddle_price_bid)]
atm_options_30d[, median((straddle_price_ask - straddle_price) / straddle_price_ask)]
atm_options_30d[, median((straddle_price_bid - straddle_price) / straddle_price_bid)]

# Sort
setorder(atm_options_30d, ticker, trade_date)

# Plot Straddles
ggplot(atm_options_30d[ticker == "AAPL"], aes(x = straddle_price)) +
  geom_histogram()
ggplot(atm_options_30d[ticker == "V"], aes(x = straddle_price)) +
  geom_histogram()
ggplot(atm_options_30d[ticker == "SPY"], aes(x = straddle_price)) +
  geom_histogram()

# Merge atm data with iv_data_simplified data
back = merge(atm_options_30d[, .(ticker, trade_date, straddle_price,
                                 straddle_price_ask, straddle_price_bid, days_to_expiry)],
             iv_data_simplified,
             by = c("ticker", "trade_date"),
             all.x = TRUE,
             all.y = FALSE)
back = back[days_to_expiry %between% c(90, 150)]
back = back[data.table::wday(trade_date) == 3]
back = na.omit(back)
setorder(back, ticker, trade_date)
### IMPORTANT
# back[, wret := shift(straddle_price, 1, type = "lead") / straddle_price - 1, by = ticker]
back[, wret := shift(straddle_price_bid, 1, type = "lead") / straddle_price_ask - 1, by = ticker]
back[, wret := shift(straddle_price_bid, 1, type = "lead") / straddle_price - 1, by = ticker]
### IMPORTANT

back = na.omit(back)
back[, q := dplyr::ntile(SLOPE, 10), by = trade_date]
back[, .(SLOPE = mean(SLOPE)), by = q][order(q)] |>
  ggplot(data=_, aes(x = q, y = SLOPE)) +
  geom_bar(stat = "identity") +
  scale_x_continuous(breaks = 1:10)
back_q = back[q == 10]
back_q[, weights := 1 / .N, by = trade_date]
back_q[, mean(wret, na.rm = TRUE)]
portfolio = back_q[, .(ret = sum(weights * (wret))), by = trade_date]
setorder(portfolio, trade_date)
charts.PerformanceSummary(as.xts.data.table(portfolio))
charts.PerformanceSummary(as.xts.data.table(portfolio)["2020"])

# Plot returns across bins
back[, .(ret = mean(wret)), by = q][order(q)] |>
  ggplot(aes(q, ret)) +
  geom_bar(stat = "identity")
back[, .(ret = median(wret)), by = q][order(q)] |>
  ggplot(aes(q, ret)) +
  geom_bar(stat = "identity")

# Save to QC
qc_data = back_q[, .(date = trade_date, ticker)]
setorder(qc_data, date)
qc_data = qc_data[, .(symbol = paste0(ticker, collapse = "|")), by = date]
qc_data[, date := as.character(date)]
qc_data[, date := paste0(date, " 00:00:00")]
blob_key = Sys.getenv("BLOB-KEY-SNP")
endpoint = "https://snpmarketdata.blob.core.windows.net/"
BLOBENDPOINT = storage_endpoint(endpoint, key=blob_key)
cont = storage_container(BLOBENDPOINT, "qc-backtest")
delete_storage_file(cont, "sss.csv", confirm = FALSE)
storage_write_csv(qc_data, cont, "sss.csv")

back_q[trade_date > as.Date("2020-01-01") & trade_date < as.Date("2020-01-15"), unique(ticker)]
