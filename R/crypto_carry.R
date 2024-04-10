library(data.table)
library(fs)
library(cryptoQuotes)
library(xts)
library(ggplot2)
library(arrow)


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
cm_indexpriceklines_hour  = read_("futures_cm_indexpriceklines_1h")
cm_indexpriceklines_daily = read_("futures_cm_indexpriceklines_1d")
futures_cm_klines_hour    = read_("futures_cm_klines_1h")
futures_cm_klines_daily   = read_("futures_cm_klines_1d")
spot_klines_daily         = read_("spot_klines_1d")
spot_klines_hour          = read_("spot_klines_1h")
cm_funding_rates          = read_("futures_cm_fundingrate", FALSE)
cm_funding_rates[, let(calc_time = as.POSIXct(as.numeric(calc_time / 1000)))]

# Upasample funding rates to daily
cm_funding_rates[, date := as.IDate(calc_time)]
funding_daily = cm_funding_rates[
  , .(rate = sum(last_funding_rate, na.rm = TRUE)), by = c("symbol", "date")
  ]

# Summary statistics
cm_funding_rates[, .(
  mean = mean(last_funding_rate, na.rm = TRUE),
  median = median(last_funding_rate, na.rm = TRUE),
  sd = sd(last_funding_rate, na.rm = TRUE),
  min = min(last_funding_rate, na.rm = TRUE),
  max = max(last_funding_rate, na.rm = TRUE)
)]

# Visualize some rates
symb_ = cm_funding_rates[grep("BTC", symbol), unique(symbol)]
dt_ = cm_funding_rates[symbol == symb_, .(calc_time, last_funding_rate)]
xts_ = as.xts.data.table(unique(dt_[, .(calc_time, last_funding_rate)]))
plot(xts_, main=paste0(symb_, " funding rate"))

# Average daily funding
avg_daily_fun = funding_daily[, .(rate = mean(rate, na.rm = TRUE)), by = "symbol"]
all_coins_avg = avg_daily_fun[, mean(rate, na.rm = TRUE)]
all_coins_avg * 100 * 365
avg_daily_fun %>%
  ggplot(aes(x = symbol, y = rate*100*365)) +
  geom_bar(stat = "identity") +
  coord_flip() +
  labs(
    title = "Average daily funding, annualised",
    y = "Annualised funding rate, %"
  )

# costs
cost = 4*0.07/100
horizon = cost/all_coins_avg
# horizon = 2

# Predict next 22 days rates from 5 days history
cols = paste0("rate_", 1:4)
funding_daily[, (cols) := shift(rate, 1:4), by = symbol]
funding_daily[, predicted_funding := as.integer(horizon) * rowMeans(.SD, na.rm = TRUE)
              , .SDcols = cols]
funding_daily[, (cols) := NULL]
cols = paste0("rate_", 1:as.integer(horizon))
funding_daily[, (cols) := shift(rate, -1:-as.integer(horizon), type = "shift")
              , by = symbol]
funding_daily[, actual := rowSums(.SD, na.rm = TRUE), .SDcols = cols]
funding_daily[, (cols) := NULL]

# Here's a plot of predicted funding vs actual funding:
funding_daily %>%
  na.omit() %>%
  ggplot(aes(x = actual, y = predicted_funding)) +
  geom_point() +
  geom_smooth(method = "lm") +
  labs(title = "Predicted vs actual funding, all data")

# And with overlapping data removed:
keep_rows = funding_daily[, .I %% as.integer(horizon) == 0, by = symbol]
funding_daily[keep_rows[, V1]] %>%
  na.omit() %>%
  ggplot(aes(x = actual, y = predicted_funding)) +
  geom_point() +
  geom_smooth(method = "lm") +
  labs(title = "Predicted vs actual funding, overlapping data removed")

# winsorize
data_plot = copy(funding_daily)
data_plot = data_plot[keep_rows[, V1]]
data_plot[, actual := DescTools::Winsorize(actual, probs = c(0.01, 0.99))]
data_plot[, predicted_funding := DescTools::Winsorize(predicted_funding,
                                                      probs = c(0.01, 0.99),
                                                      na.rm = TRUE)]
data_plot %>%
  na.omit() %>%
  ggplot(aes(x = actual, y = predicted_funding)) +
  geom_point() +
  geom_smooth(method = "lm") +
  labs(title = "Predicted vs actual funding, overlapping data removed")

# Residuals
data_plot[, residual := predicted_funding - actual]
data_plot %>%
  ggplot(aes(x = actual, y = residual)) +
  geom_point() +
  geom_smooth(method = "loess") +
  labs(title = "Model residuals, overlapping data removed")

# cutoff value
cutoff = 1e6 # remove from universe all assets with volume < cutoff

# merge futures and spot data
futures_cm_klines_daily[, unique(symbol)]
universe_df = futures_cm_klines_daily[symbol %like% "PERP", .(
  symbol = gsub("_PERP|USD", "", symbol),
  date = as.Date(close_time),
  close_perp = close,
  volume_perp = volume
)]
spot_klines_daily[symbol %like% "^BTC.*USD", unique(symbol)]
universe_df = spot_klines_daily[, .(
    symbol = gsub("USDT", "", symbol),
    date = as.Date(close_time),
    close_spot = close
  )][universe_df, on = c("symbol", "date") ]

# plot spot and future prices for some symbols
data_ = as.xts.data.table(universe_df[symbol %chin% c("BTC"), .(date, close_spot, close_perp)])
plot(data_)
plot(tail(data_, 50))
data_ = as.xts.data.table(universe_df[symbol %chin% c("BTC"), .(date, volume_perp)])
plot(data_)

# missing values
universe_df[!is.na(close_spot)]
universe_df = na.omit(universe_df)

# sort
setorder(universe_df, symbol, date)

# get 5-day rolling mean volume
universe_df[, roll_mean_volume := frollmean(volume_perp, 5), by = symbol]
universe_df[, is_universe := 0]
universe_df[shift(roll_mean_volume) > cutoff, is_universe := 1]
head(universe_df, 99)

# Check how many perpetuals are in our universe given these criteria
universe_df[, .(universe_size = sum(is_universe, na.rm = TRUE)), by = date] |>
  ggplot(aes(x = date, y = universe_size)) +
  geom_line() +
  labs(title = sprintf("Universe size, 5-day average volume > %d", cutoff))

# Create daily funging rates and calculate predicted rates
funding_daily = copy(cm_funding_rates)
funding_daily[, rate := shift(last_funding_rate, -1, type = "shift"), by = symbol]
funding_daily = funding_daily[, .(rate = sum(rate, na.rm = TRUE)), by = c("symbol", "date")]
cols = paste0("rate_", 1:4)
funding_daily[, (cols) := shift(rate, 1:4), by = symbol]
funding_daily[, predicted_funding := as.integer(horizon) * rowMeans(.SD, na.rm = TRUE)
              , .SDcols = cols]
funding_daily[, (cols) := NULL]
funding_daily[, symbol := gsub("_PERP|USD", "", symbol)]

# backtest data
backtest_df = merge(universe_df, funding_daily, by = c("symbol", "date"),
                    all.x = TRUE, all.y = FALSE)
backtest_df[, let(
  return_perp = close_perp / shift(close_perp) - 1,
  return_spot = close_spot / shift(close_spot) - 1
), by = "symbol"]
backtest_df[, let(
  fwd_return_perp = shift(return_perp, -1, "shift"),
  fwd_return_spot = shift(return_spot, -1, "shift"),
  fwd_funding = shift(rate, -2, type = "shift")
), by = "symbol"]

## MY LINE
backtest_df = na.omit(backtest_df, cols = c("rate"))

#  Next we take positions in the top ten by predicted funding, over some threshold.
threshold_ann = 0.2  # annualised funding
threshold = threshold_ann/(365/7.)  # next 7-day's funding
top_n = 10

positions_df = backtest_df[is_universe == TRUE]
positions_df = positions_df[predicted_funding > threshold]
setorder(positions_df, date, -predicted_funding)
positions_df[, rank := rowid(date)]
data.table::first(positions_df, 20)
positions_df[, pos := 0]
positions_df[rank <= top_n, pos := 1]
positions_df[, num_positions := sum(pos), by = date]

# plot positions
positions_df |>
  ggplot(aes(x = date, y = num_positions)) +
  geom_line() +
  labs(title = "Check number of positions per day")

# inspect why are there more than 10 positions
positions_df[num_positions > 15]
positions_df[date == as.Date("2023-01-18")]

# Next, we'll estimate costs in basis points.
cost = 0.12/100

# 3)
backtest_dt = copy(positions_df)
backtest_dt[is.na(pos), pos := 0]
backtest_dt[, let(diff_pos = c(1, abs(diff(pos)))), by = symbol]
backtest_dt[diff_pos == 1]
backtest_dt[, costs := 0]
backtest_dt[diff_pos == 1, costs := cost]

# 2)
# backtest_dt = backtest_df[, .(symbol, date, fwd_return_perp, fwd_return_spot, fwd_funding)]
# backtest_dt = backtest_dt[positions_df[, .(symbol, date, pos)], on = .(symbol, date)]
# backtest_dt[!is.na(pos)]
# backtest_dt[is.na(pos), pos := 0]
# backtest_dt[, let(diff_pos = c(1, abs(diff(pos)))), by = symbol]
# backtest_dt[diff_pos == 1]
# backtest_dt[, costs := 0]
# backtest_dt[diff_pos == 1, costs := cost]

# 1)
# backtest_dt = backtest_df[, .(symbol, date, fwd_return_perp, fwd_return_spot, fwd_funding)]
# backtest_dt = backtest_dt[positions_df, on = .(symbol, date), nomatch = 0]
# backtest_dt[is.na(pos), pos := 0]
# backtest_dt[, `:=`(
#   diff_pos = c(1, abs(diff(pos))),
#   costs = 0  # Initialize costs column
# ), by = symbol]
# backtest_dt[diff_pos == 1, costs := cost]


backtest_dt %>%
  dplyr::group_by(date) %>%
  dplyr::summarise(
    num_positions = sum(pos),
    tot_cost = sum(costs)/num_positions) %>%
  ggplot(aes(x = date, y = tot_cost*10000)) +
  geom_line() +
  labs(
    title = "Check on daily total costs",
    y = "Total cost (bps)"
  )

# P&L
capital = 1000

library(dplyr)
results_df = backtest_dt %>%
  dplyr::group_by(date) %>%
  dplyr::summarise(
    num_positions = sum(pos),
    # pnl to being equal weight in the short perp positions (half our capital is in perps)
    perp_pnl = dplyr::case_when(num_positions > 0 ~ sum(-pos*fwd_return_perp*capital/(2*num_positions), na.rm = TRUE), TRUE ~ 0.),
    # pnl to being equal weight in the long spot positions (half our capital is in spot)
    spot_pnl = dplyr::case_when(num_positions > 0 ~ sum(pos*fwd_return_spot*capital/(2*num_positions), na.rm = TRUE), TRUE ~ 0.),
    # pnl from change in basis
    basis_change_pnl = (perp_pnl + spot_pnl),
    # pnl from funding on positions (we get funding on the half of our capital which is short perps)
    funding_pnl = case_when(num_positions > 0 ~ sum(pos*fwd_funding*capital/(2*num_positions)), TRUE ~ 0.),
    # costs accrue proportionally to each position (noting no minimum commission)
    costs = case_when(num_positions > 0 ~ -sum(costs*capital/num_positions, na.rm = TRUE), TRUE ~ 0.),
    total_pnl = basis_change_pnl + funding_pnl + costs
  )

results_df %>%
  select(date, basis_change_pnl, funding_pnl, costs, total_pnl) %>%
  tidyr::pivot_longer(-date, names_to = "pnl_source", values_to = "pnl") %>%
  ggplot(aes(x = date, y = pnl, colour = pnl_source)) +
  geom_line() +
  labs(title = "PnL by source")

options(repr.plot.width = 14, repr.plot.height=15)
results_df %>%
  filter(date >= as.data.table(results_df)[, min(date)]) %>%
  mutate(
    basis_cum_pnl = cumsum(basis_change_pnl),
    funding_cum_pnl = cumsum(funding_pnl),
    costs = cumsum(costs),
    total_cum_pnl = cumsum(total_pnl)
  ) %>%
  select(date, basis_cum_pnl, funding_cum_pnl, costs, total_cum_pnl) %>%
  tidyr::pivot_longer(-date, names_to = "pnl_source", values_to = "pnl") %>%
  ggplot(aes(x = date, y = pnl, colour = pnl_source)) +
  geom_line() +
  facet_wrap(~pnl_source, scales = "free_y", ncol = 1) +
  labs(
    title = "PnL to price change, funding, and costs. Starting capital ",
    subtitle = "Note different y-axes"
  )
options(repr.plot.width = 14, repr.plot.height=7)


na.omit(results_df) %>%
  summarise(
    Ann.Return = mean(total_pnl)*365/capital,
    Ann.Vol = sd(total_pnl/capital)*sqrt(365),
    Ann.Sharpe = Ann.Return/Ann.Vol
  ) %>%
  mutate(across(.cols = everything(), .fns = ~ round(.x, 3)))
