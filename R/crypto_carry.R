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
read_ = function(x) read_parquet(path(PATH, paste0(x, ".parquet")))
cm_indexpriceklines_hour  = read_("futures_cm_indexpriceklines_1h")
cm_indexpriceklines_daily = read_("futures_cm_indexpriceklines_1d")
cm_funding_rates          = read_("futures_cm_fundingrate")
spot_klines_1d

klines               = get_data(PATH,"klines", "1h")
klines_daily         = get_data(PATH, "klines", "1d")
klines_spot_daily    = get_data(PATHSPOT, "klines", "1d")
setnames(klines_spot_daily, colnames(klines_daily))

# clean function
clean_market_data = function(dt_) {
  dt_[, `:=`(
    open_time = as.POSIXct(as.numeric(open_time / 1000)),
    close_time = as.POSIXct(as.numeric(close_time / 1000))
  )]
  dt_
}
index_price_klines_dt = clean_market_data(index_price_klines)
premium_index_klines_dt = clean_market_data(premium_index_klines)
klines_dt       = clean_market_data(klines)
klines_daily_dt = clean_market_data(klines_daily)
klines_spot_daily_dt = clean_market_data(klines_spot_daily)
funding_rate_dt = unique(funding_rate, by = c("symbol", "calc_time"))
funding_rate_dt[, calc_time := as.POSIXct(as.numeric(calc_time / 1000))]

# Upasample dtas to daily
funding_rate_dt[, date := as.IDate(calc_time)]
funding_daily = funding_rate_dt[, .(rate = sum(last_funding_rate, na.rm = TRUE)),
                                by = c("symbol", "date")]

# Summary statistics
funding_rate_dt[, .(
  mean = mean(last_funding_rate, na.rm = TRUE),
  median = median(last_funding_rate, na.rm = TRUE),
  sd = sd(last_funding_rate, na.rm = TRUE),
  min = min(last_funding_rate, na.rm = TRUE),
  max = max(last_funding_rate, na.rm = TRUE)
)]

# Visualize some rates
symb_ = funding_rate_dt[grep("BTC", symbol), unique(symbol)]
dt_ = funding_rate_dt[symbol == symb_, .(calc_time, last_funding_rate)]
xts_ = as.xts.data.table(dt_[, .(calc_time, last_funding_rate)])
plot(xts_)

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
horizon

# Predict next 22 days rates from 5 days history
cols = paste0("rate_", 1:5)
funding_daily[, (cols) := shift(rate, 1:5), by = symbol]
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

# merge features and spot data
klines_daily_dt[symbol %like% "^BTC.*PERP", unique(symbol)]
klines_spot_daily_dt[symbol %like% "^BTC", unique(symbol)]
universe_df = klines_daily_dt[symbol %like% ".*PERP", .(
  symbol = gsub("_PERP|USD", "", symbol),
  date = as.Date(close_time),
  close_perp = close,
  volume_perp = volume
)]
universe_df = klines_spot_daily_dt[, .(
    symbol = gsub("USDT", "", symbol),
    date = as.Date(close_time),
    close_spot = close
  )][universe_df, on = c("symbol", "date") ]

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
universe_df[, .(universe_size = sum(is_universe, na.rm = TRUE)), by =date] |>
  ggplot(aes(x = date, y = universe_size)) +
  geom_line() +
  labs(title = glue("Universe size, 5-day average volume > {cutoff}"))

#
funding_daily = copy(funding_rate_dt)
funding_daily[, let(date = as.Date(calc_time), hour=hour(calc_time))]
funding_daily[, rate := shift(last_funding_rate, -1, type = "shift"), by = symbol]
funding_daily = funding_daily[, .(rate = sum(rate, na.rm = TRUE)), by = c("symbol", "date")]
cols = paste0("rate_", 1:5)
funding_daily[, (cols) := shift(rate, 1:5), by = symbol]
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
)]
backtest_df[, let(
  fwd_return_perp = shift(return_perp, -1, "shift"),
  fwd_return_spot = shift(return_spot, -1, "shift"),
  fwd_funding = shift(rate, -2, type = "shift")
)]

#  Next we take positions in the top ten by predicted funding, over some threshold.
threshold_ann = 0.2  # annualised funding
threshold = threshold_ann/(365/7.)  # next 7-day's funding
top_n = 10

positions_df = backtest_df[is_universe == TRUE]
positions_df = positions_df[predicted_funding > threshold]
setorder(positions_df, date, -predicted_funding)
positions_df[, rank := rleid(predicted_funding), by = date]
data.table::first(positions_df, 20)
positions_df[, pos := 0]
positions_df[rank <= top_n, pos := 1]
positions_df[, num_positions := sum(pos), by = date]

# plot positions
positions_df |>
  ggplot(aes(x = date, y = num_positions)) +
  geom_line() +
  labs(title = "Check number of positions per day")

# Next, we'll estimate costs in basis points.
cost <- 0.12/100

backtest_dt = backtest_df[, .(symbol, date, fwd_return_perp, fwd_return_spot, fwd_funding)]
backtest_dt = backtest_dt[positions_df, on = .(symbol, date), nomatch = 0]
backtest_dt[is.na(pos), pos := 0]
backtest_dt[, `:=`(
  diff_pos = c(1, abs(diff(pos))),
  costs = 0  # Initialize costs column
), by = symbol]
backtest_dt[diff_pos == 1, costs := cost]


backtest_dt %>%
  group_by(date) %>%
  summarise(
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

results_df = backtest_dt %>%
  group_by(date) %>%
  summarise(
    num_positions = sum(pos),
    # pnl to being equal weight in the short perp positions (half our capital is in perps)
    perp_pnl = case_when(num_positions > 0 ~ sum(-pos*fwd_return_perp*capital/(2*num_positions), na.rm = TRUE), TRUE ~ 0.),
    # pnl to being equal weight in the long spot positions (half our capital is in spot)
    spot_pnl = case_when(num_positions > 0 ~ sum(pos*fwd_return_spot*capital/(2*num_positions), na.rm = TRUE), TRUE ~ 0.),
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
  pivot_longer(-date, names_to = "pnl_source", values_to = "pnl") %>%
  ggplot(aes(x = date, y = pnl, colour = pnl_source)) +
  geom_line() +
  labs(title = "PnL by source")

options(repr.plot.width = 14, repr.plot.height=10)

results_df %>%
  filter(date >= '2021-07-01') %>%
  mutate(
    basis_cum_pnl = cumsum(basis_change_pnl),
    funding_cum_pnl = cumsum(funding_pnl),
    costs = cumsum(costs),
    total_cum_pnl = cumsum(total_pnl)
  ) %>%
  select(date, basis_cum_pnl, funding_cum_pnl, costs, total_cum_pnl) %>%
  pivot_longer(-date, names_to = "pnl_source", values_to = "pnl") %>%
  ggplot(aes(x = date, y = pnl, colour = pnl_source)) +
  geom_line() +
  facet_wrap(~pnl_source, scales = "free_y", ncol = 1) +
  labs(
    title = glue("PnL to price change, funding, and costs. Starting capital ${capital}"),
    subtitle = "Note different y-axes"
  )

options(repr.plot.width = 14, repr.plot.height=7)


results_df %>%
  summarise(
    Ann.Return = mean(total_pnl)*365/capital,
    Ann.Vol = sd(total_pnl/capital)*sqrt(365),
    Ann.Sharpe = Ann.Return/Ann.Vol
  ) %>%
  mutate(across(.cols = everything(), .fns = ~ round(.x, 3)))
