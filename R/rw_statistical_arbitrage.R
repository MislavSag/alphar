# session  options
options(repr.plot.width = 14, repr.plot.height=7, warn = -1)

# library(tidyverse)
# library(tibbletime)
library(roll)
library(patchwork)
library(ggplot2)
library(data.table)


# chart options
theme_set(theme_bw())
theme_update(text = element_text(size = 20))

perps = fread("https://github.com/Robot-Wealth/r-quant-recipes/raw/master/quantifying-combining-alphas/binance_perp_daily.csv")
head(perps)

# Create universe
universe_dt = perps[, let(
  trail_volume            = roll_mean(dollar_volume, 30),
  total_fwd_return_simple = shift(funding_rate, -1, type = "shift") +
    (shift(close, -1, type = "shift") - close) / close
), by = ticker]
universe_dt = perps[, let(
  total_fwd_return_simple_2 = shift(total_fwd_return_simple, -1, type = "shift"),
  total_fwd_return_log = log(1 + total_fwd_return_simple)
), by = ticker]
universe_dt = na.omit(universe_dt)
universe_dt[, volume_decile := dplyr::ntile(trail_volume, 10), by = date] # TODO Find data.table way to do this
universe_dt[, is_universe := volume_decile >= 3]

# Plot Universe size
universe_dt[, .(count = .N), by = .(date, is_universe)] |>
  ggplot(aes(x = date, y = count, color = is_universe)) +
  geom_line() +
  labs(title = 'Universe size')

# # Checks
# test = as.data.table(universe)[universe_dt, on = c("ticker", "date")]
# test[, all(close == i.close)]
# test[, all(trail_volume == i.trail_volume)]
# test[, all(volume_decile == i.volume_decile)]
# test[, .(volume_decile, i.volume_decile)]
# test[, all(is_universe == i.is_universe)]

# Create simple features
setorder(universe_dt, ticker, date)
features_dt = universe_dt[, let(
  breakout = 9.5 - frollapply(close, 20, function(x) {
    idx_of_high = which.max(x)
    days_since_high = length(x) - idx_of_high
    days_since_high
  }),
  momo = close - shift(close, 10, type = "lag") / close,
  carry = funding_rate
), by = ticker]
features_dt = na.omit(features_dt)

# # Checks
# test = as.data.table(features)[features_dt, on = c("ticker", "date")]
# test = na.omit(test)
# test[, all(close == i.close)]
# test[, all(carry == i.carry)]
# test[, all(momo == i.momo)]
# test[, all(breakout == i.breakout)]

# Plot features
melt(features_dt[is_universe == TRUE],
     id.vars = setdiff(names(features_dt[is_universe == TRUE]), c("breakout", "momo", "carry")),
     variable.name = "feature",
     value.name = "value",
             measure.vars = c("breakout", "momo", "carry")) |>
  ggplot(aes(x = value, colour = feature)) +
  geom_density() +
  facet_wrap(~feature, scales = "free")

# Scale features
features_scaled_dt = features_dt[is_universe == TRUE][
  , let(
    demeaned_fwd_returns = total_fwd_return_simple - mean(total_fwd_return_simple),
    zscore_carry = (carry - mean(carry, na.rm = TRUE)) / sd(carry, na.rm = TRUE),
    decile_carry = dplyr::ntile(carry, 10),
    zscore_momo = (momo - mean(momo, na.rm = TRUE)) / sd(momo, na.rm = TRUE),
    decile_momo = dplyr::ntile(momo, 10)
  ),
  by = date
]
features_scaled_dt = na.omit(features_scaled_dt)

# # Checks
# test = as.data.table(features_scaled)[features_scaled_dt, on = c("ticker", "date")]
# test = na.omit(test)
# test[, all(zscore_carry == i.zscore_carry)]
# test[, all(decile_carry == i.decile_carry)]
# test[, all(zscore_momo == i.zscore_momo)]
# test[, all(decile_momo == i.decile_momo)]

# Factor plot of the decile_carry feature against next day relative returns
features_scaled_dt[, .(mean_return = mean(mean(demeaned_fwd_returns))), by = decile_carry] |>
  ggplot(aes(x = factor(decile_carry), y = mean_return)) +
  geom_bar(stat = "identity") +
  labs(
    x = "Carry Decile",
    y = "Cross-Sectional Return",
    title = "Carry decile feature vs next-day cross-sectional return"
  )

# Factor plot of the breakout feature feature against next day relative returns
features_scaled_dt[, .(mean_return = mean(mean(total_fwd_return_simple))), by = breakout] |>
  ggplot(aes(x = breakout, y = mean_return)) +
  geom_bar(stat = "identity") +
  labs(
    x = "Carry Decile",
    y = "Cross-Sectional Return",
    title = "Carry decile feature vs next-day cross-sectional return"
  )

# Factor plot of the momentum feature feature against next day relative returns
features_scaled_dt[, .(mean_return = mean(mean(demeaned_fwd_returns))), by = decile_momo] |>
  ggplot(aes(x = decile_momo, y = mean_return)) +
  geom_bar(stat = "identity") +
  labs(
    x = "Carry Decile",
    y = "Cross-Sectional Return",
    title = "Carry decile feature vs next-day cross-sectional return"
  )

# Information coefficient
melt(features_scaled_dt,
     id.vars = setdiff(names(features_scaled_dt),
                       c("breakout", "zscore_carry", "zscore_momo", "decile_carry", "decile_momo")),
     variable.name = "feature",
     value.name = "value",
     measure.vars = c("breakout", "zscore_carry", "zscore_momo", "decile_carry", "decile_momo")) |>
  _[, .(IC = cor(value, demeaned_fwd_returns)), by = feature] |>
  ggplot(aes(x = factor(feature, levels = c('breakout', 'zscore_carry', 'decile_carry', 'zscore_momo', 'decile_momo')), y = IC)) +
  geom_bar(stat = "identity") +
  labs(
    x = "Feature",
    y = "IC",
    title = "Relative Return Information Coefficient"
  )

melt(features_scaled_dt,
     id.vars = setdiff(names(features_scaled_dt),
                       c("breakout", "zscore_carry", "zscore_momo", "decile_carry", "decile_momo")),
     variable.name = "feature",
     value.name = "value",
     measure.vars = c("breakout", "zscore_carry", "zscore_momo", "decile_carry", "decile_momo")) |>
  _[, .(IC = cor(value, total_fwd_return_simple)), by = feature] |>
  ggplot(aes(x = factor(feature, levels = c('breakout', 'zscore_carry', 'decile_carry', 'zscore_momo', 'decile_momo')), y = IC)) +
  geom_bar(stat = "identity") +
  labs(
    x = "Feature",
    y = "IC",
    title = "Relative Return Information Coefficient"
  )

# Decay
setorder(features_scaled_dt, ticker, date)
features_scaled_decay_dt = features_scaled_dt[is_universe == TRUE][, let(
  demeaned_fwd_returns_2 = shift(demeaned_fwd_returns, -1, type = "shift"),
  demeaned_fwd_returns_3 = shift(demeaned_fwd_returns, -2, type = "shift"),
  demeaned_fwd_returns_4 = shift(demeaned_fwd_returns, -3, type = "shift"),
  demeaned_fwd_returns_5 = shift(demeaned_fwd_returns, -4, type = "shift"),
  demeaned_fwd_returns_6 = shift(demeaned_fwd_returns, -5, type = "shift")
), by = ticker]
features_scaled_decay_dt = na.omit(features_scaled_decay_dt)
features_scaled_decay_dt = melt(features_scaled_decay_dt,
     id.vars = setdiff(names(features_scaled_decay_dt),
                       c("zscore_carry", "zscore_momo", "decile_carry", "decile_momo")),
     variable.name = "feature",
     value.name = "value",
     measure.vars = c("zscore_carry", "zscore_momo", "decile_carry", "decile_momo"))
features_scaled_decay_dt[, .(
  IC_1 = cor(value, demeaned_fwd_returns),
  IC_2 = cor(value, demeaned_fwd_returns_2),
  IC_3 = cor(value, demeaned_fwd_returns_3),
  IC_4 = cor(value, demeaned_fwd_returns_4),
  IC_5 = cor(value, demeaned_fwd_returns_5),
  IC_6 = cor(value, demeaned_fwd_returns_6)
), by = feature] |>
  melt(dt,
       id.vars = "feature",
       measure.vars = setdiff(names(dt), "feature"),
       variable.name = "IC_period",
       value.name = "IC") |>
  ggplot(aes(x = factor(IC_period), y = IC, colour = feature, group = feature)) +
  geom_line() +
  geom_point() +
  labs(
    title = "IC by forward period against relative returns",
    x = "IC period"
  )

# Time series decay
setorder(features_scaled_dt, ticker, date)
features_scaled_decay_dt = features_scaled_dt[is_universe == TRUE][, let(
  fwd_returns_2 = shift(total_fwd_return_simple, -1, type = "shift"),
  fwd_returns_3 = shift(total_fwd_return_simple, -2, type = "shift"),
  fwd_returns_4 = shift(total_fwd_return_simple, -3, type = "shift"),
  fwd_returns_5 = shift(total_fwd_return_simple, -4, type = "shift"),
  fwd_returns_6 = shift(total_fwd_return_simple, -5, type = "shift")
), by = ticker]
features_scaled_decay_dt = na.omit(features_scaled_decay_dt)
features_scaled_decay_dt = melt(features_scaled_decay_dt,
                                id.vars = setdiff(names(features_scaled_decay_dt),
                                                  c("breakout")),
                                variable.name = "feature",
                                value.name = "value",
                                measure.vars = c("breakout"))
features_scaled_decay_dt[, .(
  IC_1 = cor(value, total_fwd_return_simple),
  IC_2 = cor(value, fwd_returns_2),
  IC_3 = cor(value, fwd_returns_3),
  IC_4 = cor(value, fwd_returns_4),
  IC_5 = cor(value, fwd_returns_5),
  IC_6 = cor(value, fwd_returns_6)
), by = feature] |>
  melt(dt,
       id.vars = "feature",
       measure.vars = setdiff(names(dt), "feature"),
       variable.name = "IC_period",
       value.name = "IC") |>
  ggplot(aes(x = factor(IC_period), y = IC, colour = feature, group = feature)) +
  geom_line() +
  geom_point() +
  labs(
    title = "IC by forward period against relative returns",
    x = "IC period"
  )

# Start simulation from date we first have n tickers in the universe
min_trading_universe_size = 10
start_date = features_dt[, .(count = .N), by = .(date, is_universe)][order(date)]
start_date = start_date[count >= min_trading_universe_size]
start_date = start_date[, first(date)]
model_dt = features_dt[is_universe == TRUE]
model_dt = model_dt[date >= start_date]
model_dt[, let(
  carry_decile = ntile(carry, 10),
  momo_decile = ntile(momo, 10)
), by = date]
model_dt[, let(
  carry_weight = (carry_decile - 5.5),
  momo_weight = -(momo_decile - 5.5),
  breakout_weight = breakout / 2
), by = date]
model_dt[, let(
  combined_weight = (0.5*carry_weight + 0.2*momo_weight + 0.3*breakout_weight)
), by = date]
model_dt[, let(
  scaled_weight = if_else(combined_weight == 0, 0, combined_weight/sum(abs(combined_weight)))
), by = date]
returns_plot = model_dt[, .(
  returns = scaled_weight * total_fwd_return_simple
), by = date]
returns_plot = returns_plot[, .(logreturns = log(returns + 1)), by = date]
setorder(returns_plot, date)
returns_plot = ggplot(returns_plot, aes(x=date, y=cumsum(logreturns))) +
  geom_line() +
  labs(
    title = 'Combined Carry, Momentum, and Trend Model on top 80% Perp Universe',
    subtitle = "Unleveraged returns",
    x = "",
    y = "Cumulative return"
  )
weights_plot = model_dt[, .(total_weight = sum(scaled_weight)), by = date]
weights_plot = weights_plot |>
  ggplot(aes(x = date, y = total_weight)) +
  geom_line() +
  labs(x = "Date", y = "Portfolio net weight")
returns_plot / weights_plot + plot_layout(heights = c(2,1))



# CHAPTER 2 ---------------------------------------------------------------
# importlibriries
library(rsims)

# Import data
perps = fread("https://github.com/Robot-Wealth/r-quant-recipes/raw/master/quantifying-combining-alphas/binance_perp_daily.csv")

#
url <- "https://stablecoins.llama.fi/stablecoins?includePrices=true"
response <- httr::GET(url)

stables <- response %>%
  httr::content(as = "text", encoding = "UTF-8") %>%
  jsonlite::fromJSON(flatten = TRUE) %>%
  pluck("peggedAssets") %>%
  pull(symbol)

perps <- perps %>%
  filter(!ticker %in% glue::glue("{stables}USDT"))

# just get the top 30 by trailing 30-day volume
trading_universe_size <- 30

universe <- perps %>%
  group_by(ticker) %>%
  mutate(trail_volume = roll_mean(dollar_volume, 30)) %>%
  na.omit() %>%
  group_by(date) %>%
  mutate(
    volume_rank = row_number(-trail_volume),
    is_universe = volume_rank <= trading_universe_size
  )

universe %>%
  group_by(date, is_universe) %>%
  summarize(count = n(), .groups = "drop") %>%
  ggplot(aes(x=date, y=count, color = is_universe)) +
  geom_line() +
  labs(
    title = 'Universe size'
  )

# calculate features
rolling_days_since_high_20 <- purrr::possibly(
  tibbletime::rollify(
    function(x) {
      idx_of_high <- which.max(x)
      days_since_high <- length(x) - idx_of_high
      days_since_high
    },
    window = 20, na_value = NA),
  otherwise = NA
)

features <- universe %>%
  group_by(ticker) %>%
  arrange(date) %>%
  mutate(
    breakout = lag(9.5 - rolling_days_since_high_20(close)),  # puts this feature on a scale -9.5 to +9.5
    momo = lag(close - lag(close, 10)/close),
    carry = lag(funding_rate)
  ) %>%
  ungroup() %>%
  na.omit()

head(features)

# calculate target weights
# filter on is_universe so that we calculate features only for stuff that's in the universe today
# (we'd have to do this differently if any of these calcs depended on past data, eg if we were doing z-score smoothing)
# then, join on original prices for backtesting

# tickers that were ever in the universe
universe_tickers <- features %>%
  filter(is_universe) %>%
  pull(ticker) %>%
  unique()

# print(length(universe_tickers))

# start simulation from date we first have n tickers in the universe
start_date <- features %>%
  group_by(date, is_universe) %>%
  summarize(count = n(), .groups = "drop") %>%
  filter(count >= trading_universe_size) %>%
  head(1) %>%
  pull(date)

# calculate weights
model_df <- features %>%
  filter(is_universe) %>%
  group_by(date) %>%
  mutate(
    carry_decile = ntile(carry, 10),
    carry_weight = (carry_decile - 5.5),  # will run -4.5 to 4.5
    momo_decile = ntile(momo, 10),
    momo_weight = -(momo_decile - 5.5),  # will run -4.5 to 4.5
    breakout_weight = breakout / 2,
    combined_weight = (0.5*carry_weight + 0.2*momo_weight + 0.3*breakout_weight),
    # scale weights so that abs values sum to 1 - no leverage condition
    scaled_weight = combined_weight/sum(abs(combined_weight))
  )  %>%
  select(date, ticker, scaled_weight) %>%
  # join back onto df of prices for all tickers that were ever in the universe
  # so that we have prices before and after a ticker comes into or out of the universe
  # for backtesting purposes
  right_join(
    features %>%
      filter(ticker %in% universe_tickers) %>%
      select(date, ticker, close, funding_rate),
    by = c("date", "ticker")
  ) %>%
  # give anything with a NA weight (due to the join) a zero
  replace_na(list(scaled_weight = 0)) %>%
  arrange(date, ticker) %>%
  filter(date >= start_date)


# get weights as a wide matrix
# note that date column will get converted to unix timestamp
backtest_weights <- model_df %>%
  pivot_wider(id_cols = date, names_from = ticker, values_from = c(close, scaled_weight)) %>%  # pivot wider guarantees prices and theo_weight are date aligned
  select(date, starts_with("scaled_weight")) %>%
  data.matrix()
backtest_weights_2 <- model_df %>%
  pivot_wider(id_cols = date, names_from = ticker, values_from = scaled_weight) %>%  # pivot wider guarantees prices and theo_weight are date aligned
  data.matrix()
all(backtest_weights == backtest_weights_2, na.rm = TRUE)
all.equal(backtest_weights, backtest_weights_2)

# NA weights should be zero
backtest_weights[is.na(backtest_weights)] <- 0

head(backtest_weights, c(5, 5))

# get prices as a wide matrix
# note that date column will get converted to unix timestamp
backtest_prices <- model_df %>%
  pivot_wider(id_cols = date, names_from = ticker, values_from = c(close, scaled_weight)) %>%  # pivot wider guarantees prices and theo_weight are date aligned
  select(date, starts_with("close_")) %>%
  data.matrix()

head(backtest_prices, c(5, 5))

# get funding as a wide matrix
# note that date column will get converted to unix timestamp
backtest_funding <- model_df %>%
  pivot_wider(id_cols = date, names_from = ticker, values_from = c(close, funding_rate)) %>%  # pivot wider guarantees prices and funding_returns_simple are date aligned
  select(date, starts_with("funding_rate_")) %>%
  data.matrix()

head(backtest_funding, c(5, 5))

# fees - reasonable approximation of actual binance costs (spread + market impact + commission)
fees <- tribble(
  ~tier, ~fee,
  0, 0.,  # use for cost-free simulations
  1, 0.0015,
  2, 0.001,
  3, 0.0008,
  4, 0.0007,
  5, 0.0006,
  6, 0.0004,
  7, 0.0002
)

# make a nice plot with some summary statistics
# plot equity curve from output of simulation
plot_results <- function(backtest_results,
                         weighting_protocol = "0.5/0.2/0.3 Carry/Momo/Breakout",
                         trade_on = "close") {
  margin <- backtest_results %>%
    group_by(Date) %>%
    summarise(Margin = sum(Margin, na.rm = TRUE))

  cash_balance <- backtest_results %>%
    filter(ticker == "Cash") %>%
    select(Date, Value) %>%
    rename("Cash" = Value)

  equity <- cash_balance %>%
    left_join(margin, by = "Date") %>%
    mutate(Equity = Cash + Margin)

  fin_eq <- equity %>%
    tail(1) %>%
    pull(Equity)

  init_eq <- equity %>%
    head(1) %>%
    pull(Equity)

  total_return <- (fin_eq/init_eq - 1) * 100
  days <- nrow(equity)
  ann_return <- total_return * 365/days
  sharpe <- equity %>%
    mutate(returns = Equity/lag(Equity)- 1) %>%
    na.omit() %>%
    summarise(sharpe = sqrt(365)*mean(returns)/sd(returns)) %>%
    pull()

  equity %>%
    ggplot(aes(x = Date, y = Equity)) +
    geom_line() +
    labs(
      title = "Crypto Stat Arb Simulation",
      subtitle = glue::glue(
        "{weighting_protocol}, costs {commission_pct*100}% of trade value, trade buffer = {trade_buffer}, trade on {trade_on}
          {round(total_return, 1)}% total return, {round(ann_return, 1)}% annualised, Sharpe {round(sharpe, 2)}"
      )
    )
}

# calculate sharpe ratio from output of simulation
calc_sharpe <- function(backtest_results) {
  margin <- backtest_results %>%
    group_by(Date) %>%
    summarise(Margin = sum(Margin, na.rm = TRUE))

  cash_balance <- backtest_results %>%
    filter(ticker == "Cash") %>%
    select(Date, Value) %>%
    rename("Cash" = Value)

  equity <- cash_balance %>%
    left_join(margin, by = "Date") %>%
    mutate(Equity = Cash + Margin)

  equity %>%
    mutate(returns = Equity/lag(Equity)- 1) %>%
    na.omit() %>%
    summarise(sharpe = sqrt(355)*mean(returns)/sd(returns)) %>%
    pull()
}



# Navigating Cost Tradeoffs using Heuristics ------------------------------
# session  options
options(repr.plot.width = 14, repr.plot.height=7, warn = -1)

pacman::p_load_current_gh("Robot-Wealth/rsims", dependencies = TRUE)  # this will take some time the first time you build the package
library(rsims)
library(tidyverse)
library(tibbletime)
library(roll)
library(patchwork)

# chart options
theme_set(theme_bw())
theme_update(text = element_text(size = 20))

perps <- read_csv("https://github.com/Robot-Wealth/r-quant-recipes/raw/master/quantifying-combining-alphas/binance_perp_daily.csv")
head(perps)

# remove stablecoins
# list of stablecoins from defi llama
url <- "https://stablecoins.llama.fi/stablecoins?includePrices=true"
response <- httr::GET(url)

stables <- response %>%
  httr::content(as = "text", encoding = "UTF-8") %>%
  jsonlite::fromJSON(flatten = TRUE) %>%
  pluck("peggedAssets") %>%
  pull(symbol)

# sort(stables)

perps <- perps %>%
  filter(!ticker %in% glue::glue("{stables}USDT"))

# just get the top 30 by trailing 30-day volume
trading_universe_size <- 30

universe <- perps %>%
  group_by(ticker) %>%
  mutate(trail_volume = roll_mean(dollar_volume, 30)) %>%
  na.omit() %>%
  group_by(date) %>%
  mutate(
    volume_rank = row_number(-trail_volume),
    is_universe = volume_rank <= trading_universe_size
  )

universe %>%
  group_by(date, is_universe) %>%
  summarize(count = n(), .groups = "drop") %>%
  ggplot(aes(x=date, y=count, color = is_universe)) +
  geom_line() +
  labs(
    title = 'Universe size'
  )

# calculate features
rolling_days_since_high_20 <- purrr::possibly(
  tibbletime::rollify(
    function(x) {
      idx_of_high <- which.max(x)
      days_since_high <- length(x) - idx_of_high
      days_since_high
    },
    window = 20, na_value = NA),
  otherwise = NA
)

features <- universe %>%
  group_by(ticker) %>%
  arrange(date) %>%
  mutate(
    breakout = lag(9.5 - rolling_days_since_high_20(close)),  # puts this feature on a scale -9.5 to +9.5
    momo = lag(close - lag(close, 10)/close),
    carry = lag(funding_rate)
  ) %>%
  ungroup() %>%
  na.omit()

head(features)


# calculate target weights
# filter on is_universe so that we calculate features only for stuff that's in the universe today
# (we'd have to do this differently if any of these calcs depended on past data, eg if we were doing z-score smoothing)
# then, join on original prices for backtesting

# tickers that were ever in the universe
universe_tickers <- features %>%
  filter(is_universe) %>%
  pull(ticker) %>%
  unique()

# print(length(universe_tickers))

# start simulation from date we first have n tickers in the universe
start_date <- features %>%
  group_by(date, is_universe) %>%
  summarize(count = n(), .groups = "drop") %>%
  filter(count >= trading_universe_size) %>%
  head(1) %>%
  pull(date)

# calculate weights
model_df <- features %>%
  filter(is_universe) %>%
  group_by(date) %>%
  mutate(
    carry_decile = ntile(carry, 10),
    carry_weight = (carry_decile - 5.5),  # will run -4.5 to 4.5
    momo_decile = ntile(momo, 10),
    momo_weight = -(momo_decile - 5.5),  # will run -4.5 to 4.5
    breakout_weight = breakout / 2,
    combined_weight = (0.5*carry_weight + 0.2*momo_weight + 0.3*breakout_weight),
    # scale weights so that abs values sum to 1 - no leverage condition
    scaled_weight = combined_weight/sum(abs(combined_weight))
  )  %>%
  select(date, ticker, scaled_weight) %>%
  # join back onto df of prices for all tickers that were ever in the universe
  # so that we have prices before and after a ticker comes into or out of the universe
  # for backtesting purposes
  right_join(
    features %>%
      filter(ticker %in% universe_tickers) %>%
      select(date, ticker, close, funding_rate),
    by = c("date", "ticker")
  ) %>%
  # give anything with a NA weight (due to the join) a zero
  replace_na(list(scaled_weight = 0)) %>%
  arrange(date, ticker) %>%
  filter(date >= start_date)

# get weights as a wide matrix
# note that date column will get converted to unix timestamp
backtest_weights <- model_df %>%
  pivot_wider(id_cols = date, names_from = ticker, values_from = c(close, scaled_weight)) %>%  # pivot wider guarantees prices and theo_weight are date aligned
  select(date, starts_with("scaled_weight")) %>%
  data.matrix()

# NA weights should be zero
backtest_weights[is.na(backtest_weights)] <- 0

head(backtest_weights, c(5, 5))

# get prices as a wide matrix
# note that date column will get converted to unix timestamp
backtest_prices <- model_df %>%
  pivot_wider(id_cols = date, names_from = ticker, values_from = c(close, scaled_weight)) %>%  # pivot wider guarantees prices and theo_weight are date aligned
  select(date, starts_with("close_")) %>%
  data.matrix()

head(backtest_prices, c(5, 5))

# get funding as a wide matrix
# note that date column will get converted to unix timestamp
backtest_funding <- model_df %>%
  pivot_wider(id_cols = date, names_from = ticker, values_from = c(close, funding_rate)) %>%  # pivot wider guarantees prices and funding_returns_simple are date aligned
  select(date, starts_with("funding_rate_")) %>%
  data.matrix()

head(backtest_funding, c(5, 5))

# fees - reasonable approximation of actual binance costs (spread + market impact + commission)
fees <- tribble(
  ~tier, ~fee,
  0, 0.,  # use for cost-free simulations
  1, 0.0015,
  2, 0.001,
  3, 0.0008,
  4, 0.0007,
  5, 0.0006,
  6, 0.0004,
  7, 0.0002
)

# make a nice plot with some summary statistics
# plot equity curve from output of simulation
plot_results <- function(backtest_results, weighting_protocol = "0.5/0.2/0.3 Carry/Momo/Breakout", trade_on = "close") {
  margin <- backtest_results %>%
    group_by(Date) %>%
    summarise(Margin = sum(Margin, na.rm = TRUE))

  cash_balance <- backtest_results %>%
    filter(ticker == "Cash") %>%
    select(Date, Value) %>%
    rename("Cash" = Value)

  equity <- cash_balance %>%
    left_join(margin, by = "Date") %>%
    mutate(Equity = Cash + Margin)

  fin_eq <- equity %>%
    tail(1) %>%
    pull(Equity)

  init_eq <- equity %>%
    head(1) %>%
    pull(Equity)

  total_return <- (fin_eq/init_eq - 1) * 100
  days <- nrow(equity)
  ann_return <- total_return * 365/days
  sharpe <- equity %>%
    mutate(returns = Equity/lag(Equity)- 1) %>%
    na.omit() %>%
    summarise(sharpe = sqrt(365)*mean(returns)/sd(returns)) %>%
    pull()

  equity %>%
    ggplot(aes(x = Date, y = Equity)) +
    geom_line() +
    labs(
      title = "Crypto Stat Arb Simulation",
      subtitle = glue::glue(
        "{weighting_protocol}, costs {commission_pct*100}% of trade value, trade buffer = {trade_buffer}, trade on {trade_on}
          {round(total_return, 1)}% total return, {round(ann_return, 1)}% annualised, Sharpe {round(sharpe, 2)}"
      )
    )
}

# calculate sharpe ratio from output of simulation
calc_sharpe <- function(backtest_results) {
  margin <- backtest_results %>%
    group_by(Date) %>%
    summarise(Margin = sum(Margin, na.rm = TRUE))

  cash_balance <- backtest_results %>%
    filter(ticker == "Cash") %>%
    select(Date, Value) %>%
    rename("Cash" = Value)

  equity <- cash_balance %>%
    left_join(margin, by = "Date") %>%
    mutate(Equity = Cash + Margin)

  equity %>%
    mutate(returns = Equity/lag(Equity)- 1) %>%
    na.omit() %>%
    summarise(sharpe = sqrt(355)*mean(returns)/sd(returns)) %>%
    pull()
}


# cost-free, no trade buffer
# simulation parameters
initial_cash <- 10000
fee_tier <- 0
capitalise_profits <- FALSE  # remain fully invested?
trade_buffer <- 0.
commission_pct <- fees$fee[fees$tier==fee_tier]
margin <- 0.05

# simulation
results_df <- fixed_commission_backtest_with_funding(
  prices = backtest_prices,
  target_weights = backtest_weights,
  funding_rates = backtest_funding,
  trade_buffer = trade_buffer,
  initial_cash = initial_cash,
  margin = margin,
  commission_pct = commission_pct,
  capitalise_profits = capitalise_profits
) %>%
  mutate(ticker = str_remove(ticker, "close_")) %>%
  # remove coins we don't trade from results
  drop_na(Value)

plot_results(results_df)

# check that actual weights match intended (can trade fractional contracts, so should be equal)
results_df %>%
  left_join(model_df %>% select(ticker, date, scaled_weight), by = c("ticker", "Date" = "date")) %>%
  group_by(Date) %>%
  mutate(
    actual_weight = Value/(initial_cash)
  )  %>%
  filter(scaled_weight != 0) %>%
  tail(10)


# explore costs-turnover tradeoffs
# with costs, no trade buffer
fee_tier <- 1.
commission_pct <- fees$fee[fees$tier==fee_tier]

# simulation
results_df <- fixed_commission_backtest_with_funding(
  prices = backtest_prices,
  target_weights = backtest_weights,
  funding_rates = backtest_funding,
  trade_buffer = trade_buffer,
  initial_cash = initial_cash,
  margin = margin,
  commission_pct = commission_pct,
  capitalise_profits = capitalise_profits
) %>%
  mutate(ticker = str_remove(ticker, "close_")) %>%
  # remove coins we don't trade from results
  drop_na(Value)

results_df %>%
  plot_results()

results_df %>%
  filter(ticker != "Cash") %>%
  group_by(Date) %>%
  summarise(Turnover = 100*sum(abs(TradeValue))/initial_cash) %>%
  ggplot(aes(x = Date, y = Turnover)) +
  geom_line() +
  geom_point() +
  labs(
    title = "Turnover as % of trading capital",
    y = "Turnover, %"
  )
