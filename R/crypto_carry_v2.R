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
cm_funding_rates[, date := as.IDate(calc_time)]

# Inspect funding rates
cm_funding_rates[, unique(funding_interval_hours)]
cm_funding_rates[, .N, by = funding_interval_hours]
cm_funding_rates = cm_funding_rates[funding_interval_hours == 8]
cm_funding_rates[, unique(hour(calc_time))]
cm_funding_rates[, .N, by = c("symbol", "date")][, table(N)]

# Upasample funding rates to daily
cm_funding_rates_daily = cm_funding_rates[
  , .(rate = sum(last_funding_rate, na.rm = TRUE)), by = c("symbol", "date")
]

# check for duplicates
any_duplicates = function(dt) dt[, .N, by = .(symbol, open_time)][N > 1]
lapply(list(cm_indexpriceklines_hour,
            cm_indexpriceklines_daily,
            futures_cm_klines_hour,
            futures_cm_klines_daily,
            spot_klines_daily,
            spot_klines_hour), any_duplicates)
cm_funding_rates[, .N, by = .(symbol, calc_time)][N > 1]

# Remove duplicates for objects that have duplicates
spot_klines_hour = unique(spot_klines_hour, by = c("symbol", "open_time"))

# Summary statistics
cm_funding_rates[, .(
  mean = mean(last_funding_rate, na.rm = TRUE),
  median = median(last_funding_rate, na.rm = TRUE),
  sd = sd(last_funding_rate, na.rm = TRUE),
  min = min(last_funding_rate, na.rm = TRUE),
  max = max(last_funding_rate, na.rm = TRUE)
)]
cm_funding_rates_daily[, .(
  mean = mean(rate, na.rm = TRUE),
  median = median(rate, na.rm = TRUE),
  sd = sd(rate, na.rm = TRUE),
  min = min(rate, na.rm = TRUE),
  max = max(rate, na.rm = TRUE)
)]

# Visualize some rates
symb_ = cm_funding_rates[grep("BTC", symbol), unique(symbol)]
dt_ = cm_funding_rates[symbol == symb_, .(calc_time, last_funding_rate)]
plot(as.xts.data.table(dt_), main=paste0(symb_, " funding rate"))

# Average daily funding
avg_daily_fun = cm_funding_rates_daily[, .(rate = mean(rate, na.rm = TRUE)), by = "symbol"]
all_coins_avg = avg_daily_fun[, mean(rate, na.rm = TRUE)]
all_coins_avg * 100 * 365
avg_daily_fun |>
  ggplot(aes(x = symbol, y = rate*100*365)) +
  geom_bar(stat = "identity") +
  coord_flip() +
  labs(
    title = "Average daily funding, annualised",
    y = "Annualised funding rate, %"
  )

# Mean funding rate through time
mean_rate_daily = cm_funding_rates_daily[, .(mean_rate = mean(rate, na.rm = TRUE)), by = date]
setorder(mean_rate_daily, date)
ggplot(mean_rate_daily, aes(x = date, y = mean_rate * 100 * 365)) +
  geom_line() +
  labs(
    title = "Mean funding rate through time",
    x = "Date",
    y = "Mean funding rate"
  )

# costs
cost = 4*0.07/100
horizon = cost/all_coins_avg
