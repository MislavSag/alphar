library(BatchGetSymbols)
library(quantmod)
library(PerformanceAnalytics)
library(data.table)
library(lubridate)
library(future.apply)
source('C:/Users/Mislav/Documents/GitHub/alphar/R/import_data.R')

# performance
plan(multiprocess(workers = availableCores() - 8))  # multiprocess(workers = availableCores() - 8)


# intraday market data
market_data <- import_mysql(
  symbols = c('SPY'),
  trading_hours = TRUE,
  upsample = 1,
  use_cache = TRUE,
  save_path = 'D:/market_data/usa/ohlcv',
  RMySQL::MySQL(),
  dbname = 'odvjet12_market_data_usa',
  username = 'odvjet12_mislav',
  password = 'Theanswer0207',
  host = '91.234.46.219',
)[[1]]
head(market_data)
#plot(Cl(market_data))
market_data$returns <- Return.calculate(Cl(market_data))
market_data_sample <- market_data['2005/2007']
cl <-  as.vector(zoo::coredata(Cl(market_data_sample)))
returns <- as.vector(zoo::coredata(market_data_sample$returns))
close <- Cl(market_data_sample)


# utils
f <- function(y, m, alpha) {
  weights <- (1 - alpha)^((m - 1):0)
  ewma <- sum(weights * y) / sum(weights)
  bias <- sum(weights)^2 / (sum(weights)^2 - sum(weights^2))
  ewmsd <- sqrt(bias * sum(weights * (y - ewma)^2) / sum(weights))
  ewmsd
}

# daily volatility
daily_volatility <- function(close, span, win) {

  # convert to data.table because I will use DT merge with rolling
  if (is.xts(close) | is.data.frame(close)) {
    DT <- as.data.table(close)
  }

  # 1 day delta
  t_day_lag <- data.table(index = DT$index - lubridate::days(1))

  # merge t_day_lag with input index to get first and last period of daily vol
  DT_index = DT[, .(index) ]
  x <- DT_index[, nearest:=(index)][t_day_lag, on = 'index', roll = Inf, ]
  x <- cbind(DT$index, x)
  x <- x[!is.na(nearest)]

  # calculate returns of daily observations
  y = (zoo::coredata(close[x[, V1]]) / zoo::coredata(close[x[, nearest]])) - 1

  # calculate alpha based on span argument
  alpha <- 2 / (span + 1)

  # calculate ewm sd
  y <- frollapply(y, win, function(x) f(x, win, alpha))
  y <- cbind(x[, 1], y)
  colnames(y) <- c('index', 'daily_vol')
  setkey(y, 'index')
  return(y)
}


cusum_filter <- function(price, threshold) {

  # calculate returns
  returns <- diff(log(price))

  # threshold to vector if on element
  if (length(threshold) == 1) {
    threshold <- rep(threshold, length(returns))
  } else {
    threshold <- threshold[2:length(returns)]
  }

  # init vars for loop
  s_pos <- 0
  s_neg <- 0
  t_events = c()

  # clacluate cusum events0
  for (i in seq_along(returns)) {
    s_pos <- max(0, s_pos + returns[i])
    s_neg <- min(0, s_neg + returns[i])
    if (s_neg < -threshold[i]) {
      s_neg <- 0
      t_events <- c(t_events, i)
    }
    if (s_pos > threshold[i]) {
      s_pos <- 0
      t_events <- c(t_events, i)
    }
  }
  # add one because we removed one observation in the begging when calculating returns
  t_events + 1
}

t_events = cusum_events
close_date = zoo::index(close)

# add vertical bar
add_vertical_barrier <- function(t_events,
                                 close_date,
                                 num_days=1,
                                 num_hours=0,
                                 num_minutes=0,
                                 num_seconds=0
) {
  timedelta <- days(num_days) + hours(num_hours) + minutes(num_minutes) + seconds(num_seconds)
  t_events_timdelta <- t_events[[1]] + timedelta
  t_events_timdelta <- as.data.table(close_date)[,nearest := close_date][
    as.data.table(t_events_timdelta), on = 'x', roll = -Inf
  ]
  vertical_barrier <- t_events_timdelta$nearest
  # vertical_barrier <- vertical_barrier[vertical_barrier %in% t_events$index]
  vertical_barrier <- data.table(index = t_events$index, t1 = vertical_barrier, key = 'index')
  # vertical_barrier[93:119] for test
  return(vertical_barrier)
}


# TEST --------------------------------------------------------------------

# Compute daily volatility
daily_vol <- daily_volatility(close, 50, 1000)

# Apply Symmetric CUSUM Filter and get timestamps for events
cusum_events <- cusum_filter(cl, mean(daily_vol$daily_vol, na.rm =TRUE))
cusum_events <- zoo::index(Cl(market_data_sample)[cusum_events])
cusum_events <- data.table(index = cusum_events, key = 'index')

# Compute vertical barrier
vertical_barriers <- add_vertical_barrier(cusum_events,
                                          zoo::index(close),
                                          num_days=1)


# events <- vertical_barriers[daily_vol[cusum_events]]
# colnames(events) <- c('t0', 't1', 'trgt')


close = as.data.table(Cl(market_data_sample))
t_events = cusum_events
pt_sl = c(1, 2)
target = daily_vol
min_return = 0.0002
num_threads = 1
vertical_barrier_times = vertical_barriers
side_prediction = NA
verbose = TRUE


# 1) Get target
target = target[index %in% t_events$index]
target = target[daily_vol > min_return]  # min_ret

# 2) Get vertical barrier (max holding period)
if (isFALSE(vertical_barrier_times)) {
  vertical_barrier_times <- xts::xts(rep(NA, length(t_events)), order.by = t_events)
}

# 3) Form events object, apply stop loss on vertical barrier
if (is.na(side_prediction)) {
  side_ <- data.table(index = target$index, side = rep(1, nrow(target)))
} else {
  side_ <- side_prediction
}

# Create a new df with [v_barrier, target, side] and drop rows that are NA in target
events <- target[vertical_barrier_times, on = 'index'][side_, on = 'index']
events <- events[, .(index, t1, daily_vol, side)]
colnames(events) <- c('t0', 't1', 'trgt', 'side')
events <- events[!is.na(trgt)]
events <- na.omit(events)

events_ <- events
out <- copy(events[, .(t1)])

profit_taking_multiple = pt_sl[1]
stop_loss_multiple = pt_sl[2]

if (profit_taking_multiple > 0) {
  profit_taking <- data.table(index = events$t0, profit_taking_multiple * events$trgt)
} else {
  profit_taking <- data.table(index = events$t0, NA)
}
if (stop_loss_multiple > 0) {
  stop_loss <- data.table(index = events$t0, -stop_loss_multiple * events$trgt)
} else {
  stop_loss <- data.table(index = events$t0, NA)
}

# future.apply::future_vapply()

t0_close <- future.apply::future_vapply(events_$t0, function(x) which(close$index == x), FUN.VALUE = integer(1))
t1_close <- future.apply::future_vapply(events_$t1, function(x) which(close$index == x), FUN.VALUE = integer(1))
side_ <- events_$side
close_ <- close$close
pt_ <- profit_taking$V2
sl_ <- stop_loss$V2
pt <- c()
sl <- c()
for (i in seq_along(t0_close)) {
#  i <- 1
  closing_prices <- close_[t0_close[i]:t1_close[i]]
  cum_returns <- (closing_prices / closing_prices[1] - 1) * side_[i]
  sl <- c(sl, cum_returns[min(which(cum_returns < sl_[i]))]) # Earliest stop loss date
  pt <- c(pt, cum_returns[min(which(cum_returns > pt_[i]))]) # Earliest profit taking date
}
out.at[loc, 'sl'] = cum_returns[cum_returns < stop_loss[loc]].index.min()  # Earliest stop loss date
out.at[loc, 'pt'] = cum_returns[cum_returns > profit_taking[loc]].index.min()  # Earliest profit taking date






out['pt'] = pd.Series(dtype=events.index.dtype)
out['sl'] = pd.Series(dtype=events.index.dtype)

# Get events
for loc, vertical_barrier in events_['t1'].fillna(close.index[-1]).iteritems():
  closing_prices = close[loc: vertical_barrier]  # Path prices for a given trade
  cum_returns = (closing_prices / close[loc] - 1) * events_.at[loc, 'side']  # Path returns
  out.at[loc, 'sl'] = cum_returns[cum_returns < stop_loss[loc]].index.min()  # Earliest stop loss date
  out.at[loc, 'pt'] = cum_returns[cum_returns > profit_taking[loc]].index.min()  # Earliest profit taking date

return out


# Apply Triple Barrier
first_touch_dates = mp_pandas_obj(func=apply_pt_sl_on_t1,
                                  pd_obj=('molecule', events.index),
                                  num_threads=num_threads,
                                  close=close,
                                  events=events,
                                  pt_sl=pt_sl_,
                                  verbose=verbose)

for ind in events.index:
  events.at[ind, 't1'] = first_touch_dates.loc[ind, :].dropna().min()

if side_prediction is None:
  events = events.drop('side', axis=1)

# Add profit taking and stop loss multiples for vertical barrier calculations
events['pt'] = pt_sl[0]
events['sl'] = pt_sl[1]

