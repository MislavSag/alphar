library(BatchGetSymbols)
library(quantmod)
library(PerformanceAnalytics)
source('C:/Users/Mislav/Documents/GitHub/alphar/R/import_data.R')


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
plot(Cl(market_data))
market_data$returns <- Return.calculate(Cl(market_data))
market_data_sample <- market_data['2005/2007']
cl <-  as.vector(zoo::coredata(Cl(market_data_sample)))
returns <- as.vector(zoo::coredata(market_data_sample$returns))


close <- Cl(market_data_sample)
lookback <- 100

if (is.xts(close) | is.data.frame(close)) {
  DT <- as.data.table(close)
}

t_day_lag <- data.table(index = DT$index - lubridate::days(1))
DT_index = DT[, .(index) ]
x <- DT_index[, nearest:=(index)][t_day_lag, on = 'index', roll = Inf, ]
y <- cbind(DT$index, x)
y <- y[!is.na(nearest)]

df0 = (zoo::coredata(close[y[, V1]]) / zoo::coredata(close[y[, nearest]])) - 1

ewmsd <- function(x, alpha) {
  n <- length(x)
  sapply(
    1:n,
    function(i, x, alpha) {
      y <- x[1:i]
      m <- length(y)
      weights <- (1 - alpha)^((m - 1):0)
      ewma <- sum(weights * y) / sum(weights)
      bias <- sum(weights)^2 / (sum(weights)^2 - sum(weights^2))
      ewmsd <- sqrt(bias * sum(weights * (y - ewma)^2) / sum(weights))
    },
    x = x,
    alpha = alpha
  )
}

test <- ewmsd(as.vector(df0), alpha)

test <- frollapply(as.vector(df0), n = 100, FUN = function(x) ewmsd(x, alpha)[100])
head(test)
tail(test)

span <- 100
alpha <- 2 / (span + 1)
weights <- (1 - alpha) ^ (1:10)



date
2005-01-04 15:31:00         NaN
2005-01-04 15:32:00    0.000288
2005-01-04 15:33:00    0.000204
2005-01-04 15:34:00    0.000198
2005-01-04 15:35:00    0.000279
...
2007-12-31 21:56:00    0.001308
2007-12-31 21:57:00    0.001374
2007-12-31 21:58:00    0.001456
2007-12-31 21:59:00    0.001535
2007-12-31 22:00:00    0.001638

# daily volatility
daily_volatility <- function() {

}


df0 = df0.ewm(span=lookback).std()




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
  return(t_events + 1)
}

cusum_events <- cusum_filter(cl, 0.003)
cusum_events <- zoo::index(Cl(market_data_sample)[cusum_events])

# add vertical bar
library(lubridate)
library(data.table)
add_vertical_barrier <- function(t_events,
                                 close_date,
                                 num_days=1,
                                 num_hours=0,
                                 num_minutes=0,
                                 num_seconds=0
) {
  timedelta <- days(num_days) + hours(num_hours) + minutes(num_minutes) + seconds(num_seconds)
  t_events_timdelta <- t_events + timedelta
  vertical_barrier <- as.data.table(close_date)[
    as.data.table(t_events_timdelta), on = 'x', roll = 'nearest'
  ]
  return(unlist(vertical_barrier))
}




# cusum events
t0_events <- cusum_filter(cl, 0.003)
t0_events <- zoo::index(Cl(market_data_sample)[t0_events])
t1_events <- add_vertical_barrier(cusum_events, zoo::index(Cl(market_data)))
length(t0_events)
length(t1_events)

# calculate triple barrier events
triple_barrier_events <- data.frame(
  t0 = t0_events,
  t1 = t1_events,
  trgt = 0.003,
  side = 1
)

# meta labels
meta_labels <- fmlr::label_meta(
  x = cl,
  events = triple_barrier_events,
  ptSl = c(2, 2),
  n_ex = 10
)
head(meta_labels)



close = Cl(market_data_sample)
t_events = cusum_events
pt_sl = c(1, 2)
target = 0.003
min_return = 0.0002
num_threads = 1
vertical_barrier_times = FALSE
side_prediction = NA
verbose = TRUE


# 1) Get target
target = target[t_events]
target = target[target > min_ret]  # min_ret


# 1) Get target
target = target.reindex(t_events)
target = target[target > min_ret]  # min_ret

# 2) Get vertical barrier (max holding period)
if vertical_barrier_times is False:
  vertical_barrier_times = pd.Series(pd.NaT, index=t_events, dtype=t_events.dtype)

# 3) Form events object, apply stop loss on vertical barrier
if side_prediction is None:
  side_ = pd.Series(1.0, index=target.index)
pt_sl_ = [pt_sl[0], pt_sl[1]]
else:
  side_ = side_prediction.reindex(target.index)  # Subset side_prediction on target index.
pt_sl_ = pt_sl[:2]

# Create a new df with [v_barrier, target, side] and drop rows that are NA in target
events = pd.concat({'t1': vertical_barrier_times, 'trgt': target, 'side': side_}, axis=1)
events = events.dropna(subset=['trgt'])

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


library(httr)

'/time-series/advanced_dividends/AAPL'

httr::GET()





library(devtools)
install_github("arbuzovv/rusquant")



library(rusquant)
getSymbolList('Finam') # download all available symbols in finam.ru

install.packages('devtools')

