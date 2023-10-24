library(data.table)
library(lubridate)
library(PerformanceAnalytics)
library(ggplot2)



# import adjusted daily market data
dt = fread("F:/lean_root/data/all_stocks_daily.csv")

# this want be necessary after update
setnames(dt, c("date", "open", "high", "low", "close", "volume", "close_adj", "symbol"))

# select columns
dt = dt[, .(symbol, date, close, close_adj, volume)]

# remove duplicates
dt = unique(dt, by = c("symbol", "date"))

# remove missing values
dt = na.omit(dt)

# create help colummns
dt[, year_month_id := ceiling_date(date, unit = "month") - days(1)]

# remove negative prices
dt = dt[close > 0 & close_adj > 0]

# add variables
dt[, dollar_volume := volume * close]

# downsample to lower frequency
# dtm = dt[, .(date = tail(date, 1),
#              close = tail(close_adj, 1),
#              close_raw = tail(close, 1),
#              volume_mean = mean(volume, na.rm = TRUE),
#              dollar_volume = sum(dollar_volume, na.rm = TRUE),
#              dollar_volume_mean = mean(dollar_volume, na.rm = TRUE),
#              volume = sum(volume, na.rm = TRUE)),
#          by = c("symbol", "year_month_id")]
# setorder(dtm, symbol, year_month_id)

# calculate momentum indicators
setorder(dt, symbol, date)
moms = seq(21, 21 * 12, 21)
mom_cols = paste0("mom", 1:12)
dt[, (mom_cols) := lapply(moms, function(x) close_adj  / shift(close_adj , x) - 1), by = symbol]
moms = seq(21 * 2, 21 * 12, 21)
mom_cols_lag = paste0("mom_lag", 2:12)
dt[, (mom_cols_lag) := lapply(moms, function(x) shift(close_adj, 21) / shift(close_adj , x) - 1), by = symbol]

# create target var
moms_target = c(5, 10, 21, 42, 63)
moms_target_cols = paste0("mom_target_", c("w", "2w", "m", "2m", "3m"))
dt[, (moms_target_cols) := lapply(moms_target, function(x) (shift(close_adj , -x, "shift") / close_adj ) - 1),
   by = symbol]

# downsample
dtm = dt[, .SD[.N], by = c("symbol", "year_month_id")]
dtm_2 = dt[, .(
  dollar_volume_sum = sum(dollar_volume, na.rm = TRUE),
  dollar_volume_mean = mean(dollar_volume, na.rm = TRUE),
  volume_sum = sum(volume, na.rm = TRUE),
  volume_mean = mean(volume, na.rm = TRUE)
), by = c("symbol", "year_month_id")]
dtm = dtm_2[dtm, on = c("symbol", "year_month_id")]
dtm[symbol == "aapl"]

# order data
setorder(dtm, symbol, year_month_id)

# plots
plot(dtm[symbol == "aapl", .(date, dollar_volume)],
     type = "l", main = "Volume and dolalr volme")
plot(dtm[symbol == "aapl", .(date, close_adj)],
     type = "l", main = "Volume and dolalr volme")




# BACKTEST OPTIMIZATION ---------------------------------------------------
# paramwters
num_coarse = c(50, 100, 200, 300, 500, 1000, 2000) # number of stocks we leave after coarse universe
filter_var = "dollar_volume_mean"                  # variable we use for filtering in coarse universe
min_price = c(1, 2, 5, 10)                         # minimal unadjusted price of the stock at month
num_long = c(10, 30, 50)                         # number of stocks in portfolio
mom_vars = colnames(dt)[grep("mom\\d+$|lag", colnames(dt))] # momenutm variables we apply ranking on
target_vars = "mom_target_m" # colnames(dt)[grep("target", colnames(dt))] # target variables
risk_variable = c("pra", "minmax")
params = expand.grid(num_coarse, filter_var, min_price, num_long, mom_vars,
                     target_vars,
                     stringsAsFactors = FALSE)
colnames(params) = c("num_coarse", "filter_var", "min_price", "num_long",
                     "mom_vars", "target_variables")

# momentum backtest function
strategy_momentum = function(x,
                             filter_var = "dollar_volume_mean",
                             num_coarse = 200,
                             min_price = 1,
                             num_long = 50,
                             mom_var = "mom11",
                             target_variables = "mom_target_m",
                             return_cumulative = TRUE) {

  # debug
  # num_coarse = 50
  # min_price = 2
  # num_long = 10
  # mom_var = "mom10"
  # target_variables = "mom_target_m"
  # x = copy(dtm)

  # remove missing values
  cols = c("symbol", "year_month_id", "close", filter_var, mom_var, target_variables)
  x = na.omit(x[, ..cols])

  # remove assets with price < x$
  x = x[, .SD[close > min_price], by = year_month_id]

  # filter 500 with highest volume
  setorderv(x, c("year_month_id", filter_var), order = 1L)
  x = x[, tail(.SD, num_coarse), by = year_month_id]

  # choose n with highest growth
  setorderv(x, c("year_month_id", mom_var), order = c(1, -1))
  y = x[, head(.SD, num_long), by = year_month_id]
  # y[year_month_id == as.Date("2023-04-30")]

  # get returns by month
  results = y[, .(returns = sum(get(target_variables) * 1/nrow(.SD))), by = year_month_id]

  # CAR
  if (return_cumulative) {
    car = Return.cumulative(as.xts.data.table(results))
    return(car)
  } else {
    return(results)
  }
}
strategy_momentum(dtm, "dollar_volume_mean", 200, 1, 50, "mom11", "mom_target_m") # example

# backtest across all parameters
results_l = vapply(1:nrow(params), function(i) {
  strategy_momentum(dtm,
                    filter_var = params$filter_var[i],
                    num_coarse = params$num_coarse[i],
                    min_price = params$min_price[i],
                    num_long = params$num_long[i],
                    mom_var = params$mom_vars[i],
                    target_variables = params$target_variables[i])
}, FUN.VALUE = numeric(1L))
results = as.data.table(cbind(params, results_l))

# analyse results
tail(results[order(results_l), ], 10)
results[mom_vars == "mom10" & min_price == 2 & num_long == 10 & num_coarse == 50]
ggplot(results, aes(num_coarse, min_price, fill = results_l)) +
  geom_tile()
ggplot(results, aes(num_coarse, num_long, fill = results_l)) +
  geom_tile()
ggplot(results, aes(min_price, num_long, fill = results_l)) +
  geom_tile()

# inspect atom
strategy_momentum(dtm, "dollar_volume_mean", 50, 2, 10, "mom10", "mom_target_m", TRUE)
x = strategy_momentum(dtm, "dollar_volume_mean", 50, 2, 10, "mom10", "mom_target_m", FALSE)
charts.PerformanceSummary(x)
x[order(returns)]

# compare with QC
dtm[year_month_id == "2023-05-31"] # same
symbol_ = "isee"
dt[symbol == symbol_]
plot(as.xts.data.table(dt[symbol == symbol_, .(date, close_adj)]), type = "l")
dtm[symbol == symbol_]
dtm[symbol == symbol_ & date == "2023-04-28"]
dtms[symbol == symbol_]
dtms[symbol == symbol_ & date == "2010-01-31"]
# Symbol F close_t-n 1.550195415 close_t-1 6.373025595 time 2010-01-01 00:00:00
# Symbol LVS close_t-n 1.72733057 close_t-1 10.91752951 time 2010-01-29 15:00:00   5.320463
# Symbol LVS close_t-n 1.6673075 close_t-1 10.31062958 time 2010-02-01  12:00:00   5.184
6.373025595 / 1.550195415 - 1
dt[symbol == symbol_ & date %between% c("2023-02-01", "2023-04-02")]
dt[symbol == symbol_ & date %between% c("2009-01-26", "2009-02-02")]
dt[symbol == symbol_ & date %between% c(as.Date("2009-12-28") - 270, as.Date("2009-12-28") - 230)]
dt[symbol == symbol_ & round(close_adj, 3) == 1.727]
dt[symbol == symbol_ & round(close_adj, 3) == 11.058]


# vis results
charts.PerformanceSummary(portfolio_ret)

# Portfolio results
SharpeRatio(portfolio_ret)
SharpeRatio.annualized(portfolio_ret)


# inspect
test = as.data.table(portfolio_ret)
test[portfolio.returns > 0.5]
test[portfolio.returns <- -0.5]
