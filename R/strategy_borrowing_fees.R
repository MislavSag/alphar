library(fastverse)
library(finutils)
library(httr)
library(ggplot2)
library(PerformanceAnalytics)


# PRICES ------------------------------------------------------------------
# Import daily prices
prices = qc_daily(
  file_path = "F:/lean/data/stocks_daily.csv",
  min_obs = 252,
  add_dv_rank = TRUE,
  add_day_of_month = FALSE
)

# Select 1000 most liquid on last 365 calnedar days
symbols = prices[date > as.Date("2018-01-01"), mean(dollar_vol_rank), by = symbol]
setorder(symbols, V1)
symbols = symbols[, unique(head(symbol, 1000))]


# RW DATA -----------------------------------------------------------------
# Import IB borrowing dataset
ibb = fread("F:/data/rw_data/macropod/ib_shortstock_2023.csv.gzip")
ibb[, available := gsub(">", "", available)]
ibb[, available := as.integer(available)]
setorder(ibb, date)

# merge prices and fees
dt = prices[, .(symbol, date, open, close, returns)] |>
  _[ibb[,. (symbol = tolower(symbol), date, fee_rate, rebate_rate)],
    on = c("symbol", "date")]
dt = na.omit(dt, cols = c("close", "fee_rate"))

# Summary stats
plot(as.xts.data.table(dt[, mean(fee_rate), by = date]))
plot(as.xts.data.table(dt[, median(fee_rate), by = date]))
dt[, mean(fee_rate), by = symbol] |>
  ggplot(aes(x = V1)) +
  geom_histogram(bins = 30) +
  theme_minimal()

# Create target variablehttp://127.0.0.1:36087/graphics/b15167ab-ec94-434e-8761-47d21a3e6bec.png
dt[, unique(symbol)]
setorder(dt, symbol, date)
dt[, fee_decile := dplyr::ntile(rebate_rate, 10), by = date]

# Buy asset with lowest fee
dt[, mean(returns), by = fee_decile][order(fee_decile)] |>
  ggplot(aes(x = fee_decile, y = V1)) +
  geom_bar(stat = "identity") +
  theme_minimal()
dt[, median(returns), by = fee_decile][order(fee_decile)] |>
  ggplot(aes(x = fee_decile, y = V1)) +
  geom_bar(stat = "identity") +
  theme_minimal()

# Backtest
back = copy(dt)
back[, fee_decile := shift(fee_decile), by = symbol]
back = na.omit(back)
back[, weight := 1 / .N, by = .(fee_decile, date)]
portfolio = back[, .(returns = sum(weight * returns)), .(fee_decile, date)]
portfolio = dcast(portfolio, date ~ fee_decile, value.var = "returns")
portfolio_xts = as.xts.data.table(portfolio)
spy_xts = prices[symbol == "spy" & date %between% c(back[, min(date)], back[, max(date)])]
spy_xts = as.xts.data.table(spy_xts[, .(date, returns)])
portfolio_xts = portfolio_xts[rowSums(portfolio_xts < 1) == 10]
charts.PerformanceSummary(cbind(portfolio_xts, spy_xts))

# Weekly rebalance
dt[, week := as.integer(paste0(data.table::year(date), stringr::str_pad(data.table::week(date), width = 2, pad = "0")))]
back = dt[, .(
  open = data.table::first(open),
  close = data.table::last(close),
  rebate = data.table::last(rebate_rate),
  fee = data.table::last(fee_rate)
), by = .(symbol, week)]
back[, week_ret := close / open - 1, by = symbol]
back[, week_ret := shift(week_ret), by = symbol]
back = na.omit(back)
back[, rebate_rank := frank(rebate, ties.method = "dense"), by = week]
n = 30
portfolio = back[rebate_rank <= n, .(returns = sum(1/n * week_ret)), by = week]
portfolio[, week := paste0(substr(week, 1, 4), "-W", substr(week, 5, 6))]
portfolio[, week := ISOweek:: ISOweek2date(paste0(week, "-1"))]
charts.PerformanceSummary(as.xts.data.table(portfolio))


# SCRAPED BORROWING DATA --------------------------------------------------
# Scrap data from https://www.iborrowdesk.com
path = "D:/strategies/ibborrow/fees"
# dir.create(path, showWarnings = FALSE)
url = "https://www.iborrowdesk.com/api/ticker/"
urls = paste0(url, gsub("\\.\\d+", "", toupper(symbols)))
ibborrow = lapply(urls, function(u) {
  # u = urls[1]
  file_name = file.path(path, paste0(basename(u), ".rds"))
  if (file.exists(file_name)) {
    print(paste0("Exist ", file_name))
    return(NULL)
  }

  Sys.sleep(5L)
  print(u)
  p = RETRY(
    "GET",
    u,
    add_headers("User-Agent" = "Mozilla/5.0"),
    times = 5,
    terminate_on = c(200, 404),
    timeout(10)
  )
  if (status_code(p) == 404) {
    print(paste0("No data for ", u))
    return(NULL)
  } else if (status_code(p) == 444) {
    print(paste0("Too many requests for ", u))
    Sys.sleep(60L)
    p = RETRY(
      "GET",
      u,
      add_headers("User-Agent" = "Mozilla/5.0"),
      times = 5
    )
  }
  res = content(p)
  saveRDS(res, file_name)
  return(1)
})
lengths(ibborrow)

# Import results
files = list.files(path, full.names = TRUE)
ibborrow = lapply(files, readRDS)

# Extract daily and save
daily = lapply(ibborrow, function(x) {
  # x = ibborrow[[1]]
  meta_ = as.data.table(x[lengths(x) == 1])
  daily_ = rbindlist(lapply(x$daily, as.data.table), fill = TRUE)
  daily_ = cbind(meta_, daily_)
  return(daily_)
})
daily = rbindlist(daily, fill = TRUE)
daily[, date := as.Date(date)]
fwrite(daily, "D:/strategies/ibborrow/daily.csv")

# Extract realtime ans save
# realtime = lapply(ibborrow, function(x) {
#   # x = ibborrow[[1]]
#   meta_ = as.data.table(x[lengths(x) == 1])
#   daily_ = rbindlist(lapply(x$real_time, as.data.table), fill = TRUE)
#   daily_ = cbind(meta_, daily_)
#   return(daily_)
# })
# realtime = rbindlist(realtime, fill = TRUE)
# realtime[, errors := NULL]
# fwrite(realtime, "D:/strategies/ibborrow/realtime.csv")

# merge prices and fees
dt = prices[, .(symbol, date, open, close, returns)] |>
  _[daily[,. (symbol = tolower(symbol), date, fee, rebate)],
    on = c("symbol", "date")]
dt = na.omit(dt, cols = "close")

# Summary stats
plot(as.xts.data.table(dt[, mean(fee), by = date]))
plot(as.xts.data.table(dt[, median(fee), by = date]))
dt[, mean(fee), by = symbol] |>
  ggplot(aes(x = V1)) +
  geom_histogram(bins = 30) +
  theme_minimal()

# Create target variablehttp://127.0.0.1:36087/graphics/b15167ab-ec94-434e-8761-47d21a3e6bec.png
dt[, unique(symbol)]
setorder(dt, symbol, date)
dt[, fee_decile := dplyr::ntile(fee, 10), by = date]

# Buy asset with lowest fee
dt[, mean(returns), by = fee_decile][order(fee_decile)] |>
  ggplot(aes(x = fee_decile, y = V1)) +
  geom_bar(stat = "identity") +
  theme_minimal()
dt[, median(returns), by = fee_decile][order(fee_decile)] |>
  ggplot(aes(x = fee_decile, y = V1)) +
  geom_bar(stat = "identity") +
  theme_minimal()

# Backtest
back = copy(dt)
back[, fee_decile := shift(fee_decile), by = symbol]
back = na.omit(back)
back[, weight := 1 / .N, by = .(fee_decile, date)]
portfolio = back[, .(returns = sum(weight * returns)), .(fee_decile, date)]
portfolio = dcast(portfolio, date ~ fee_decile, value.var = "returns")
portfolio_xts = as.xts.data.table(portfolio)
spy_xts = prices[symbol == "spy" & date %between% c(back[, min(date)], back[, max(date)])]
spy_xts = as.xts.data.table(spy_xts[, .(date, returns)])
charts.PerformanceSummary(cbind(portfolio_xts, spy_xts))

# Weekly rebalance
dt[, week := as.integer(paste0(data.table::year(date), stringr::str_pad(data.table::week(date), width = 2, pad = "0")))]
back = dt[, .(
  open = data.table::first(open),
  close = data.table::last(close),
  rebate = data.table::last(rebate),
  fee = data.table::last(fee)
), by = .(symbol, week)]
back[, week_ret := close / open - 1, by = symbol]
back[, week_ret := shift(week_ret), by = symbol]
back = na.omit(back)
back[, rebate_rank := frank(rebate, ties.method = "dense"), by = week]
n = 30
portfolio = back[rebate_rank <= n, .(returns = sum(1/n * week_ret)), by = week]
portfolio[, week := paste0(substr(week, 1, 4), "-W", substr(week, 5, 6))]
portfolio[, week := ISOweek:: ISOweek2date(paste0(week, "-1"))]
charts.PerformanceSummary(as.xts.data.table(portfolio))

# Monthly rebalance
dt[, month := data.table::yearmon(date)]
back = dt[, .(
  open = data.table::first(open),
  close = data.table::last(close),
  rebate = data.table::last(rebate),
  fee = data.table::last(fee)
), by = .(symbol, month)]
back[, mon_ret := close / open - 1, by = symbol]
back[, mon_ret := shift(mon_ret), by = symbol]
back = na.omit(back)
back[, rebate_rank := frank(rebate, ties.method = "dense"), by = month]
n = 30
portfolio = back[rebate_rank <= n, .(returns = sum(1/n * mon_ret)), by = month]
portfolio[, month := zoo::as.Date.yearmon(month)]
charts.PerformanceSummary(as.xts.data.table(portfolio))
