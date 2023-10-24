library(data.table)
library(quantreg)
library(AzureStor)
library(qlcal)
library(lubridate)



# DATA IMPORT -------------------------------------------------------------
# set up
setCalendar("UnitedStates/NYSE")

# import daily market data
system.time({dt = fread("F:/lean_root/data/all_stocks_daily.csv")})

# this want be necessary after update
setnames(dt, c("date", "open", "high", "low", "close", "volume", "close_adj", "symbol"))

# remove duplicates
dt = unique(dt, by = c("symbol", "date"))

# remove missing values
dt = na.omit(dt)

# order data
setorder(dt, "symbol", "date")

# adjust all prices, not just close
adjust_cols <- c("open", "high", "low")
adjust_cols_new <- c("open_adj", "high_adj", "low_adj")
dt[, (adjust_cols_new) := lapply(.SD, function(x) x * (close_adj / close)), .SDcols = adjust_cols] # adjust open, high and low prices

# calculate returns
dt[, returns := close_adj / shift(close_adj) - 1, by = symbol] # calculate returns
dt <- dt[returns < 1] # TODO:: better outlier detection mechanism. For now, remove daily returns above 100%

# plot
plot(as.xts.data.table(dt[symbol == "aapl", .(date, close_adj)]))
plot(as.xts.data.table(dt[symbol == "meta", .(date, close_adj)]))
plot(as.xts.data.table(dt[symbol == "fb", .(date, close_adj)]))

# check for zero prices
dt[close_adj == 0] # there is not zero prices
dt = dt[close > 0 & close_adj > 0]

# remove symobls with < 252 observations
dt_n <- dt[, .N, by = symbol]
dt_n <- dt_n[N > 252 * 4]
dt <- dt[symbol %in% dt_n[, symbol]]

# save SPY for later and keep only events symbols
spy <- dt[symbol == "spy"]



# SEASONALITY MINING ------------------------------------------------------
# define target variables
dt[, return_day := shift(close_adj, 1, type = "lead") / close_adj - 1, by = symbol]
dt[, return_day3 := shift(close_adj, 3, type = "lead") / close_adj - 1, by = symbol]
dt[, return_week := shift(close_adj, 5, type = "lead") / close_adj - 1, by = symbol]
dt[, return_week2 := shift(close_adj, 10, type = "lead") / close_adj - 1, by = symbol]

# define frequency unit
dt[, yearmonthid := round(date, digits = "month")]
dt[, day_of_month := 1:.N, by = .(symbol, yearmonthid)]
dt[, day_of_month := as.factor(day_of_month)]

# remove missing values
dt = na.omit(dt, cols = c("symbol", "return_week2", "day_of_month"))

# get coeffs from summary of quantile regression
get_coeffs = function(df, y = "return_week") {
  # df = dt[symbol == "a.1"]
  res = rq(as.formula(paste0(y, " ~ day_of_month")), data = as.data.frame(df))
  summary_fit = summary.rq(res, se = 'nid')
  as.data.table(summary_fit$coefficients, keep.rownames = TRUE)
}

# define all year-months and start year
yearmonthids = dt[, sort(unique(yearmonthid))]
end_years = seq.Date(as.Date("2019-01-01"),
                     as.Date("2023-05-01"), by = "month")
end_years = as.character(end_years)
# first_year = "2010-01-01"

# get median regression coefficients
# symbols = dt[, unique(symbol)]
# sample_ = dt[symbol %in% symbols[1:20000]]
dt[day_of_month == 23, day_of_month := 22] # 23 day to 22 day
dt_sample = dt[, .SD[(as.IDate(end_years[1])-max(date)) < 7], by = symbol] # we must have
dt_sample = dt_sample[, .SD[nrow(.SD) > 1008], by = symbol] # we must have at least 3 yers of data

# dt_sample[, year := year(date)]
X_seasons_day3 = dt_sample[, lapply(as.IDate(end_years), function(y) {
  if ((y - max(date)) > 7) return(list(NA))
  tryCatch(list(get_coeffs(.SD[yearmonthid %between% c(y - 2520, y)]), "return_day3"),
           error = function(e) list(NA))
  }), by = .(symbol)]

cols = paste0("month", strftime(end_years, format = "%y%m%d"))
colnames(X_seasons_day3)[2:length(colnames(X_seasons_day3))] = cols

# save
time = strftime(Sys.time(), "%Y%m%d%H%M%S")
saveRDS(X_seasons_day3, file.path("D:/features", paste0("seasonality-day3", time, ".rds")))


# CREATE PORTFOLIOS -------------------------------------------------------
# create portfolio function
portfolios_l = list()
for (i in seq_along(cols)) {

  # sample
  col = cols[i]
  cols_ = c("symbol", col)
  x = X_seasons[, ..cols_]

  # remove missing values
  x[, number_of_rows := vapply(get(col), function(y) length(y), FUN.VALUE = integer(1L))]
  x = x[number_of_rows > 1]

  # unnest
  x = x[, rbindlist(get(col)), by = symbol]

  # remove intercept
  x = x[rn != "(Intercept)"]

  # keep min Pr for every symbol
  x[, minp := min(`Pr(>|t|)`) == `Pr(>|t|)`, by = symbol]

  # keep lowet p for every stock
  x_min = x[minp == TRUE]

  # filter symbols to trade
  setorder(x_min, "Pr(>|t|)")
  portfolios_l[[i]] = cbind(date = col, head(x_min, 10))
}
portfolio1 = rbindlist(portfolios_l)

# create portfolio function v2
portfolios_l = list()
for (i in seq_along(cols)) {

  # sample
  col = cols[i]
  cols_ = c("symbol", col)
  x = X_seasons[, ..cols_]

  # remove missing values
  x[, number_of_rows := vapply(get(col), function(y) length(y), FUN.VALUE = integer(1L))]
  x = x[number_of_rows > 1]

  # unnest
  x = x[, rbindlist(get(col)), by = symbol]

  # remove intercept
  x = x[rn != "(Intercept)"]

  # filter longs
  x = x[Value > 0]

  #
  x[, minp := min(`Pr(>|t|)`) == `Pr(>|t|)`, by = symbol]

  # keep lowet p for every stock
  x_min = x[minp == TRUE]

  # filter symbols to trade
  setorder(x_min, "Pr(>|t|)")
  portfolios_l[[i]] = cbind(date = col, head(x_min, 10))
}
portfolio2 = rbindlist(portfolios_l)


# create portfolio function v3
portfolios_l = list()
for (i in seq_along(cols)) {

  # sample
  col = cols[i]
  cols_ = c("symbol", col)
  x = X_seasons[, ..cols_]

  # remove missing values
  x[, number_of_rows := vapply(get(col), function(y) length(y), FUN.VALUE = integer(1L))]
  x = x[number_of_rows > 1]

  # unnest
  x = x[, rbindlist(get(col)), by = symbol]

  # remove intercept
  x = x[rn != "(Intercept)"]

  # filter longs
  x = x[Value > 0]

  #
  x[, rnn := as.integer(gsub("day_of_month", "", rn))]
  setorder(x, symbol, rnn)
  x[, pr_roll := frollapply(`Pr(>|t|)`, 4, function(x) all(x < 0.01)), by = symbol]
  x = na.omit(x)
  x = x[pr_roll == 1]

  # filter symbols to trade
  x = x[, (rn = head(rn, 1)), by = symbol]
  portfolios_l[[i]] = cbind(date = col, x)
}
portfolio3 = rbindlist(portfolios_l)
setnames(portfolio3, "V1", "rn")
portfolio3[, Value := 1]

# clean portfolios
portfolio_prepare = function(portfolio) {
  # set trading dates
  portfolio[, date := as.Date(gsub("month", "", date), format = "%y%m%d")]
  portfolio[, rn := gsub("day_of_month", "", rn)]

  # get trading days
  date_ = portfolio[, date]
  seq_ = 1:nrow(portfolio)
  seq_dates = lapply(date_, function(x) getBusinessDays(x, x %m+% months(1) - 1))
  dates = mapply(function(x, y) x[y], x = seq_dates, y = portfolio[, as.integer(rn)])
  portfolio[, dates_trading := as.Date(dates, origin = "1970-01-01")]
  portfolio
}
portfolio1 = portfolio_prepare(portfolio1)
portfolio2 = portfolio_prepare(portfolio2)
portfolio3 = portfolio_prepare(portfolio3)

# save to Azure for backtesting
save_qc = function(portfolio, file_name) {
  portfoliosqc = portfolio[, .(dates_trading, symbol, rn, Value)]
  setorder(portfoliosqc, dates_trading)
  portfoliosqc = na.omit(portfoliosqc)
  setnames(portfoliosqc, "dates_trading", "date")
  portfoliosqc = portfoliosqc[, .(symbol = paste0(symbol, collapse = "|"),
                                  rn     = paste0(rn, collapse = "|"),
                                  value  = paste0(Value, collapse = "|")),
                              by = date]
  portfoliosqc[, date := as.character(date)]
  portfoliosqc = na.omit(portfoliosqc)
  blob_key = "0M4WRlV0/1b6b3ZpFKJvevg4xbC/gaNBcdtVZW+zOZcRi0ZLfOm1v/j2FZ4v+o8lycJLu1wVE6HT+ASt0DdAPQ=="
  endpoint = "https://snpmarketdata.blob.core.windows.net/"
  BLOBENDPOINT = storage_endpoint(endpoint, key=blob_key)
  cont = storage_container(BLOBENDPOINT, "qc-backtest")
  storage_write_csv(portfoliosqc, cont, file_name)
}
save_qc(portfolio1, "seasons-portfolio1.csv")
save_qc(portfolio2, "seasons-portfolio2.csv")
save_qc(portfolio3, "seasons-portfolio3.csv")

# test
dt[symbol == "aac"]
dt[symbol == "aac" & date %between% c("2019-10-01", "2020-01-10")]
