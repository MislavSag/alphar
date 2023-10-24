library(data.table)
library(stringr)
library(qlcal)
library(lubridate)
library(AzureStor)


# set up
setCalendar("UnitedStates/NYSE")

# get all files
list.files("D:/features", pattern = "seas")
results = readRDS("D:/features/seasonality-20230602055604.rds")

# cols
cols = colnames(results)[2:ncol(results)]

# portfolio 1
portfolios_l = list()
for (i in seq_along(cols)) {

  # sample
  col = cols[i]
  cols_ = c("symbol", col)
  x = results[, ..cols_]

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

# portfolio 2
portfolios_l = list()
for (i in seq_along(cols)) {

  # sample
  col = cols[i]
  cols_ = c("symbol", col)
  x = results[, ..cols_]

  # remove missing values
  x[, number_of_rows := vapply(get(col), function(y) length(y), FUN.VALUE = integer(1L))]
  x = x[number_of_rows > 1]

  # unnest
  x = x[, rbindlist(get(col)), by = symbol]

  # remove intercept
  x = x[rn != "(Intercept)"]

  # rolling low p value and same sign
  x[, rnn := as.integer(gsub("day_of_month", "", rn))]
  setorder(x, symbol, rnn)
  x[, pr_roll := frollapply(`Pr(>|t|)`, 3, function(x) all(x < 0.01) & length(unique(sign(x))) == 1), by = symbol]
  x = na.omit(x)
  x = x[pr_roll == 1]

  # filter symbols to trade
  x = x[, (head(.SD, 1)), by = symbol]
  portfolios_l[[i]] = cbind(date = col, x)
}
portfolio2 = rbindlist(portfolios_l)

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
portfolio1_cleaned = portfolio_prepare(portfolio1)
portfolio2_cleaned = portfolio_prepare(portfolio2)

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
save_qc(portfolio1_cleaned, "seasons-portfolio1.csv")
save_qc(portfolio2_cleaned, "seasons-portfolio2.csv")
