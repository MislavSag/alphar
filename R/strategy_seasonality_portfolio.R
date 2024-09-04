library(data.table)
library(qlcal)
library(lubridate)
library(AzureStor)


# Set calendar
calendars
setCalendar("UnitedStates/NYSE")

# Import results
file = list.files("D:/features", pattern = "week", full.names = TRUE)
seasonality_results = readRDS(file)
setDT(seasonality_results)
class(seasonality_results)
dim(seasonality_results)
colnames(seasonality_results)
seasonality_results[[3]]

# Portfolio 1:
# 1) keep min Pr for every symbol
# 2) keep top 10 for every month
portfolio1 = melt(seasonality_results, id.vars = "symbol", variable.name = "date", value.name = "value")
portfolio1 = na.omit(portfolio1)
portfolio1 = portfolio1[, rbindlist(value), by = .(date, symbol)]
portfolio1 = portfolio1[rn != "(Intercept)"]
portfolio1[, minp := min(`Pr(>|t|)`) == `Pr(>|t|)`, by = .(date, symbol)]
portfolio1 = portfolio1[minp == TRUE]
setorderv(portfolio1, c("date", "Pr(>|t|)"))
portfolio1 = portfolio1[, head(.SD, 100), by = date]

# Clean portfolios
portfolio_prepare = function(portfolio) {
  # portfolio = copy(portfolio1)

  # set trading dates
  # portfolio[, date := as.Date(gsub("month", "", date), format = "%y%m%d")]
  portfolio[, rn := gsub("day_of_month", "", rn)]

  # get trading days
  date_ = portfolio[, as.Date(paste0(gsub("month", "", date), "01"), format = "%y%m%d")]
  seq_ = 1:nrow(portfolio)
  seq_dates = lapply(date_, function(x) getBusinessDays(x, x %m+% months(1) - 1))
  dates = mapply(function(x, y) x[y], x = seq_dates, y = portfolio[, as.integer(rn)])
  portfolio[, dates_trading := as.Date(dates, origin = "1970-01-01")]
  portfolio
}
portfolio1 = portfolio_prepare(portfolio1)
# portfolio2 = portfolio_prepare(portfolio2)
# portfolio3 = portfolio_prepare(portfolio3)

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
# save_qc(portfolio2, "seasons-portfolio2.csv")
# save_qc(portfolio3, "seasons-portfolio3.csv")
