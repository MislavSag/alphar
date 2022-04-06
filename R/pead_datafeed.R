library(data.table)
library(runner)
library(equityData)
library(AzureStor)
library(mlfinance)
library(xts)
library(future.apply)
library(doParallel)
library(forecast)
library(mlfinance)



# save earnings announcement data to blob. THIS TAKES SOME TIME
events <- get_earning_announcements(
  as.Date("1990-01-01"),
  Sys.Date(),
  blob_file = "earnings-calendar.rds"
)

# save earnings calendar from investing.com to blob.
es_ic <- get_investingcom_earnings_calendar_bulk(start_date = "2015-01-01", blob_file = "earnings-calendar-investingcom.rds")

# save earnings calendar from yahoo to blob.
# es_yahoo <- get_yahoo_calendar_all(as.Date("2003-01-01"), end_date = Sys.Date(), blob_file = "earnings-calendar-yahoo.rds")

# save transcripts data to blob. THIS TAKES LONG TIME
downloaded_transcripts <- get_all_blob_files("transcripts")
get_transcripts(
  symbols = setdiff(unique(events$symbol), gsub("\\.rds", "", downloaded_transcripts$name)),
  years = data.table::year(min(events$date)):data.table::year(Sys.Date()),
  blob_cont = "transcripts"
)

# save daily prices
prices <- get_daily_prices(
  symbols = unique(events$symbol),
  start_date = min(events$date) - 365,
  end_date = Sys.Date(),
  blob_file = "prices.rds",
  blob_cont = "fundamentals"
)

# abnormal returns labeling
events <- get_blob_file("earnings-calendar.rds", "fundamentals", save_file = "D:/fundamental_data/earnings-calendar.rds", refresh_data_old = 100)
events_car <- events[, .(symbol, date)]
data.table::setnames(events_car, c("name", "when"))
events_car <- na.omit(events_car)
prices <- get_blob_file("prices.rds", container = "fundamentals", save_file = "D:/fundamental_data/prices.rds", refresh_data_old = 100)
prices <- setorderv(prices, c("symbol", "date"))
prices[, returns := adjClose / data.table::shift(adjClose) - 1, by = symbol]
prices <- na.omit(prices)
firm_returns <- dcast(prices, date ~ symbol, value.var = "returns")
firm_returns <- as.xts.data.table(firm_returns)
plan(multicore(workers = 8L))
spy <- get_daily_prices("SPY", start_date = min(events_car$when) - 100, end_date = Sys.Date(), blob_file = "SPY.rds")
spy <- setorder(spy, "date")
market_returns <- xts(spy$adjClose / shift(spy$adjClose, 1L) - 1, order.by = as.Date(spy$date))
# apply function
labels <- labeling_abnormal_returns(
  events = events_car,
  firm_returns = firm_returns,
  market_returns = market_returns,
  event_horizon = 62,
  model_type = "excessReturn" # None, marketModel
)
save_blob_files(labels, file_name = "car_labels_excessReturn.rds", container = "fundamentals")
