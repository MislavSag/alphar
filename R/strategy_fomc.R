library(rvest)
library(data.table)
library(finutils)
library(qlcal)
library(PerformanceAnalytics)
library(stringr)
library(jsonlite)
library(AzureStor)
library(RollingWindow)


# Setup
PATH = "F:/strategies/fomc"
if (!dir.exists(PATH)) {
  dir.create(PATH, recursive = TRUE)
}

# Set calendar
qlcal::calendars
setCalendar("UnitedStates/NYSE")

# Get FOMC metadata
fomc_recent  = fromJSON("https://www.federalreserve.gov/monetarypolicy/materials/assets/final-recent.json")$mtgitems
fomc_history = fromJSON("https://www.federalreserve.gov/monetarypolicy/materials/assets/final-hist.json")$mtgitems
setDT(fomc_recent)
setDT(fomc_history)
fomc = rbindlist(list(fomc_recent, fomc_history), fill = TRUE)
saveRDS(fomc, file.path(PATH, "fomc_metadata.rds"))

# Extract dates
fomc = fomc[type == "St"]
fomc[, date := as.Date(d, format = "%Y-%m-%d")]
fomc_dates = fomc[, sort(date)]
fomc_dates = fomc_dates[fomc_dates >= as.Date("1998-01-01")]
fomc_dates = unique(fomc_dates)

# Save dates to quantconnect for backtesting
qc_data = data.table(date = fomc_dates)
# qc_data[, date := as.Date(vapply(date, function(x) qlcal::advanceDate(x, -1L), FUN.VALUE = lubridate::Date(1L)))]
qc_data[, date := as.character(date)]
endpoint = storage_endpoint(Sys.getenv("BLOB-ENDPOINT-SNP"),
                            key=Sys.getenv("BLOB-KEY-SNP"))
cont = storage_container(endpoint, "qc-backtest")
storage_write_csv(qc_data, cont, "fomc_dates.csv")


# # INDIVIDUAL --------------------------------------------------------------
# # Import data for SPY and TLT
# symbols = c("spy", "tlt", "gld", "gsg")
# prices = qc_hour(
#   file_path = "F:/lean/data/stocks_hour.csv",
#   symbols = symbols
# )
#
# # Loop over fomc dates and caluclate returns
# fomc_returns_l = list()
# day_offset = 1 # can be 0, 1, 2
# for (i in seq_along(fomc_dates)) {
#   # print(i)
#   # Extract date
#   # i = 101
#   date_ = fomc_dates[i]
#
#   # Get prices for the date range
#   # qlcal::isBusinessDay(date_-1)
#   if (!qlcal::isBusinessDay(date_)) {
#     print(paste0("Date ", date_, " is not business date."))
#     next
#   }
#   if (day_offset == 0) {
#     dt_ = prices[as.Date(date) == date_]
#   } else {
#     dt_ = prices[as.Date(date) %between% c(date_ - day_offset, date_)]
#   }
#
#   # Calculate returns and save
#   fomc_returns_l[[i]] = dt_[, .(date = data.table::last(date_),
#                                 ret = data.table::last(close) / data.table::first(close) - 1),
#                             by = symbol]
# }
# fomc_returns = rbindlist(fomc_returns_l)
# setorder(fomc_returns, date)
# fomc_returns = dcast(fomc_returns, date ~ symbol, value.var = "ret", fill = NA_real_)
# fomc_returns_xts = as.xts.data.table(fomc_returns)
# charts.PerformanceSummary(fomc_returns_xts)
# SharpeRatio.annualized(fomc_returns_xts)
# Return.cumulative(fomc_returns_xts)
# # charts.PerformanceSummary(fomc_returns_xts["2022/"])
#
# # Equal weights
# fomc_returns_xts_equal = na.omit(fomc_returns)
# fomc_returns_xts_equal = fomc_returns_xts_equal[, -"gsg"]
# fomc_returns_xts_equal[, weight := 1 / length(symbols)]
# fomc_returns_xts_equal = fomc_returns_xts_equal[, .(ret = weight * gld + weight * spy + weight * tlt), by = date]
# # fomc_returns_xts_equal = fomc_returns_xts_equal[, .(ret = weight * gld + weight * spy + weight * tlt + weight * gsg), by = date]
# setorder(fomc_returns_xts_equal, date)
# fomc_returns_xts = as.xts.data.table(fomc_returns_xts_equal)
# charts.PerformanceSummary(fomc_returns_xts)
# SharpeRatio.annualized(fomc_returns_xts, scale = 12)
# Return.annualized(fomc_returns_xts, scale = 12)
# min(Drawdowns(fomc_returns_xts))
# # charts.PerformanceSummary(fomc_returns_xts["2010/"])


# FOMC MINING -------------------------------------------------------------
# ETF symbols
etf_symbols = list.files("F:/lean/data/equity/usa/universes/etf")
etf_symbols = unique(etf_symbols)

# Import hour prices
prices = qc_hour(
  file_path = "F:/lean/data/stocks_hour.csv",
  symbols = etf_symbols,
  min_obs = 7*252*10
)
prices[, length(unique(symbol))]

# Loop over fomc dates and caluclate returns
fomc_returns_l = list()
day_offset = 1
for (i in seq_along(fomc_dates)) {
  # get date
  date_ = fomc_dates[i]

  # Get prices for the date range
  if (!qlcal::isBusinessDay(date_)) {
    print(paste0("Date ", date_, " is not business date."))
    next
  }
  if (day_offset == 0) {
    dt_ = prices[as.Date(date) == date_]
  } else {
    dt_ = prices[as.Date(date) %between% c(date_ - day_offset, date_)]
  }

  # Calculate returns and save
  fomc_returns_l[[i]] = dt_[, .(date = data.table::last(date_),
                                ret = data.table::last(close) / data.table::first(close) - 1),
                            by = symbol]
}
fomc_returns = rbindlist(fomc_returns_l)
setorder(fomc_returns, symbol, date)

# Returns by ETF
performance = fomc_returns[, .(
  returns_annualized = Return.annualized(ret, scale = 12),
  returns_cumulative = Return.cumulative(ret),
  sharpe = SharpeRatio.annualized(.SD[, .(date, ret)], scale = 12)
  # dd = min(Drawdowns(.SD[, .(date, ret)]), na.rm = TRUE)
), by = symbol]
setorder(performance, -sharpe)
head(performance, 50)

# Rolling momentum FOMC
symbols_remove = fomc_returns[, length(ret) <= 60, by = symbol]
symbols_remove[V1 == TRUE]
Sharpe = function(x, Rf, scale = 60){
  r = prod(1 + x - Rf)^(scale / length(x)) - 1
  res = r / (sd(x) * sqrt(scale))
  return(res)
}
fomc_returns[, roll_sharpe := frollapply(
  ret, n = 60, FUN = Sharpe, Rf = 0.0, align = "right", fill = NA_real_
), by = symbol]
setorder(fomc_returns, symbol, date)
fomc_returns[, sharpe_rank := frankv(roll_sharpe, order = -1L, ties.method = "first"), by = date]
fomc_returns[, signal := shift(sharpe_rank) %in% 1:3]
portfolio = fomc_returns[signal == TRUE]
portfolio[, weights := 1 / length(signal), by = date]
portfolio = portfolio[date > as.Date("2005-01-01")]
portfolio_xts = portfolio[, .(ret = sum(weights * ret)), by = date]
setorder(portfolio_xts, date)
portfolio_xts = as.xts.data.table(portfolio_xts)
charts.PerformanceSummary(portfolio_xts)
SharpeRatio.annualized(portfolio_xts, scale = 12)
Return.annualized(portfolio_xts, scale = 12)
Return.cumulative(portfolio_xts)
min(Drawdowns(portfolio_xts))


# Archive -----------------------------------------------------------------
# Get FOMC dates
# url ="https://en.wikipedia.org/wiki/History_of_Federal_Open_Market_Committee_actions"
# p = read_html(url)
# fomc_dates = p |>
#   html_elements("table")
# fomc_dates = fomc_dates[[2]] |>
#   html_table()
# setDT(fomc_dates)
# fomc_dates[, date := as.Date(Date, format = "%b %d, %Y")]
# setorder(fomc_dates, date)
#
# # Checks
# fomc_dates[, any(is.na(date))]
# fomc_dates[, date]

# # Get FOMC dates history
# urls = paste0(
#   "https://www.federalreserve.gov/monetarypolicy/fomchistorical", 2000:2019, ".htm"
# )
# fomc_dates = c()
# for (url in urls) {
#   # u = urls[1]
#   p = read_html(url)
#   dates_ = p |>
#     html_elements(".panel") |>
#     html_elements("h5") |>
#     html_text()
#   month_ = stringr::word(dates_, 1, 1)
#   month_ = str_split(month_, pattern = "/")
#   month_ = vapply(month_, function(x) ifelse(length(x) == 2, x[2], x[1]), FUN.VALUE = character(1))
#   year_  = stringr::word(dates_, -1, -1)
#   day_ = stringr::str_extract_all(dates_, "\\d{1,2}")
#   day_ = vapply(day_, function(x) ifelse(length(x) == 3, x[1], x[2]), FUN.VALUE = character(1))
#   fomc_dates = c(
#     fomc_dates,
#     as.Date(paste0(year_, "-", month_, "-", day_), format = "%Y-%B-%d")
#   )
#   Sys.sleep(0.5)
# }
# fomc_dates = as.Date(fomc_dates)
#
# # Get FOMC dates from 2020
# url = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
# p = read_html(url)
# fomc_tables = p |>
#   html_elements(".panel")
# fomc_tables[[1]] |>
#   html_elements(".fomc-meeting__date col-xs-4 col-sm-9 col-md-10 col-lg-1") |>
#   html_text()
# p |>
#   html_elements("#article > div:nth-child(6) > div:nth-child(2) > div.fomc-meeting__date.col-xs-4.col-sm-9.col-md-10.col-lg-1")
# #article > div:nth-child(6) > div:nth-child(2) > div.fomc-meeting__date.col-xs-4.col-sm-9.col-md-10.col-lg-1

