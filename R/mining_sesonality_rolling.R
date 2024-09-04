library(data.table)
library(quantreg)
library(qlcal)
library(lubridate)
library(AzureStor)


# SET UP ------------------------------------------------------------------
# global vars
PATH = "F:/data/equity/us"

# Set calendar
calendars
setCalendar("UnitedStates/NYSE")


# PRICE DATA --------------------------------------------------------------
# Import QC daily data
prices = fread("F:/lean/data/stocks_daily.csv")
setnames(prices, gsub(" ", "_", c(tolower(colnames(prices)))))

# Remove duplicates
prices = unique(prices, by = c("symbol", "date"))

# Remove duplicates - there are same for different symbols (eg. phun and phun.1)
dups = prices[, .(symbol , n = .N),
              by = .(date, open, high, low, close, volume, adj_close,
                     symbol_first = substr(symbol, 1, 1))]
dups = dups[n > 1]
dups[, symbol_short := gsub("\\.\\d$", "", symbol)]
symbols_remove = dups[, .(symbol, n = .N),
                      by = .(date, open, high, low, close, volume, adj_close,
                             symbol_short)]
symbols_remove[n >= 2, unique(symbol)]
symbols_remove = symbols_remove[n >= 2, unique(symbol)]
symbols_remove = symbols_remove[grepl("\\.", symbols_remove)]
prices = prices[symbol %notin% symbols_remove]

# Adjust all columns
prices[, adj_rate := adj_close / close]
prices[, let(
  open = open*adj_rate,
  high = high*adj_rate,
  low = low*adj_rate
)]
setnames(prices, "close", "close_raw")
setnames(prices, "adj_close", "close")
prices[, let(adj_rate = NULL)]
setcolorder(prices, c("symbol", "date", "open", "high", "low", "close", "volume"))

# Remove observations where open, high, low, close columns are below 1e-008
# This step is opional, we need it if we will use finfeatures package
prices = prices[open > 1e-008 & high > 1e-008 & low > 1e-008 & close > 1e-008]

# Remove missing values
prices = na.omit(prices)

# Keep only symbol with at least 2 years of data
# This step is optional
symbol_keep = prices[, .N, symbol][N >= 2 * 252, symbol]
prices = prices[symbol %chin% symbol_keep]

# Sort
setorder(prices, symbol, date)

# save SPY for later and keep only events symbols
spy = prices[symbol == "spy"]

# free memory
gc()


# PREPARE DATA FOR SEASONALITY ANALYSIS -----------------------------------
# Calculate return
prices[, returns := close / shift(close) - 1, by = symbol] # calculate returns

# Remove outliers
nrow(prices[returns > 1]) / nrow(prices)
prices = prices[returns < 1] # TODO:: better outlier detection mechanism. For now, remove daily returns above 100%

# define target variables
prices[, return_day := shift(close, 1, type = "lead") / close - 1, by = symbol]
prices[, return_day3 := shift(close, 3, type = "lead") / close - 1, by = symbol]
prices[, return_week := shift(close, 5, type = "lead") / close - 1, by = symbol]
prices[, return_week2 := shift(close, 10, type = "lead") / close - 1, by = symbol]

# define frequency unit
prices[, yearmonthid := yearmon(date)]
prices[, day_of_month := 1:.N, by = .(symbol, yearmonthid)]
prices[, day_of_month := as.factor(day_of_month)]

# Remove missing values and select columns we need
dt = na.omit(prices, cols = c("symbol", "return_week2", "day_of_month"))

# Structure of dates
dt[, .N, by = day_of_month] # we probbably want to turn 23 to 22
dt[day_of_month == 23, day_of_month := 22] # 23 day to 22 day
dt[day_of_month == 22, day_of_month := 21] # not sure about this but lets fo with it

# Remove symbols with  less than 750 observations (3 years of data)
symbols_keep = dt[, .N, by = symbol]
symbols_keep = symbols_keep[N >= 750, symbol]
dt = dt[symbol %in% symbols_keep]


# SEASONALITY MINING ------------------------------------------------------
# define all year-months and start year
yearmonthids = dt[, sort(unique(yearmonthid))]
end_dates = seq.Date(as.Date("2019-01-01"), dt[, max(date)], by = "month")
end_dates = as.IDate(end_dates)

# Remove symbols inactive before first date. This can  produce survivorship bias,
# but we will be faster. If this doesn't work, it want work with all data fro sure.
symbols_keep = dt[, (end_dates[1] - max(date)) < 7, by = symbol] # we must have at least 7 days of data
dt = dt[symbol %in% symbols_keep[V1 == TRUE, symbol]]

# Get coeffs from summary of quantile regression
get_coeffs = function(df, y = "return_week") {
  res = rq(as.formula(paste0(y, " ~ day_of_month")), data = as.data.frame(df))
  summary_fit = summary.rq(res, se = 'nid')
  as.data.table(summary_fit$coefficients, keep.rownames = TRUE)
}

# Sample data - this is just for test
dt_sample = dt[symbol %in% dt[, sample(unique(symbol), 10)]]

# get median regression coefficients - experiment
sample_size_days = 2520
seasonality_results = dt[, lapply(end_dates, function(date_) {
  if ((date_ - max(date)) > 7) return(list(NA))
  # print(date_ - sample_size_days)
  # get_coeffs(.SD[yearmonthid %between% c(yearmon(y - sample_size_days), yearmon(y))])
  tryCatch(list(get_coeffs(.SD[yearmonthid %between% c(yearmon(date_ - sample_size_days), yearmon(date_))])),
           error = function(e) list(NA))
  }), by = symbol]
cols = paste0("month", strftime(end_dates, format = "%y%m%d"))
colnames(seasonality_results)[2:length(colnames(seasonality_results))] = cols

# save
time = strftime(Sys.time(), "%Y%m%d%H%M%S")
saveRDS(seasonality_results, file.path("D:/features", paste0("seasonality-week", time, ".rds")))

# Import data


# INSPECT RESULTS ---------------------------------------------------------
# seasonality_results[1, month190101]


# CREATE PORTFOLIOS -------------------------------------------------------
# Portfolio 1 - keep min Pr for every symbol
portfolios_l = list()
for (i in seq_along(cols)) {

  # sample
  col = cols[i]
  cols_ = c("symbol", col)
  x = seasonality_results[, ..cols_]

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
  x = seasonality_results[, ..cols_]

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
  x = seasonality_results[, ..cols_]

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
