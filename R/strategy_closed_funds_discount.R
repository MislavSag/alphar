library(httr)
library(data.table)
library(finutils)
library(ggplot2)
library(PerformanceAnalytics)
library(lubridate)
library(qlcal)
library(AzureStor)
library(rvest)


# Utils
ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36 Edg/137.0.0.0"
DIRSAVE = "F:/strategies/closed_funds"
if (!dir.exists(DIRSAVE)) {
  dir.create(DIRSAVE, recursive = TRUE)
}
calendars
setCalendar("UnitedStates/NYSE")

# Get symbols
url = "https://www.cefconnect.com/api/v3/DailyPricing?props=Ticker,Name,DistributionRateNAV,LastUpdated,Discount,DistributionRatePrice,ReturnOnNAV,CategoryId,CategoryName,IsManagedDistribution,Price,PriceChange,NAV,NAVPublished,Cusip/&_=1750440041058"
p = GET(url, add_headers("User-Agent" = ua))
closed_funds = content(p)
closed_funds = lapply(closed_funds, as.data.table)
closed_funds = rbindlist(closed_funds, fill = TRUE)

# Get prices for all symbols
dates = seq.Date(as.Date("1998-01-01"), Sys.Date(), by = "month")
dates = format(dates, "%m-%d-%Y")
DIRSAVE_PRICES = file.path(DIRSAVE, "prices")
if (!dir.exists(DIRSAVE_PRICES)) {
  dir.create(DIRSAVE_PRICES, recursive = TRUE)
}
for (ticker in closed_funds$Ticker) {
  # ticker = "CIF"
  print(ticker)
  file_name = file.path(DIRSAVE_PRICES, paste0(ticker, ".csv"))
  if (file.exists(file_name)) {
    print("File already exists, skipping...")
    next
  }

  # Check if ticker is valid# Create uls
  urls = paste0(
    "https://www.cefconnect.com/api/v3/PriceHistory/",
    ticker,
    "/1M/",
    dates
  )
  # Get dat afor all urls
  prices = lapply(urls, function(x) {
    # x = urls[1]
    Sys.sleep(0.3)
    p = RETRY("GET", x, add_headers("User-Agent" = ua))
    dt = content(p)
    dt = lapply(dt$Data, as.data.table)
    dt = rbindlist(dt)
    return(dt)
  })
  dt = rbindlist(prices)
  if (nrow(dt) == 0) {
    fwrite(data.table(), file_name)
    print("No data found for this ticker, skipping...")
    next
  }
  dt = unique(dt)
  dt[, date := as.Date(DataDateJs, "%Y/%m/%d")]
  setorder(dt, date)
  dt[, unique(date)]

  # save
  fwrite(dt, file_name)
}

# Get asset allocations
for (ticker in closed_funds$Ticker) {
  #
  url_ = paste0("https://www.cefconnect.com/fund/", ticker)
  p = RETRY("GET", url_, add_headers("User-Agent" = ua))
  fees = content(p)
  fees |>
    html_elements("table") |>
    html_table()
  "https://www.cefconnect.com/api/v3/assetallocation/EVF"
}



#
url_ = paste0("https://www.cefconnect.com/fund/", ticker)
p = RETRY("GET", url_, add_headers("User-Agent" = ua))
fees = content(p)
fees |>
  html_elements("table") |>
  html_table()



# Import all data
files = list.files(DIRSAVE, full.names = TRUE)
funds = lapply(files, fread)
names(funds) = gsub(".*/|.csv", "", files)
funds = rbindlist(funds, fill = TRUE, idcol = "ticker")
funds[, ticker := tolower(ticker)]
setnames(funds, c("NAVData", "DiscountData", "Data"), c("nav", "discount", "close"))
funds[ticker == "aef"]

# Downsample to monthly
fundsm = funds[, .(
  nav = data.table::last(nav),
  discount = data.table::last(discount),
  close = data.table::last(close)
), by = .(ticker, month = data.table::yearmon(date))]
setorder(fundsm, ticker, month)
fundsm[, target := shift(close, 1, type = "lead") / close - 1, by = ticker]
fundsm = na.omit(fundsm)

# Add metadata
fundsm = closed_funds[, .(ticker = tolower(Ticker), category = CategoryName)][fundsm, on = "ticker"]
fundsm = fundsm[category == "Equity-U.S. Equity"]

# Summary
fundsm[, length(unique(ticker))]
fundsm[, .N, by = month] |>
  ggplot(aes(month, N)) +
  geom_line() +
  labs(title = "Number of closed funds with prices per date",
       x = "Date",
       y = "Number of funds") +
  theme_minimal()
fundsm[, .N, by = .(month, discount < 0)][order(month)]  |>
  ggplot(aes(month, N, color = as.factor(discount))) +
  geom_line()

# Filter period
fundsm = fundsm[month >= 2005]

# Return for deciles of discount and premium
fundsm[, mean(target), by = .(bin = dplyr::ntile(discount, 10))][order(bin)] |>
  ggplot(aes(bin, V1)) +
  geom_col() +
  labs(title = "Mean return for deciles of discount",
       x = "Decile of discount",
       y = "Mean return") +
  theme_minimal()
fundsm[, median(target), by = .(bin = dplyr::ntile(discount, 10))][order(bin)] |>
  ggplot(aes(bin, V1)) +
  geom_col() +
  labs(title = "Mean return for deciles of discount",
       x = "Decile of discount",
       y = "Mean return") +
  theme_minimal()

# Long-short backtest
setorder(fundsm, month, discount)
portfolio_long = fundsm[, head(.SD, 3), by = month]
portfolio_short = fundsm[, tail(.SD, 3), by = month]
portfolio_long[, weight := 1 / .N, by = month]
portfolio_short[, weight := -1 / .N, by = month]
portfolio = rbind(portfolio_long, portfolio_short)
# portfolio = portfolio[weight > 0]
portfolio_results = portfolio[, .(ret = sum(weight * target)), by = .(month)]
portfolio_xts = as.xts.data.table(portfolio_results[, .(date = zoo::as.yearmon(month), ret)])
charts.PerformanceSummary(portfolio_xts)
SharpeRatio.annualized(portfolio_xts, scale = 12)
# charts.PerformanceSummary(portfolio_xts["2020-01-01/"])

# Save for quantconnect
qc_data = portfolio[, .(ticker, month = as.Date(zoo::as.yearmon(month)), side = fifelse(weight > 0, "long", "short"))]
qc_data[, month := ceiling_date(month, "month") - 1]
qc_data = dcast(
  qc_data,
  month ~ side,
  value.var  = "ticker",
  fun.aggregate = \(x) paste(x, collapse = "|"),
  fill = NA_character_
)
setorder(qc_data, month)
qc_data[, month := as.Date(vapply(month, advanceDate, FUN.VALUE = Date(1L)))]
qc_data[, month := as.character(month)]
endpoint = storage_endpoint(Sys.getenv("BLOB-ENDPOINT-SNP"),
                            key=Sys.getenv("BLOB-KEY-SNP"))
cont = storage_container(endpoint, "qc-backtest")
storage_write_csv(qc_data, cont, "closed_funds.csv", col_names = FALSE)


# ROBUSTNESS --------------------------------------------------------------
# Import daily prices
symbols = fundsm[, unique(ticker)]
prices = qc_daily_parquet(
  file_path = "F:/lean/data/all_stocks_daily",
  duplicates  = "fast",
  add_dv_rank = FALSE,
  symbols = symbols
)

# Check symbols
sum(symbols %in% prices[, unique(symbol)]) / length(symbols) * 100
symbols[symbols %notin% prices[, unique(symbol)]]

# Downsample prices
pricesm = prices[, .(
  closep = data.table::last(close),
  closep_raw = data.table::last(close_raw)
), by = .(symbol, month = data.table::yearmon(date))]

# Merge prices and fundsm
dt = pricesm[fundsm, on = .(symbol = ticker, month)]
setorder(dt, symbol, month)
dt[, target := shift(closep, 1, type = "lead") / closep - 1, by = symbol]
dt = na.omit(dt)

# Check diff in prices
dt[abs(((close - closep_raw) / closep) * 100) > 100]

# Summary
dt[, length(unique(ticker))]
dt[, .N, by = month] |>
  ggplot(aes(month, N)) +
  geom_line() +
  labs(title = "Number of closed funds with prices per date",
       x = "Date",
       y = "Number of funds") +
  theme_minimal()
dt[, .N, by = .(month, discount < 0)][order(month)]  |>
  ggplot(aes(month, N, color = as.factor(discount))) +
  geom_line()

# Filter period
fundsm = fundsm[month >= 2005]

# Return for deciles of discount and premium
dt[, mean(target), by = .(bin = dplyr::ntile(discount, 10))][order(bin)] |>
  ggplot(aes(bin, V1)) +
  geom_col() +
  labs(title = "Mean return for deciles of discount",
       x = "Decile of discount",
       y = "Mean return") +
  theme_minimal()
dt[, median(target), by = .(bin = dplyr::ntile(discount, 10))][order(bin)] |>
  ggplot(aes(bin, V1)) +
  geom_col() +
  labs(title = "Mean return for deciles of discount",
       x = "Decile of discount",
       y = "Mean return") +
  theme_minimal()

# Long-short backtest
setorder(dt, month, discount)
portfolio_long = dt[, head(.SD, 3), by = month]
portfolio_short = dt[, tail(.SD, 3), by = month]
portfolio_long[, weight := 1 / .N, by = month]
portfolio_short[, weight := -1 / .N, by = month]
portfolio = rbind(portfolio_long, portfolio_short)
# portfolio = portfolio[weight < 0]
portfolio_results = portfolio[, .(ret = sum(weight * target)), by = .(month)]
portfolio_xts = as.xts.data.table(portfolio_results[, .(date = zoo::as.yearmon(month), ret)])
charts.PerformanceSummary(portfolio_xts)
SharpeRatio.annualized(portfolio_xts, scale = 12)
# charts.PerformanceSummary(portfolio_xts["2020-01-01/"])

# Save for quantconnect
qc_data = portfolio[, .(symbol, month = as.Date(zoo::as.yearmon(month)), side = fifelse(weight > 0, "long", "short"))]
qc_data[, month := ceiling_date(month, "month") - 1]
qc_data = dcast(
  qc_data,
  month ~ side,
  value.var  = "symbol",
  fun.aggregate = \(x) paste(x, collapse = "|"),
  fill = NA_character_
)
setorder(qc_data, month)
qc_data[, month := as.Date(vapply(month, advanceDate, FUN.VALUE = Date(1L)))]
qc_data[, month := as.character(month)]
endpoint = storage_endpoint(Sys.getenv("BLOB-ENDPOINT-SNP"),
                            key=Sys.getenv("BLOB-KEY-SNP"))
cont = storage_container(endpoint, "qc-backtest")
storage_write_csv(qc_data, cont, "closed_funds.csv", col_names = FALSE)

# Compare QC and local
portfolio_long[month >= 2020][month <= 2020.5]

