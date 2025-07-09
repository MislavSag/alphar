library(fastverse)
library(finutils)
library(arrow)
library(ggplot2)
library(PerformanceAnalytics)
library(lubridate)
library(qlcal)
library(AzureStor)


# Setup
calendars
setCalendar("UnitedStates/NYSE")

# Import fundamentals data
fundamentals = read_parquet("F:/data/equity/us/predictors_daily/factors/fundamental_factors.parquet")
fundamentals[, 1:10]
fundamentals[, month := data.table::yearmon(acceptedDate)]

# Downsample to daily
colnames(fundamentals)
cols = colnames(fundamentals)[which(colnames(fundamentals) == "cashAndCashEquivalents"):(ncol(fundamentals) - 1)]
fundm = fundamentals[, data.table::last(.SD), .SDcols = cols, by = .(symbol, month)]

# Checks
dim(fundamentals)
dim(fundm)

# Select columns we need
colnames(fundm)[grepl("cash", colnames(fundm), ignore.case = TRUE)]
colnames(fundm)[grepl("eps", colnames(fundm), ignore.case = TRUE)]
fundss = fundm[, .(symbol, month, cashPerShare, EPSDiluted)]
fundss = fundss[month >= 1998]

# Add same data for every month using locf
months_ = fundss[, sort(unique(month))]
symbols_ =  fundss[, sort(unique(symbol))]
dt = CJ(symbol = symbols_, month = months_)
dt = fundss[dt, on = .(symbol, month)]
setnafill(dt, type = "locf", cols = "cashPerShare")
dt = na.omit(dt)
dt = dt[cashPerShare > 0]
dt = dt[EPSDiluted > 0]

# Import prices
prices = qc_daily_parquet(
  file_path = "F:/lean/data/all_stocks_daily",
  duplicates = "fast"
)
prices[, month := data.table::yearmon(date)]

# Create FMP cloud
prices[, symbol_fmp := toupper(symbol)]
prices[, symbol_fmp := gsub("\\..*", "", symbol_fmp)]

# Duplicated symbol
anyDuplicated(prices, by = c("symbol_fmp", "date"))
symbol_dups = prices[duplicated(prices, by = c("symbol_fmp", "date")) | duplicated(prices, by = c("symbol_fmp", "date"), fromLast = TRUE), symbol]
symbol_dups = unique(symbol_dups)
length(symbol_dups) / prices[, length(unique(symbol_fmp))]
prices = prices[symbol %notin% symbol_dups]

# Downsample prices
pricesm = prices[, .(
  open = data.table::first(open),
  close = data.table::last(close),
  close_raw = data.table::last(close_raw),
  volume = sum(volume)
), by = .(symbol_fmp, month)]

# Merge QC prices and FMP fundamentals
dt = pricesm[dt, on = .(symbol_fmp = symbol, month)]

# Create target variable
dt[, target := close / open - 1, by = symbol_fmp]
setorder(dt, symbol_fmp, month)
dt[, target := shift(target, 1, type = "lead"), by = symbol_fmp]

# Remove rows with NA target
dt = na.omit(dt, cols = "target")


# STRATEGY ----------------------------------------------------------------
# Bars
dt[, cash_premium := cashPerShare - close_raw]
dt[, cash_premium_bin := dplyr::ntile(cash_premium, 10)]
na.omit(dt)[, .(ret = mean(target, na.rm = TRUE)), by = cash_premium_bin] |>
  ggplot(aes(x = cash_premium_bin, y = ret)) +
  geom_col() +
  labs(title = "Cash Premium vs Target Return", x = "Cash Premium Bin", y = "Average Target Return") +
  theme_minimal()
na.omit(dt)[, .(ret = median(target, na.rm = TRUE)), by = cash_premium_bin] |>
  ggplot(aes(x = cash_premium_bin, y = ret)) +
  geom_col() +
  labs(title = "Cash Premium vs Target Return", x = "Cash Premium Bin", y = "Average Target Return") +
  theme_minimal()

# Simple backtset strategy
setorder(dt, month, cash_premium)
cheap = na.omit(dt)
cheap = cheap[cash_premium < 0.1]
cheap = cheap[close_raw %between% c(0.5, 2) & volume > 100000]
cheap = cheap[, tail(.SD, 30), by = month]
cheap[, weights := 1 / length(target), by = month]
portfolio = cheap[, .(ret = sum(target * weights, na.rm = TRUE)), by = month]
setorder(portfolio, month)
portfolio_xts = as.xts.data.table(portfolio[, .(month = zoo::as.yearmon(month), ret)])
charts.PerformanceSummary(portfolio_xts, main = "Cash Premium Strategy Performance")
# charts.PerformanceSummary(portfolio_xts["2021/"], main = "Cash Premium Strategy Performance")
SharpeRatio.annualized(portfolio_xts, Rf = 0.0, scale = 12)
Return.annualized(portfolio_xts, scale = 12)
min(Drawdowns(portfolio_xts))


# SAVE FOR QC BACKTEST ----------------------------------------------------
# extract dtaa for best clustering method
qc_data = cheap[, .(
  month  = ceiling_date(as.Date(zoo::as.yearmon(month)), unit = "month") - 1,
  symbol = tolower(symbol_fmp)
)]
qc_data[, month := as.Date(vapply(month, advanceDate, FUN.VALUE = Date(1L)))]
qc_data = qc_data[, .(symbol = paste0(symbol, collapse = "|")), by = month]
setorder(qc_data, "month")
qc_data[, month := as.character(month)]
endpoint = storage_endpoint(Sys.getenv("BLOB-ENDPOINT-SNP"),
                            key=Sys.getenv("BLOB-KEY-SNP"))
cont = storage_container(endpoint, "qc-backtest")
storage_write_csv(qc_data, cont, "cash.csv", col_names = FALSE)
