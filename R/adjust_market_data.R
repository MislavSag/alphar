library(data.table)
library(httr)
library(rvest)
library(stringr)
library(quantmod)
library(fmpcloudr)


# GLOBALS
API_KEY = '15cd5d0adf4bc6805a724b4417bbaafc'
fmpc_set_token(API_KEY)

# get sp 500 stocks
SP500 = fmpc_symbols_index()
SP500_DELISTED <- content(GET(paste0('https://financialmodelingprep.com/api/v3/historical/sp500_constituent?apikey=', API_KEY)))
SP500_DELISTED <- rbindlist(SP500_DELISTED)
SP500_SYMBOLS <- unique(c(SP500$symbol, SP500_DELISTED$symbol))

# get market data
market_data <- import_local_data("D:/market_data/equity/usa/hour/trades")
market_data_daily <- market_data[, .SD[.N], by = .(symbol, date = as.Date(datetime))]
symbols <- unique(market_data$symbol)

# get ticker changes
get_ticker_changes <- function(ticker) {
  p <- POST('https://www.quantumonline.com/search.cfm',
            body = list(
              tickersymbol = ticker,
              sopt = 'symbol',
              '1.0.1' = 'Search'
            ))
  changes <- content(p) %>%
    html_elements(xpath = "//*[contains(text(),'Previous Ticker')]") %>%
    html_text() %>%
    gsub('.*Symbol:', '', .) %>%
    trimws(.)
  date <- as.Date(str_extract(changes, '\\d+/\\d+/\\d+'), '%m/%d/%Y')
  tickers <- str_extract(changes, '\\w+')
  changes <- data.table(ticker = ticker, date = date, ticker_change = tickers)
  return(changes)
}

# get stock splits from web page https://www.stocksplithistory.com/
get_stock_split <- function(ticker) {
  splits <- content(GET("https://www.splithistory.com/", query = list(symbol = ticker))) %>%
    html_nodes(xpath = "//table[@width='208' and @style='font-family: Arial; font-size: 12px']") %>%
    html_table(header = TRUE) %>%
    .[[1]]
  splits$date <- as.Date(splits$Date, "%m/%d/%Y")
  ratio1 <- as.numeric(str_extract(splits$Ratio, "\\d+"))
  ratio2 <- as.numeric(str_extract(splits$Ratio, "\\d+$"))
  splits$ratio <- ratio2 / ratio1
  splits <- splits[splits$date > as.Date("1998-01-01"), ]
  return(as.data.table(splits[, c('date', 'ratio')]))
}

# check differences in stock splits
check_split_consinstency <- function(ticker) {
  splits <- fmpc_security_splits(ticker, startDate = '1998-01-01')
  splits <- data.table(date = splits$date, ratio = splits$denominator / splits$numerator)
  splits_yahoo <- tryCatch(getSplits(ticker, from = "1998-01-01"), error = function(e) e$message)
  splits_yahoo <- as.data.table(splits_yahoo)
  splits_web <- get_stock_split(ticker)
  splits_web <- splits_web[order(date)]
  if (nrow(splits) == 0 & all(is.na(splits_yahoo)) & nrow(splits_web) == 0) {
    return(NA)
  } else if (all(round(splits$ratio, 2) == round(splits_yahoo[, 2], 2)) &
             all(round(splits$ratio, 2) == round(splits_web$ratio, 2))) {
    return(splits)
  } else {
    return(0)
  }
}

# get stock split factor
get_stock_split_factor <- function(ticker) {
  splits <- check_split_consinstency(ticker)
  # splits <- splits[-nrow(splits), ] # TEST!!!!!!!!!!!!!!!!!!!!!
  if (is.na(splits)) {
    return(NA)
  } else if (splits == 0) {
    break(paste0('Check stock split for ', ticker))
  } else {
    splits$split_factor <- rev(cumprod(rev(splits$ratio)))
  }
  return(splits[, .(date, split_factor)])
}

# adjust data for stock splits
adjust_stock_splits <- function(ticker, ohlcv) {
  # ohlcv <- data_by_symbol
  splits <- get_stock_split_factor(ticker)
  splits[, date := date -1]
  ohlcv[, date := as.Date(formated)]
  ohlcv <- splits[ohlcv, on = 'date', roll = -Inf]
  # ohlcv[, split_factor := na.locf(split_factor, fromLast = TRUE, na.rm = FALSE)]
  ohlcv[is.na(split_factor), split_factor := 1]
  cols <- colnames(ohlcv)[4:8]
  ohlcv[, (cols) := lapply(.SD, function(x) x * split_factor), .SDcols = cols]
  ohlcv[, v := as.integer(v)]
  return(ohlcv)
}


# get changes
SP500_CHANGES <- lapply(SP500_SYMBOLS, get_ticker_chanes)

# adjust for stock splits
split_data_check <- lapply(symbols, function(x) {
  print(x)
  check_split_consinstency(x)
})

# generate factor file
ticker <- "AAPL" # "AAPL"
df <- market_data_daily[symbol == ticker, .(date, close)]

# add splits data
splits <- get_stock_split_factor(ticker)
if (all(is.na(splits))) {
  df[, split_factor := NA]
} else {
  df <- splits[df, on = "date"]
}

# get dividends
dividends <- as.data.table(fmpc_security_dividends(ticker, startDate = '2004-01-01'))
dividends <- dividends[, dividend := ifelse(is.na(dividend) & adjDividend > 0, adjDividend, dividend)]
dividends <- dividends[, .(date, dividend)]
if (all(is.na(dividends))) {
  df[, dividend_factor := NA]
} else {
  df <- dividends[df, on = "date"]
}

# clean data
df[, `:=`(lag_close = shift(close),
          date = shift(date)) ]

# keep oly dividend ot split days
factor_file <- df[!is.na(dividend) & dividend != 0 | !is.na(split_factor), .(date, dividend, split_factor, lag_close)]
factor_file <- factor_file[date < as.Date("2018-05-15")] # TO COMPARE WITH QC
factor_file[, split_factor := na.locf(split_factor, rev = TRUE, na.rm = FALSE)]
factor_file <- factor_file[is.na(split_factor), split_factor := 1]
# factor_file[, dividend := na.locf(dividend, rev = TRUE, na.rm = FALSE)]

# add row to the end
factor_file <- rbind(factor_file, data.table(date = as.Date("2050-01-01"), dividend = NA, lag_close = 0, split_factor = 1))

# calculate price factor
price_factor <- vector("numeric", nrow(factor_file))
lag_close <- factor_file$lag_close
split_factor <- factor_file$split_factor
dividend <- factor_file$dividend
for (i in nrow(factor_file):1) {
  print(price_factor)
  if (i == nrow(factor_file)) {
    price_factor[i] <- (lag_close[i-1] - dividend[i-1]) / lag_close[i-1] * 1
  } else if (i == 1) {
    price_factor[i] <- NA
  } else if (is.na(dividend[i-1])) {
    price_factor[i] <- price_factor[i+1]
  } else {
    price_factor[i] <- ((lag_close[i-1] - dividend[i-1]) / lag_close[i-1]) * price_factor[i+1]
  }
}
price_factor <- c(price_factor[-1], NA)

# add factor to factor file
factor_file[, price_factor := price_factor]
factor_file[, date := format.Date(date, "%Y%m%d")]
factor_file <- factor_file[, .(date, price_factor, split_factor, lag_close)]
factor_file[10] # 0.9304792
factor_file[1] # 0.8893653

# save
write.csv(factor_file, "factor_file_spy.csv")



# comopare
spy_qc_spy <- fread("C:/Users/Mislav/Documents/GitHub/lean_test/data/equity/usa/factor_files/spy.csv")
nrow(spy_qc_spy)
nrow(factor_file)
