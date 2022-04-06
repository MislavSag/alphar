library(data.table)
library(fmpcloudr)
library(httr)
library(zip)
library(rvest)
library(stringr)
library(equityData)


# set fmpcloudr api token
API_KEY = "15cd5d0adf4bc6805a724b4417bbaafc"
fmpc_set_token(API_KEY)

# get sp 500 stocks
SP500 = fmpc_symbols_index()
SP500_DELISTED <- content(GET(paste0('https://financialmodelingprep.com/api/v3/historical/sp500_constituent?apikey=', API_KEY)))
SP500_DELISTED <- rbindlist(SP500_DELISTED)
SP500_SYMBOLS <- unique(c(SP500$symbol, SP500_DELISTED$symbol))
SP500_SYMBOLS <- c("SPY", SP500_SYMBOLS)

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

# get changes
SP500_CHANGES <- lapply(SP500_SYMBOLS, get_ticker_changes)
SP500_CHANGES <- rbindlist(SP500_CHANGES)
SP500_SYMBOLS <- unique(c(SP500_SYMBOLS, SP500_CHANGES$ticker_change))

# get data for symbols
for (symbol in SP500_SYMBOLS) {
  print(symbol)

  # get data
  data_by_symbol <- get_fmpcloud_data(symbol, local = "D:/market_data/equity/usa/hour")

  # if empty continue
  if (is.null(data_by_symbol)) {
    # save to blob
    file_name <- paste0(tolower(symbol), ".csv")
    save_blob_files(data.frame(), file_name, container = "equity-usa-hour")
  } else {
    # save to blob
    file_name <- paste0(tolower(symbol), ".csv")
    save_blob_files(data_by_symbol, file_name, container = "equity-usa-hour")
  }
}


# LAST DATE SHOULD BE YESTERDAY, BECAUSE IT CAN RETRIEVE PART OF TODAY< CHECK !< DELETE NEW DATES
