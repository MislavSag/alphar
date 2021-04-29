library(data.table)
library(fmpcloudr)
library(httr)
library(zip)


# set fmpcloudr api token
APIKEY = "15cd5d0adf4bc6805a724b4417bbaafc"
fmpc_set_token(APIKEY)

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

# function for getting market data
get_market_equities <- function(symbol, multiply = 1, time = 'hour', from = as.character(Sys.Date() - 7),
                                to = as.character(Sys.Date())) {

  x <- GET(paste0('https://financialmodelingprep.com/api/v4/historical-price/',
                  symbol, '/', multiply, '/', time, '/', from, '/', to),
           query = list(apikey = API_KEY))
  if (x$status_code == 404) {
    return(NULL)
  } else if (x$status_code == 200) {
    x <- content(x)
    return(rbindlist(x$results))
  } else {
    x <- RETRY("GET",
               paste0('https://financialmodelingprep.com/api/v4/historical-price/',
                      symbol, '/', multiply, '/', time, '/', from, '/', to),
               query = list(apikey = API_KEY),
               times = 5)
    if (x$status_code == 200) {
      x <- content(x)
      return(rbindlist(x$results))
    } else {
      stop('Error in reposne. Status not 200 and not 404')
    }
  }
}

# get changes
SP500_CHANGES <- lapply(SP500_SYMBOLS, get_ticker_chanes)
SP500_CHANGES <- rbindlist(SP500_CHANGES)
SP500_SYMBOLS <- unique(c(SP500_SYMBOLS, SP500_CHANGES$ticker_change))

# get data for symbols
save_market_data <- function(symbols, save_path = 'D:/market_data/equity/usa/hour/trades') {
  start_dates <- seq.Date(as.Date('2004-01-01'), Sys.Date() - 5, 5)
  end_dates <- start_dates + 5
  for (symbol in symbols) {
    print(symbol)
    data_slice <- list()
    for (i in seq_along(start_dates)) {
      data_slice[[i]] <- get_market_equities(symbol, from = start_dates[i], to = end_dates[i])
    }
    data_by_symbol <- rbindlist(data_slice)
    if (length(data_by_symbol) == 0) {
      print(paste0("No data for symbol ", symbol))
      next()
    } else {
      data_by_symbol <- unique(data_by_symbol)
      data_by_symbol[, formated := as.POSIXct(formated)]
      data_by_symbol <- data_by_symbol[order(formated)]
      data_by_symbol <- data_by_symbol[format(formated, "%H:%M:%S") %between% c("10:00:00", "15:00:00")]
      data_by_symbol[, `:=`(DateTime = format.POSIXct(as.POSIXct(formated), "%Y%m%d %H:%M"),
                            Open = o * 10000,
                            High = h * 10000,
                            Low = l * 10000,
                            Close = c * 10000,
                            Volume = v)]
      data_by_symbol <- data_by_symbol[, .(DateTime, Open, High, Low, Close, Volume)]
      file_name <- file.path(save_path, paste0(tolower(symbol), ".csv"))
      fwrite(data_by_symbol, file_name, col.names = FALSE)
      zip_file_name <- file.path(save_path, paste0(tolower(symbol), ".zip"))
      zipr(zip_file_name, file_name)
      file.remove(file_name)
    }
  }
}

# get and save market unadjusted data
save_path <- 'D:/market_data/equity/usa/hour/trades'
SP500_UNSRCRAPED <- setdiff(SP500_SYMBOLS, toupper(gsub(".csv", "", list.files(save_path))))
save_market_data(SP500_UNSRCRAPED, save_path = 'D:/market_data/equity/usa/hour/trades')
