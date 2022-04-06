library(data.table)
library(fmpcloudr)
library(httr)
library(zip)
library(rvest)
library(stringr)
library(mrisk)
library(fasttime)
library(lubridate)
library(RcppQuantuccia)



# set up
API_KEY = Sys.getenv("APIKEY-FMPCLOUD")
fmpc_set_token(API_KEY)

# parameters
setCalendar("UnitedStates::NYSE")


# get sp 500 stocks
SP500 = fmpc_symbols_index()
SP500_DELISTED <- content(GET(paste0('https://financialmodelingprep.com/api/v3/historical/sp500_constituent?apikey=', API_KEY)))
SP500_DELISTED <- rbindlist(SP500_DELISTED)
SP500_SYMBOLS <- unique(c(SP500$symbol, SP500_DELISTED$symbol))
SP500_SYMBOLS <- c("SPY", SP500_SYMBOLS)

# get ticker changes
get_ticker_changes <- function(ticker) {

  p <- RETRY("POST",
        'https://www.quantumonline.com/search.cfm',
        body = list(
          tickersymbol = ticker,
          sopt = 'symbol',
          '1.0.1' = 'Search'
        ),
        times = 8L)
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
get_market_equities <- function(symbol, multiply = 1, time = 'minute', from = as.character(Sys.Date() - 3),
                                to = as.character(Sys.Date())) {

  x <- tryCatch({
    GET(paste0('https://financialmodelingprep.com/api/v4/historical-price/',
               symbol, '/', multiply, '/', time, '/', from, '/', to),
        query = list(apikey = API_KEY),
        timeout(100))
    }, error = function(e) NA)

  # control error
  tries <- 0
  while (is.na(x) & tries < 20) {
    print("There is an error in scraping market data. Sleep and try again!")
    Sys.sleep(60L)
    x <- tryCatch({
      GET(paste0('https://financialmodelingprep.com/api/v4/historical-price/',
                 symbol, '/', multiply, '/', time, '/', from, '/', to),
          query = list(apikey = API_KEY),
          timeout(100))
    }, error = function(e) NA)
    tries <- tries + 1
  }

  # check if status is ok. If not, try to download again
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
               times = 5,
               timeout(100))
    if (x$status_code == 200) {
      x <- content(x)
      return(rbindlist(x$results))
    } else {
      stop('Error in reposne. Status not 200 and not 404')
    }
  }
}

# get changes
SP500_CHANGES <- lapply(SP500_SYMBOLS, get_ticker_changes)
SP500_CHANGES <- rbindlist(SP500_CHANGES)
SP500_SYMBOLS <- unique(c(SP500_SYMBOLS, SP500_CHANGES$ticker_change))

# get data for symbols
save_market_data <- function(symbols, save_path = 'D:/market_data/equity/usa/minute') {
  dates_seq <- seq.Date(as.Date('2004-01-01'), Sys.Date() - 1, by = 1)
  for (symbol in symbols) {
    print(symbol)
    symbol_path <- file.path(save_path, tolower(symbol))

    # crat path if it doesnt exist, instead get already scraped
    if (!dir.exists(symbol_path)) {
      dir.create(symbol_path)
    } else {
      scraped_dates <- list.files(symbol_path)
      scraped_dates <- gsub("_.*", "", scraped_dates)
      scraped_dates <- as.Date(scraped_dates, "%Y%m%d")
      new_dates <- as.Date(setdiff(dates_seq, scraped_dates), origin = "1970-01-01")
      dates_seq <- new_dates[isBusinessDay(new_dates)]
    }

    # scrap loop
    for (i in seq_along(dates_seq)) {
      day_minute <- get_market_equities(symbol, from = dates_seq[i], to = dates_seq[i])
      if (is.null(day_minute)) {
        next()
      }
      day_minute <- unique(day_minute)
      day_minute[, formated := as.POSIXct(formated)]
      day_minute <- day_minute[order(formated)]
      date_ <- gsub("-", "", as.character(as.Date(day_minute$formated[1])))
      day_minute[, `:=`(DateTime = (hour(formated) * 3600 + minute(formated) * 60 + second(formated)) * 1000,
                        Open = o * 10000,
                        High = h * 10000,
                        Low = l * 10000,
                        Close = c * 10000,
                        Volume = v)]
      day_minute <- day_minute[, .(DateTime, Open, High, Low, Close, Volume)]

      # save
      file_name <- file.path(symbol_path, paste0(date_, "_", tolower(symbol), "_minute_trade.csv"))
      zip_file_name <- file.path(symbol_path, paste0(date_, "_trade.zip"))
      fwrite(day_minute, file_name, col.names = FALSE)
      zipr(zip_file_name, file_name)
      file.remove(file_name)
    }
  }
}

############# DELTE NEW TO REPLACE WITH TRUE #################
# delete all new data
# files <- list.files('D:/market_data/equity/usa/minute', full.names = TRUE)
# dates_delete <- format.Date(seq.Date(as.Date("2021-03-01"), Sys.Date(), 1), format = "%Y%m%d")
# dates_delete <- paste0(dates_delete, "_trade.zip")
# for (f in files) {
#   files_date <- list.files(f, full.names = TRUE)
#   files_date <- files_date[gsub(".+/", "", files_date) %in% dates_delete]
#   file.remove(files_date)
# }
############# DELTE NEW TO REPLACE WITH TRUE #################

# get and save market unadjusted data
save_market_data(SP500_SYMBOLS, save_path = 'D:/market_data/equity/usa/minute')
