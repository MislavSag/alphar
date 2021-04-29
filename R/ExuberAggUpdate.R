library(data.table)
library(exuber)
library(httr)
library(future.apply)
library(parallel)
library(lubridate)
library(fmpcloudr)
source('C:/Users/Mislav/Documents/GitHub/alphar/R/import_data.R')


# set api token
APIKEY = "15cd5d0adf4bc6805a724b4417bbaafc"
fmpc_set_token(APIKEY, noBulkWarn = TRUE)
base_url = "https://financialmodelingprep.com/api/v4"

# parameters
exuber_length <- 700


get_market_data <- function(symbol, multiply, time, from, to, api_tag = "/historical-price/") {
  q = paste(symbol, multiply, time, from, to, sep = "/")
  url <- paste0(base_url, api_tag, q)
  req <- GET(url, query = list(apikey = APIKEY))
  if ("error" %in% names(content(req))) {
    if (content(req)$error == "Not found! Please check the date and symbol.") {
      return(NULL)
    }
  } else if (req$status_code == 404) {
    req <- RETRY("GET", url, query = list(apikey = APIKEY), times = 5)
  }
  sample <- content(req)
  prices <- rbindlist(sample$results)
  return(prices)
}

get_ny_time <- function() {
  s <- Sys.time()
  s <- .POSIXct(s, "EST")
  return(s)
}


# sp500 symbols
sp500_stocks_url <- paste0("https://financialmodelingprep.com/api/v3/sp500_constituent?apikey=", APIKEY)
sp500_stocks <- content(GET(sp500_stocks_url))
sp500_stocks <- lapply(sp500_stocks, as.data.table)
sp500_stocks <- rbindlist(sp500_stocks)
symbols <- sp500_stocks$symbol


closing_time <- as.POSIXct(paste0(Sys.Date(), "16:00:00"), tz = "EST")

repeat {
  s <- get_ny_time()
  print(s)
  next_hour <- ceiling_date(s, "hour")
  if (s >= next_hour) {
    print("Exuber calculate")
    for (symbol in symbols) {
      current_market_data <- get_market_data(symbol = "AAPL",
                      multiply = 1,
                      time = "hour",
                      from, to,
                      api_tag = "/historical-price-adjusted/")
    }
  }
  if (s > closing_time) {
    print("Market is closed")
    break
  }
  Sys.sleep(3L)
}




start_time <- Sys.time()
x <- fmpc_price_intraday('AAPL', freq = '1hour', startDate = Sys.Date(), endDate = Sys.Date())
end_time <- Sys.time()
end_time - start_time


# import exuber data
exuber_data <- lapply(list.files("D:/risks/radf", full.names = TRUE), fread)
exuber_data <- rbindlist(exuber_data)
last_date <- max(exuber_data$datetime)



sample <- sp500_stocks[symbol == "AAPL"]


sp500_stocks <- sp500_stocks[symbol %in% c("SPY", sp500_symbols)]
sp500_stocks_daily <- import_daily("D:/market_data/equity/usa/day/trades", "csv")

# calculate returns
sp500_stocks[, returns := (close / shift(close)) - 1, by = .(symbol)]
sp500_stocks <- na.omit(sp500_stocks)
spy <- sp500_stocks[symbol == "SPY"]
sp500_stocks <- sp500_stocks[symbol != "SPY"]
sp500_stocks <- sp500_stocks[, .(symbol, datetime, close, returns)]

# remove outliers
sp500_stocks <- sp500_stocks[abs(returns) < 0.48]

# prepare data
sp500_stocks <- sp500_stocks[sp500_stocks[, .N, by = .(symbol)][N > 2000], on = "symbol"] # stock have at least 1 year of data
