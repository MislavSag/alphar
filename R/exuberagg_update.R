library(data.table)
library(exuber)
library(httr)
library(lubridate)
library(fmpcloudr)
library(leanr)


# set api token
APIKEY = "15cd5d0adf4bc6805a724b4417bbaafc"
fmpc_set_token(APIKEY, noBulkWarn = TRUE)
base_url = "https://financialmodelingprep.com/api/v4"

# parameters
exuber_length <- 100
frequency_in_minutes <- "5 mins" # hour minute 5 mins
min_w <- psy_minw(exuber_length)

# get NY current time
get_ny_time <- function() {
  s <- Sys.time()
  s <- .POSIXct(s, "America/New_York")
  return(s)
}

# function for calculating sadf
get_sadf <- function(...) {
  y <- radf(...)
  stats <- exuber::tidy(y)
  bsadf <- data.table::last(exuber::augment(y))[, 4:5]
  y <- cbind(stats, bsadf)
  y$id <- NULL
  y
}

# sp500 symbols
sp500_stocks_url <- paste0("https://financialmodelingprep.com/api/v3/sp500_constituent?apikey=", APIKEY)
sp500_stocks <- content(GET(sp500_stocks_url))
sp500_stocks <- lapply(sp500_stocks, as.data.table)
sp500_stocks <- rbindlist(sp500_stocks)
symbols <- sp500_stocks$symbol

# set closing time
closing_time <- as.POSIXct(paste0(Sys.Date(), "16:00:00"), tz = "EST")

# import and prepare market data for which we will calculate exuber aggregate indicator
market_data <- import_local_data("D:/market_data/equity/usa/hour/trades")
market_data <- market_data[symbol %in% symbols]
setorderv(market_data, c('symbol', 'datetime'))
keep_symbols <- market_data[, .N, by = symbol][N > exuber_length, symbol]
market_data <- market_data[symbol %in% keep_symbols]
market_data <- market_data[, tail(.SD, exuber_length), by = .(symbol)]
symbols <- unique(market_data$symbol)

# main function which calculates exuber aggregate indicator
s <- get_ny_time()
next_time_point <- ceiling_date(s, frequency_in_minutes)
repeat {
  s <- get_ny_time()
  print(s)
  if (s >= next_time_point) {
    print("Exuber calculate")
    # get current market data for all stocks
    current_market_data <- list()
    for (i in 1:length(symbols)) {
      x <- get_market_data(symbol = symbols[i],
                           multiply = 1,
                           time = "hour",
                           from = Sys.Date() - 1,
                           to = Sys.Date(),
                           api_tag = "/historical-price/")
      x$symbol <- symbols[i]
      current_market_data[[i]] <- x
    }
    current_market_data <- rbindlist(current_market_data)
    current_market_data[, formated := as.POSIXct(formated)]
    current_market_data <- current_market_data[, .(symbol, formated, o, h, l, c, v)]
    setnames(current_market_data, colnames(current_market_data), c("symbol", "datetime", "open", "high", "low", "close", "volume"))
    current_market_data <- rbind(market_data, current_market_data)
    setorderv(current_market_data, c('symbol', 'datetime'))
    current_market_data <- unique(current_market_data)
    current_market_data <- current_market_data[, tail(.SD, exuber_length), by = .(symbol)]
    # calculate exuber
    exuber_by_symbol <- current_market_data[, get_sadf(data = close, minw = min_w, lag = 1L), by = .(symbol)]
    exuber_by_symbol[, datetime := max(current_market_data$datetime)]
    exuber_by_symbol <- exuber_by_symbol[, .(symbol, datetime, adf, sadf, gsadf, bsadf)]
    print(exuber_by_symbol)
  }
  if (s > closing_time) {
    print("Market is closed")
    break
  }
  Sys.sleep(60L)
  next_hour <- ceiling_date(s, frequency_in_minutes)
}


# import exuber data
exuber_data <- lapply(list.files("D:/risks/radf", full.names = TRUE), fread)
exuber_data <- rbindlist(exuber_data)
last_date <- max(exuber_data$datetime)

