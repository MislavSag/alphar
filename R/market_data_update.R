library(data.table)
library(fmpcloudr)
library(httr)


# paths
path_hourly_trades_adjusted <- "D:/market_data/equity/usa/hour/trades_adjusted"
path_hourly_trades_unadjusted <- "D:/market_data/equity/usa/hour/trades"
path_day_trades <- "D:/market_data/equity/usa/day/trades"

# set api token
APIKEY = "15cd5d0adf4bc6805a724b4417bbaafc"
fmpc_set_token(APIKEY, noBulkWarn = TRUE)
base_url = "https://financialmodelingprep.com/api/v4"


# MARKET DATA -------------------------------------------------------------

# get market data
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

# update function for adjusted and unadjusted market data
update_market_data_all <- function(path, api_tag) {

  files <- list.files(path, full.names = TRUE)

  for (i in seq_along(tickers)) {
    print(i)
    x <- fread(files[i], sep = ";")
    ticker <- gsub(".*/|.csv", "", files[i])
    if (length(x) == 0) {
      next()
    }
    last_date <- as.Date(x[which.max(x$t)]$formated)
    if (last_date < (Sys.Date() - 10)) {
      start_dates <- seq.Date(last_date, Sys.Date() - 10, by = 10)
      end_dates <- start_dates + 10
    } else {
      start_dates <- last_date
      end_dates <- Sys.Date()
    }
    data_by_symbol <- lapply(seq_along(start_dates), function(i) {
      get_market_data(ticker, 1, 'hour', start_dates[i], end_dates[i], api_tag = api_tag)
    })
    new_data <- rbindlist(data_by_symbol)
    if (length(new_data) == 0) {
      return(NULL)
    }
    merged_data <- rbind(x, new_data)
    merged_data <- unique(merged_data)
    setorder(merged_data, t)
    fwrite(merged_data, file.path(path, paste0(ticker, '.csv')), sep = ';')
  }
}

update_market_data_last_10_days <- function(path, api_tag) {

  files <- list.files(path, full.names = TRUE)

  for (i in seq_along(files)) {
    print(i)
    x <- fread(files[i], sep = ";")
    ticker <- gsub(".*/|.csv", "", files[i])
    if (length(x) == 0) {
      next()
    }
    start_dates <- Sys.Date() - 10
    end_dates <- Sys.Date()
    data_by_symbol <- get_market_data(ticker, 1, 'hour', start_dates, end_dates, api_tag = api_tag)
    if (length(data_by_symbol) == 0) {
      next()
    } else {
      merged_data <- rbind(x, data_by_symbol)
      merged_data <- unique(merged_data)
      setorder(merged_data, t)
      fwrite(merged_data, file.path(path, paste0(ticker, '.csv')), sep = ';')
    }
  }
}

start_time <- Sys.time()
update_market_data_last_10_days(path_hourly_trades_unadjusted, api_tag = "/historical-price/")
end_time <- Sys.time()
end_time - start_time


# adjusted data
start_time <- Sys.time()
update_market_data(update_market_data_all, api_tag = "/historical-price-adjusted/")
end_time <- Sys.time()
end_time - start_time



base_url = "https://financialmodelingprep.com/api/v4"
symbol = "A"
multiply = 1
time = 'hour'
from = "2014-10-20"
to = "2014-11-10"
q = paste(symbol, multiply, time, from, to, sep = "/")
url <- paste0(base_url, "/historical-price-adjusted/", q)
req <- GET(url, query = list(apikey = APIKEY))
data <- content(req)
data <- rbindlist(data$results)
plot(as.POSIXct(data$formated), data$c, type = 'l')



