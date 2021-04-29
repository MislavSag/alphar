library(data.table)
library(httr)
library(fmpcloudr)
library(zip)


# set api token
APIKEY = "15cd5d0adf4bc6805a724b4417bbaafc"
fmpc_set_token(APIKEY, noBulkWarn = TRUE)
base_url = "https://financialmodelingprep.com/api/v4"
quantconnect_data_path <- "C:/Users/Mislav/Documents/GitHub/exuber_live/data/equity/usa/hour"

# sp500 symbols
sp500_stocks_url <- paste0("https://financialmodelingprep.com/api/v3/sp500_constituent?apikey=", APIKEY)
sp500_stocks <- content(GET(sp500_stocks_url))
sp500_stocks <- lapply(sp500_stocks, as.data.table)
sp500_stocks <- rbindlist(sp500_stocks)
symbols <- sp500_stocks$symbol
symbols <- c(symbols, "SPY")

# get market data function
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

# get data for all symbols
start_dates <- seq.Date(as.Date("2019-01-01"), Sys.Date() - 1, by = 5)
end_dates <- start_dates + 5
for (symbol in symbols) {
  print(symbol)
  data_by_symbol <- lapply(seq_along(start_dates), function(i) {
    get_market_data(symbol, 1, 'hour', start_dates[i], end_dates[i])
  })
  data_by_symbol <- rbindlist(data_by_symbol)
  if (length(data_by_symbol) == 0) {
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
    file_name <- file.path(quantconnect_data_path, paste0(tolower(symbol), ".csv"))
    fwrite(data_by_symbol, file_name, col.names = FALSE)
    zip_file_name <- file.path(quantconnect_data_path, paste0(tolower(symbol), ".zip"))
    zip::zipr(zip_file_name, file_name)
    file.remove(file_name)
  }
}
