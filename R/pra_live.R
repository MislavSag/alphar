library(httr)
library(lubridate)
library(equityData)
library(xts)
library(data.table)
library(leanr)
library(QuantTools)
library(equityData)
library(AzureStor)



# get data from azure
ENDPOINT = storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), key=Sys.getenv("BLOB-KEY"))
CONT = storage_container(ENDPOINT, "equity-usa-hour-fmpcloud-adjusted")
CONTMIN = storage_container(ENDPOINT, "equity-usa-minute-fmpcloud")
fmpcloudr::fmpc_set_token(Sys.getenv("APIKEY-FMPCLOUD"))

# parameters
pra_length <- 8 * 22 * 8
frequency_in_minutes <- "hour" # hour minute 5 mins

# get NY current time
get_ny_time <- function() {
  s <- Sys.time()
  s <- .POSIXct(s, "America/New_York")
  return(s)
}



# UPDATE RAW EXUBER DATA --------------------------------------------------

# import data
azure_blobs <- list_blobs(CONT)
market_data_list <- lapply(azure_blobs$name, function(x) {
  print(x)
  y <- tryCatch(storage_read_csv2(CONT, x), error = function(e) NA)
  if (is.null(y) | all(is.na(y))) return(NULL)
  y <- cbind(symbol = x, y)
  return(y)
})
market_data <- rbindlist(market_data_list)
market_data[, symbol := toupper(gsub("\\.csv", "", symbol))]
market_data[, returns := close / shift(close) - 1]
market_data <- na.omit(market_data)
market_data$datetime <- as.POSIXct(as.numeric(market_data$datetime),
                                   origin=as.POSIXct("1970-01-01", tz="EST"),
                                   tz="EST")
market_data <- market_data[, .(symbol, datetime, close, returns)]
market_data <- unique(market_data, by = c("symbol", "datetime"))

keep_symbols <- market_data[, .N, by = symbol][N > pra_length + 1, symbol]
market_data_sample <- market_data[symbol %in% keep_symbols]
market_data_sample <- market_data_sample[, tail(.SD, pra_length + 1), by = .(symbol)]
symbols <- unique(market_data_sample$symbol)
close_data <- market_data_sample[, .(symbol, datetime, close)]



# import and prepare market data for which we will calculate exuber aggregate indicator
# print("Import sp500 market data")
# sp500_stocks <- GET("https://financialmodelingprep.com/api/v3/sp500_constituent?apikey=15cd5d0adf4bc6805a724b4417bbaafc")
# sp500_stocks <- rbindlist(httr::content(sp500_stocks))
# market_data <- import_lean("D:/market_data/equity/usa/hour/trades", sp500_stocks$symbol)
# setorderv(market_data, c('symbol', 'datetime'))
# keep_symbols <- market_data[, .N, by = symbol][N > pra_length + 1, symbol]
# market_data_sample <- market_data[symbol %in% keep_symbols]
# market_data_sample <- market_data_sample[, tail(.SD, pra_length + 1), by = .(symbol)]
# symbols <- unique(market_data_sample$symbol)
# close_data <- market_data_sample[, .(symbol, datetime, close)]

# adjust for splits and dividends
factor_files_paths <- list.files("D:/factor_files", full.names = TRUE)
factor_files <- lapply(factor_files_paths, fread)
names(factor_files) <- gsub(".*/|.csv", "", factor_files_paths)
factor_files <- rbindlist(factor_files, idcol = TRUE)
setnames(factor_files, colnames(factor_files), c("symbol", "date", "price_factor", "split_factor", "previous_price"))
factor_files[, symbol := toupper(symbol)]
factor_files[, date := as.Date(as.character(date), "%Y%m%d")]

# get current data
get_current_data <- function(close_dt, time = "hour") {

  # set start and end dates
  ticker <- close_dt$symbol[1]
  start_date <- as.Date(max(close_dt$datetime))
  end_date <- start_date + 5

  # get market data
  ohlcv <- get_market_equities(ticker,
                               from = as.character(start_date),
                               to = as.character(end_date),
                               time = time,
                               api_key = Sys.getenv("APIKEY-FMPCLOUD"))
  if (is.null(ohlcv)) {
    print(paste0("There is no data for symbol ", ticker))
    return(NULL)
  }
  ohlcv$symbol <- ticker
  ohlcv$formated <- as.POSIXct(ohlcv$formated, tz = "EST")
  ohlcv <- ohlcv[, .(symbol, datetime = formated, open = o, high = h, low = l, close = c, volume = v)]
  ohlcv <- rbind(close_dt, ohlcv[, .(symbol, datetime, close)])
  prices <- unique(ohlcv)
  prices <- prices[format(datetime, "%H:%M:%S") %between% c("10:00:00", "15:00:00")]
  setorderv(prices, c("symbol", "datetime"))

  # add newest data becuase FP cloud can't reproduce newst data that fast
  nytime <- format(Sys.time(), tz="America/New_York", usetz=TRUE)
  if (time == "hour" && hour(nytime) > hour(max(prices$datetime))) {
    last_price <- GET(paste0("https://financialmodelingprep.com/api/v3/quote-short/", ticker, "?apikey=", Sys.getenv("APIKEY-FMPCLOUD")))
    last_price <- content(last_price)
    if (length(last_price) > 0) {
      last_price <- last_price[[1]]$price
      prices <- rbind(prices, data.table(symbol = ticker, datetime = as.POSIXct(round.POSIXt(nytime, units = "hours")), close = last_price))
    }
  }

  # adjust
  prices[, date:= as.Date(datetime)]
  prices <- merge(prices, factor_files, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE)
  prices[, `:=`(split_factor = na.locf(split_factor, na.rm = FALSE, rev = TRUE),
                price_factor = na.locf(price_factor, na.rm = FALSE, rev = TRUE)), by = symbol]
  prices[, `:=`(split_factor = ifelse(is.na(split_factor), 1, split_factor),
                price_factor = ifelse(is.na(price_factor), 1, price_factor))]
  cols_change <- c("close")
  prices[, (cols_change) := lapply(.SD, function(x) {x * price_factor * split_factor}), .SDcols = cols_change]

  return(prices)
}

# set closing time
closing_time <- as.POSIXct(paste0(Sys.Date(), "15:00:00"), tz = "America/New_York")
open_time <- as.POSIXct(paste0(Sys.Date(), "08:30:00"), tz = "America/New_York")

# main function which calculates exuber aggregate indicator
s <- get_ny_time()
next_time_point <- ceiling_date(s, frequency_in_minutes)

# main loop
repeat {

  # get current time
  s <- get_ny_time()
  print(s)

  #calculate percent rank
  if (s >= next_time_point) {
    print("Calculate percent rank")

    # get current data
    close_dt <- close_data[symbol %in% symbols]
    current_data <- close_dt[, get_current_data(data.table(symbol, datetime, close)), by = "symbol"]
    current_data[, pra := roll_percent_rank(close, pra_length), by = "symbol"]

    # calcualte pra indicator
    pra_values <- current_data[datetime == max(datetime)]
    pra_values <- pra_values[, pra]
    pra_values <- ifelse(pra_values < 0.001, 1, 0)
    pra_values <- sum(pra_values, na.rm = TRUE)

    # send mail notification if main indicator is above threshold (we have to sell)
    # if (pra_values > 10) {
    #   send_email(message = "PRA Value is above Threshold",
    #              sender = "mislav.sagovac@contentio.biz",
    #              recipients = c("mislav.sagovac@contentio.biz"))
    # }

    # save to blob
    object <- data.frame(datetime = max(current_data$datetime), pra = pra_values)
    save_blob_files(object, file_name = "pra.csv", container = "qc-live")
  }

  # break loop if exchange is closed
  if (s > closing_time) {
    print("Market is closed")
    break
  }
  Sys.sleep(1L)
  next_time_point <- ceiling_date(s, frequency_in_minutes)
}
