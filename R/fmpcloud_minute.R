library(data.table)
library(future.apply)
library(lubridate)



# set up
plan(multicore(workers = 16L))

# all files
files <- list.files("D:/market_data/equity/usa/minute/spy", full.names = TRUE)

# get full table
start_time <- Sys.time()
data_minute <- future_lapply(files, function(x) fread(paste0("unzip -p ", x)))
# spy_minute <- lapply(files[1:50], function(x) fread(paste0("unzip -p ", x)))
end_time <- Sys.time()
print(end_time - start_time)

# checksuspectfull dates
delete_files <- lapply(data_minute, nrow)
table(unlist(delete_files))

# clean
data_symbol <- rbindlist(data_minute)
setnames(data_symbol, c("date", "open", "high", "low", "close", "volume"))
ohlc <- c('open', 'high', 'low', 'close')

# quantcoonect format to ussual format
data_symbol[, (ohlc) := lapply(.SD, function(x) x / 10000), .SDcols = ohlc]
data_symbol[, datetime := as.POSIXct(date, origin = "1970-01-01")]
setorderv(market_data, c("symbol", "datetime"))
return(market_data)

# save
fwrite(data_minute, )


get_market_equities_minutes()

function(path, tickers = NA) {

  # solve No visible binding for global variable
  symbol <- datetime <- `.` <- high <- low <- volume <- NULL

  # chosse ticker to import
  if (all(is.na(tickers))) {
    market_data_files <- list.files(path, full.names = TRUE)
  } else {
    market_data_files <- paste0(path, "/", tolower(tickers))
  }

  # import data
  market_data <- lapply(market_data_files, function(x) {
    zip_files <- list.files(x, full.names = TRUE)
    daily_data <- lapply(zip_files, function(y) {
      data_ <- fread(cmd = paste0('unzip -p ', y),
                     col.names = c('datetime', "open", "high", "low", "close", "volume"))
      data_[, symbol := toupper(gsub(".*/", "", x))]
      data_[, date := as.Date(gsub(".*/|_trade.*", "", y), format = "%Y%m%d")]
    })
    daily_data <- rbindlist(daily_data)
    market_data <- daily_data[, .(symbol, datetime, date, open, high, low, close, volume)]
    ohlc <- c('open', 'high', 'low', 'close')

    # quantcoonect format to ussual format
    market_data[, (ohlc) := lapply(.SD, function(x) x / 10000), .SDcols = ohlc]
    market_data[, time := dhms(datetime / 1000)]
    market_data[, datetime := as.POSIXct(paste(date, time))]
    setorderv(market_data, c("symbol", "datetime"))
    market_data[, `:=`(time = NULL, date = NULL)]
  })
  market_data <- rbindlist(market_data)
  setorderv(market_data, c("symbol", "datetime"))
  return(market_data)
}
