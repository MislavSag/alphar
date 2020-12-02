library(RMySQL)
library(DBI)
library(fst)
library(IBrokers)


# IMPORT DATA FROM MYSQL --------------------------------------------------

import_mysql <- function(contract, save_path=getwd(), trading_days = TRUE, upsample = FALSE, ...) {
  # read from saved file if exist
  file_path <- file.path(save_path, paste0(contract, '.fts'))
  if (file.exists(file_path)) {
    market_data <- read.fst(file_path)
    old <- difftime(Sys.time(), tail(market_data$date, 1))
    print(paste0('Read from local file. The data is ', round(old, 2), ' days old.'))
  } else {
    # make connection
    con <- DBI::dbConnect(...)

    # query table
    market_data <- DBI::dbGetQuery(con, paste0('SELECT * FROM ', contract, ';'))

    # close connection
    dbDisconnect(con)

    # clean table
    market_data$date <- as.POSIXct(market_data$date)
    market_data <- market_data[base::order(market_data$date, decreasing  = FALSE), ]
    market_data$id <- NULL

    # save data to path
    write.fst(market_data, file_path)
  }

  # Convert to xts
  market_data <- xts::xts(market_data[, 2:(ncol(market_data)-1)], order.by = market_data$date, tz = 'CET')

  # Change timezone
  # tzone(market_data) <- 'America/New_York'

  # keep only trading hours
  if (trading_days) {
    market_data <- market_data["T14:30:00/T22:00:00"]
  }

  # Upsample
  if (is.numeric(upsample)) {
    market_data <- xts::to.period(market_data, period = 'minutes', k = upsample)
  }

  return(market_data)
}


import_ib <- function(contract, trading_days = TRUE, upsample = FALSE, frequencies = '1 hour', duration = '20 Y') {
  tws <- twsConnect(clientId = 10, host = '127.0.0.1', port = 7496)
  contract <- twsEquity(contract, exch = 'SMART', primary = 'ISLAND')
  ohlcv <- reqHistoricalData(tws, contract, barSize = frequencies, duration = duration)
  twsDisconnect(tws)

  # Change timezone
  tzone(ohlcv) <- 'America/New_York'

  # keep only trading hours
  if (trading_days) {
    ohlcv <- ohlcv["T09:30:00/T16:00:00"]
  }

  # Upsample
  if (is.numeric(upsample)) {
    ohlcv <- xts::to.period(ohlcv, period = 'minutes', k = upsample)
  }

  return(ohlcv)
}
