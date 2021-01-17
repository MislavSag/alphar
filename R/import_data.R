library(RMySQL)
library(DBI)
library(fst)
library(IBrokers)
library(checkmate)


# IMPORT DATA FROM MYSQL --------------------------------------------------

symbols = c('SPY')
trading_hours = TRUE
upsample = 5
use_cache = TRUE
save_path = 'D:/market_data/usa/ohlcv'

import_mysql <- function(symbols,
                         trading_hours = TRUE,
                         upsample = 1,
                         use_cache = TRUE,
                         save_path=getwd(),
                         ...) {

  # check parameters
  assert_character(symbols)
  assert_path_for_output(save_path, overwrite = TRUE)
  assert_logical(trading_hours)
  assert_logical(use_cache)
  assert_int(upsample, lower = 1, upper = 60)
  assert_character(dbname)

  # read from saved file if exist
  data_symbols <- lapply(symbols, function(x) {
    file_path <- file.path(save_path, paste0(x, '.fts'))
    if (file.exists(file_path) & use_cache) {
      market_data <- read.fst(file_path)
      old <- difftime(Sys.time(), tail(market_data$date, 1))
      print(paste0('Read ', x, ' from local file. The data is ', round(old, 2), ' days old.'))
    } else {
      # make connection
      con <- DBI::dbConnect(drv = RMySQL::MySQL(),
                            dbname = dbname,
                            username = 'odvjet12_mislav',
                            password = 'Theanswer0207',
                            host = '91.234.46.219',
      )

      # query table
      market_data <- DBI::dbGetQuery(con, paste0('SELECT * FROM ', x, ';'))

      # close connection
      dbDisconnect(con)

      # clean table
      market_data$date <- as.POSIXct(market_data$date)  # convert datetime to as.POSIXct
      market_data <- market_data[base::order(market_data$date, decreasing  = FALSE), ]  # order
      market_data[, c('id', 'ticker')] <- NULL  # remove columns

      # save data to path
      write.fst(market_data, file_path)  # cache
    }

    # Convert to xts
    market_data <- xts::xts(market_data[, 2:(ncol(market_data))], order.by = market_data$date, tz = 'CET')

    # Change timezone
    # tzone(market_data) <- 'America/New_York'

    # keep only trading hours
    if (trading_hours) {
      market_data <- market_data["T15:30:00/T22:00:00"]
    }

    # Upsample
    if (upsample > 1) {
      barcount <- period.apply(market_data$barCount, endpoints(market_data$barCount, "mins", k=upsample), sum)
      volume <- period.apply(market_data$volume, endpoints(market_data$volume, "mins", k=upsample), sum)
      average <- period.apply(market_data$average, endpoints(market_data$average, "mins", k=upsample), mean)
      ohlc <- to.period(OHLC(market_data), period = 'minutes', k = upsample)
      market_data <- merge(ohlc, volume, average, barcount)
    }

    # chage column names
    # colnames(market_data) <- paste0(x, '.', colnames(market_data))
    colnames(market_data) <- c('open', 'high', 'low', 'close', 'volume', 'average', 'barcount')
    market_data
  })

  return(data_symbols)
}


import_ib <- function(symbol, trading_hours = TRUE, frequencies = '1 hour',
                      duration = '20 Y', type = c('equity', 'index'),
                      what_to_show = 'TRADES') {
  type <- match.arg(type)
  tws <- twsConnect(clientId = 24, host = '127.0.0.1', port = 7496)
  if (type == 'equity') {
    symbol <- twsEquity(symbol, exch = 'SMART', primary = 'ISLAND')
  } else if (type == 'index') {
    symbol <- twsIndex(symbol, exch = 'CBOE', currency = 'USD')
  }
  ohlcv <- reqHistoricalData(tws,
                             contract,
                             endDateTime="",
                             barSize = frequencies,
                             duration = duration,
                             whatToShow = what_to_show)
  twsDisconnect(tws)

  # keep only trading hours
  if (trading_hours) {
    ohlcv <- ohlcv["T15:30:00/T22:00:00"]
  }

  return(ohlcv)
}


# library(rusquant)
# symbols <- getSymbolList('Finam') # download all available symbols in finam.ru
# usa_symbols <- symbols[symbols$Market == 25, ]
#
#
# getSymbols('LKOH',src='Finam') # default = main market
# rusquant::getSymbols('LKOH', src = 'Finam', market=1) # main market
# getSymbols('LKOH',src = 'Finam',market=8) # ADR of LKOH, from market id from loadSymbolList
# head(LKOH)
#
# # type period
# getSymbols('LKOH',src='Finam',period='day') # day bars - default parameter
# getSymbols('LKOH',src='Finam',period='5min') # 5 min bar
# getSymbols('LKOH',src='Finam',period='15min') # 15 min bar
#
# # download list of Symbols
# available_etf_list = c("FXMM", "FXCN", "FXIT", "FXJP", "FXDE", "FXUS", "FXAU", "FXUK", "FXRB", "FXRL", "FXRU")
# getSymbols(available_etf_list,src='Finam')
#
#
# getSymbols('AAPL',src='Finam', period='1min', from = '2007-01-01', to = '2008-01-15') # day bars - default parameter
# plot(AAPL$AAPL.Close)
#
# quantmod::getDividends('AAPL')
# quantmod::getSplits('AAPL')


# split adjustments
# div <- getDividends('AAPL', from="1900-01-01")
# s <- getSplits('AAPL', from="1900-01-01")
#
# min_time <- min(as.character(unique(strftime(zoo::index(market_data), format = "%H:%M:%S"))))
# index(s) <- as.POSIXct(paste(as.character(zoo::index(s)), min_time), tz = tzone(market_data), format = "%Y-%m-%d %H:%M:%S")
# test <- merge(market_data, s)
# test <- test[!is.na(Cl(test))]
# test$AAPL.spl[!is.na(test$AAPL.spl)]
# ratio <- ifelse(is.na(test$AAPL.spl), 1, 1 / test$AAPL.spl)
# ratio[ratio > 1]
# ratio <- cumprod(ratio)
# test$adj_close <- Cl(market_data) * ratio
# plot(test[, c('AAPL.close')])
#
# r <- adjRatios(s, NA, Cl(market_data))  # calculate adjustment ratios
# head(r)
# r[r$Split < 1]
#
# y <- adjustOHLC(x, ratio=r$Split, symbol.name=symbol)
# chartSeries(Cl(y))
# r2 <- na.locf(lag(r), fromLast=TRUE)
# y2 <- adjustOHLC(x, ratio=r2$Split, symbol.name=symbol)
# chartSeries(Cl(y2))

