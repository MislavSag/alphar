library(data.table)
library(RMySQL)
library(DBI)
library(fst)
library(IBrokers)
library(checkmate)
library(quantmod)
library(AzureStor)
library(rvest)
library(fasttime)



# IMPORT DATA FROM MYSQL --------------------------------------------------


import_blob <- function(symbols,
                        trading_hours = TRUE,
                        upsample = 1,
                        use_cache = TRUE,
                        combine_data = FALSE,
                        save_path=getwd(),
                        container="equity-usa-hour-trades") {

  # check parameters
  assert_character(symbols)
  assert_path_for_output(save_path, overwrite = TRUE)
  assert_logical(trading_hours)
  assert_logical(use_cache)
  assert_logical(combine_data)
  assert_int(upsample, lower = 1, upper = 60)

  # blob data
  bl <- storage_endpoint(
    "https://contentiobatch.blob.core.windows.net/",
    key="qdTsMJMGbnbQ5rK1mG/9R1fzfRnejKNIuOv3X3PzxoBqc1wwTxMyUuxNVSxNxEasCotuzHxwXECo79BLv71rPw==")
  cont <- storage_container(bl, container)

  # read from saved file if exist
  data_symbols <- lapply(symbols, function(x) {
    file_path <- file.path(save_path, paste0(x, '.fts'))
    if (file.exists(file_path) & use_cache) {
      market_data <- read.fst(file_path)
      old <- difftime(Sys.time(), tail(market_data$date, 1))
      print(paste0('Read ', x, ' from local file. The data is ', round(old, 2), ' days old.'))
    } else {

      # check if table exists
      if (!(storage_file_exists(cont, paste0(x, ".csv")))) {
        print(paste0("Symbol ", x, " is not in blob."))
        return(NULL)
      } else {
        market_data <- storage_read_csv(cont, paste0(x, ".csv"))
        market_data[, c('id', 'ticker')] <- NULL  # remove columns
      }

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
      barCount <- period.apply(market_data$barCount, endpoints(market_data$barCount, "mins", k=upsample), sum)
      volume <- period.apply(market_data$volume, endpoints(market_data$volume, "mins", k=upsample), sum)
      average <- period.apply(market_data$average, endpoints(market_data$average, "mins", k=upsample), mean)
      ohlc <- to.period(OHLC(market_data), period = 'minutes', k = upsample)
      market_data <- merge(ohlc, volume, average, barCount)
      colnames(market_data) <- c('open', 'high', 'low', 'close', 'volume', 'average', 'barCount')
    } else {
      market_data <- market_data[, 1:(ncol(market_data) - 1)]
      colnames(market_data) <- tolower(colnames(market_data))
    }
    market_data
  })

  # combine data
  if (isTRUE(combine_data)) {
    col_names <- purrr::map2(data_symbols, symbols, ~ {paste0(.y, '_', colnames(.x))})
    data_symbols <- do.call(merge, data_symbols)
    colnames(data_symbols) <- unlist(col_names, use.names = FALSE)
  } else {
    names(data_symbols) <- symbols
  }

  return(data_symbols)
}


import_sp500 <- function() {
  url <- 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
  sp500 <- read_html(url) %>%
    html_nodes('table') %>%
    .[[1]] %>%
    html_table(.)
  sp500_changes <- read_html(url) %>%
    html_nodes('table') %>%
    .[[2]] %>%
    html_table(., fill = TRUE)
  sp500_symbols <- c(sp500$Symbol, sp500_changes$Added[-1], sp500_changes$Removed[-1])
  sp500_symbols <- unique(sp500_symbols)
  sp500_symbols <- sp500_symbols[sp500_symbols != '']
  return(sp500_symbols)
}


import_intraday <- function(path, extension, ...) {
  files <- list.files(path, pattern = extension, full.names = TRUE)
  tickers <- gsub("\\.csv", "", list.files(path, pattern = extension))
  equities <- lapply(files, fread, drop = "t")
  names(equities) <- tickers
  equities <- rbindlist(equities, idcol = TRUE)
  equities[, formated := fasttime::fastPOSIXct(formated)]
  setnames(equities, colnames(equities), c("symbol", "open", "high", "close", "low", "volume", "datetime"))
  return(equities)
}

# import daily data
import_daily <- function(path, extension, ...) {
  files <- list.files(path, pattern = extension, full.names = TRUE)
  tickers <- gsub("\\.csv", "", list.files(path, pattern = extension))
  equities <- lapply(files, fread)
  equities <- rbindlist(equities)

  return(equities)
}




