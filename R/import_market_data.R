#' Import local market data
#'
#' @param path Path to local market data
#' @return Market data organized as panel data
#' @examples
#' add(1, 1)
import_local_data <- function(path) {
  market_data_files <- list.files(path, full.names = TRUE)
  market_data <- lapply(market_data_files, function(x) {
    y <- fread(cmd = paste0('unzip -p ', x),
          col.names = c('datetime', "open", "high", "low", "close", "volume"))
    y[, symbol := toupper(gsub(".*/|\\.zip", "", x))]
  })
  market_data <- rbindlist(market_data)
  market_data <- market_data[, .(symbol, datetime, open, high, low, close, volume)]
  ohlc <- c('open', 'high', 'low', 'close')
  market_data[, (ohlc) := lapply(.SD, function(x) x / 10000), .SDcols = ohlc]
  market_data[, datetime := lubridate::ymd_hm(datetime, tz = "EST")]
  setorderv(market_data, c("symbol", "datetime"))
  return(market_data)
}
