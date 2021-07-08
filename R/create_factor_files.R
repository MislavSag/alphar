library(data.table)
library(httr)
library(fmpcloudr)
library(leanr)
library(future.apply)


# GLOBALS
API_KEY = '15cd5d0adf4bc6805a724b4417bbaafc'
fmpc_set_token(API_KEY)

# get sp 500 stocks
SP500 = fmpc_symbols_index()
SP500_DELISTED <- content(GET(paste0('https://financialmodelingprep.com/api/v3/historical/sp500_constituent?apikey=', API_KEY)))
SP500_DELISTED <- rbindlist(SP500_DELISTED)
SP500_SYMBOLS <- unique(c(SP500$symbol, SP500_DELISTED$symbol))

# get market data
market_data <- import_lean("D:/market_data/equity/usa/hour/trades")
market_data_daily <- market_data[, .SD[.N], by = .(symbol, date = as.Date(datetime))]
symbols <- unique(market_data$symbol)

# get only data from the IPO date
plan(multicore(workers = 2L))
ipo_dates <- vapply(symbols, function(x) {
  y <- get_ipo_date(x, api_key = API_KEY)
  if (is.null(y)) {
    return("2004-01-02")
  } else {
    return(y)
  }
}, character(1))
ipo_dates_dt <- as.data.table(ipo_dates, keep.rownames = TRUE)
setnames(ipo_dates_dt, "rn", "symbol")
ipo_dates_dt[, ipo_dates := as.Date(ipo_dates)]
market_data_new <- copy(market_data)
market_data_new <- ipo_dates_dt[market_data_new, on = "symbol"]
market_data_new <- market_data_new[datetime >= ipo_dates]
market_data_new_daily <- market_data_new[, .SD[.N], by = .(symbol, date = as.Date(datetime))]
market_data_new_daily$ipo_dates <- NULL

# generate factor files
lapply(unique(market_data_new_daily$symbol), function(x) {
  print(x)
  create_factor_file(x, market_data_new_daily, API_KEY, "D:/factor_files")
})
test <- create_factor_file("CSRA", market_data_daily, API_KEY, "D:/factor_files")


# # TEST --------------------------------------------------------------------
#
# ticker = "CSRA"
# ohlcv = market_data_new_daily[symbol == ticker]
# api_key = '15cd5d0adf4bc6805a724b4417bbaafc'
# save_dir = "D:/factor_files"
#
# # solve No visible binding for global variable
# symbol <- adjDividend <- `.` <- NULL
#
# fmpc_set_token(api_key)
#
# # daily market data df should be the input
# # ohlcv$date <- as.Date(ohlcv$datetime)
# ohlcv <- as.data.table(ohlcv)
# df <- ohlcv[symbol == ticker, .(date, close)]
#
# # add splits data
# splits <- as.data.table(get_stock_split_factor(ticker))
# if (all(is.na(splits))) {
#   df[, split_factor := NA]
# } else {
#   df <- splits[df, on = "date"]
#   df$ratio <- NULL
# }
#
# # get dividends
# dividends <- fmpc_security_dividends(ticker, startDate = '2004-01-01')
# dividends <- as.data.table(dividends)
# if (all(is.na(dividends))) {
#   df[, dividend := NA]
# } else {
#   if (!("dividend" %in% names(dividends))) {
#     dividends[, dividend := adjDividend]
#   }
#   dividends <- dividends[, dividend := ifelse(is.na(dividend) & adjDividend > 0, adjDividend, dividend)]
#   dividends <- dividends[, .(date, dividend)]
#   df <- dividends[df, on = "date"]
# }
#
# # if no splits and dividends return factor files with start and end dates
# if (length(dividends) == 0 & nrow(splits) == 0) {
#   factor_file <- data.table(date = c(as.character(format.Date(df$date[1]), "%Y%m%d"), "20500101"),
#                             price_factor = c(1, 1),
#                             split_factor = c(1, 1),
#                             lag_close = c(df$close[1], 0))
#   # save
#   fwrite(factor_file, file.path(save_dir, paste0(tolower(ticker), ".csv")), col.names = FALSE, row.names = FALSE)
#   return(factor_file)
# }
#
# # clean data
# df[, `:=`(lag_close = shift(close),
#           date = shift(date))]
# df <- df[!is.na(date)]
#
# # keep oly dividend ot split days
# factor_file <- df[(!is.na(dividend) & dividend != 0) | !is.na(split_factor), .(date, dividend, split_factor, lag_close)]
# if (nrow(factor_file) == 0) {
#   factor_file <- data.table(date = c(as.character(format.Date(df$date[1]), "%Y%m%d"), "20500101"),
#                             price_factor = c(1, 1),
#                             split_factor = c(1, 1),
#                             lag_close = c(df$close[1], 0))
#   # save
#   fwrite(factor_file, file.path(save_dir, paste0(tolower(ticker), ".csv")), col.names = FALSE, row.names = FALSE)
#   return(factor_file)
# }
# factor_file <- unique(factor_file)
# # factor_file <- factor_file[date < as.Date("2018-06-01")] # TO COMPARE WITH QC
# factor_file[, split_factor := na.locf(split_factor, rev = TRUE, na.rm = FALSE)]
# factor_file <- factor_file[is.na(split_factor), split_factor := 1]
#
# # add row to the end
# factor_file <- rbind(factor_file, data.table(date = as.Date("2050-01-01"), dividend = NA, lag_close = 0, split_factor = 1))
#
# # calculate price factor
# price_factor <- vector("numeric", nrow(factor_file))
# lag_close <- factor_file$lag_close
# split_factor <- factor_file$split_factor
# dividend <- factor_file$dividend
# for (i in nrow(factor_file):1) {
#   if (i == nrow(factor_file)) {
#     price_factor[i] <- (lag_close[i-1] - dividend[i-1]) / lag_close[i-1] * 1
#   } else if (i == 1) {
#     price_factor[i] <- NA
#   } else if (is.na(dividend[i-1])) {
#     price_factor[i] <- price_factor[i+1]
#   } else {
#     price_factor[i] <- ((lag_close[i-1] - dividend[i-1]) / lag_close[i-1]) * price_factor[i+1]
#   }
# }
# price_factor <- c(price_factor[-1], NA)
# price_factor[is.na(price_factor)] <- 1
#
# # add factor to factor file
# factor_file[, price_factor := price_factor]
# factor_file[, date := format.Date(date, "%Y%m%d")]
# factor_file <- factor_file[, .(date, price_factor, split_factor, lag_close)]
