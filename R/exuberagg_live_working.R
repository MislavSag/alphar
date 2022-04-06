library(exuber)
library(httr)
library(lubridate)
library(fmpcloudr)
library(leanr)
library(mrisk)
library(httr)
library(jsonlite)
library(future.apply)
library(emayili)
library(equityData)
library(xts)
library(runner)
library(TTR)
library(data.table)
library(rdrop2)


# set up
APIKEY = Sys.getenv("APIKEY-FMPCLOUD")
fmpc_set_token(APIKEY, noBulkWarn = TRUE)
base_url = "https://financialmodelingprep.com/api/v4"
radf_path = "D:/risks/radfagg-live"

# parameters
exuber_length <- 600
sma_width <- 8
use_log = 1
price_lag = 1
frequency_in_minutes <- "hour" # hour minute 5 mins
min_w <- psy_minw(exuber_length)

# get NY current time
get_ny_time <- function() {
  s <- Sys.time()
  s <- .POSIXct(s, "America/New_York")
  return(s)
}

# email template
send_email <- function(message = enc2utf8("Order for ExuberrAgg Strategy."),
                       sender = "mislav.sagovac@contentio.biz",
                       recipients = c("mislav.sagovac@contentio.biz")) {
  email <- emayili::envelope()
  email <- email %>%
    emayili::from(sender) %>%
    emayili::to(recipients) %>%
    emayili::subject("ExuberrAgg Strategy.") %>%
    emayili::text(message)
  smtp <- server(host = "mail.contentio.biz",
                 port = 587,
                 username = "mislav.sagovac+contentio.biz",
                 password = "Contentio0207")
  smtp(email, verbose = TRUE)
}

# import and prepare market data for which we will calculate exuber aggregate indicator
print("Import sp500 market data")
sp500_stocks <- GET("https://financialmodelingprep.com/api/v3/sp500_constituent?apikey=15cd5d0adf4bc6805a724b4417bbaafc")
sp500_stocks <- rbindlist(httr::content(sp500_stocks))
market_data <- import_lean("D:/market_data/equity/usa/hour/trades", sp500_stocks$symbol)
setorderv(market_data, c('symbol', 'datetime'))
keep_symbols <- market_data[, .N, by = symbol][N > exuber_length, symbol]
market_data <- market_data[symbol %in% keep_symbols]
market_data <- market_data[, tail(.SD, exuber_length), by = .(symbol)]
symbols <- unique(market_data$symbol)
close_data <- market_data[, .(symbol, datetime, close)]

# import radf measures
print("Import radf history.")
exuber_file <- list.files('D:/risks/radf-indicators', pattern = paste(use_log, exuber_length, price_lag, sep = '-'), full.names = TRUE)
exuber_history <- fread(exuber_file)

# adjust for splits and dividends
factor_files_paths <- list.files("D:/factor_files", full.names = TRUE)
factor_files <- lapply(factor_files_paths, fread)
names(factor_files) <- gsub(".*/|.csv", "", factor_files_paths)
factor_files <- rbindlist(factor_files, idcol = TRUE)
setnames(factor_files, colnames(factor_files), c("symbol", "date", "price_factor", "split_factor", "previous_price"))
factor_files[, symbol := toupper(symbol)]
factor_files[, date := as.Date(as.character(date), "%Y%m%d")]

# radf point
radf_point <- function(ticker, window, price_lag, use_log, api_key, time = "hour") {

  # set start and end dates
  data_ <- market_data[symbol == ticker]
  start_dates <- as.Date(max(data_$datetime))
  end_dates <- start_dates + 5

  # get market data
  ohlcv <- get_market_equities(ticker,
                               from = as.character(start_dates),
                               to = as.character(end_dates),
                               time = time,
                               api_key = Sys.getenv("APIKEY-FMPCLOUD"))
  if (is.null(ohlcv)) {
    print(paste0("There is no data for symbol ", ticker))
    return(NULL)
  }
  ohlcv$symbol <- ticker
  ohlcv$formated <- as.POSIXct(ohlcv$formated, tz = "EST")
  ohlcv <- ohlcv[, .(symbol, datetime = formated, open = o, high = h, low = l, close = c, volume = v)]
  ohlcv <- rbind(data_, ohlcv)
  prices <- unique(ohlcv)
  prices <- prices[format(datetime, "%H:%M:%S") %between% c("10:00:00", "15:00:00")]
  prices <- prices[, .(symbol, datetime, close)]
  setorderv(prices, c("symbol", "datetime"))

  # add newest data becuase FP cloud can't reproduce newst data that fast
  nytime <- format(Sys.time(), tz="America/New_York", usetz=TRUE)
  if (time == "hour" && hour(nytime) > hour(max(prices$datetime))) {
    last_price <- GET(paste0("https://financialmodelingprep.com/api/v3/quote-short/", ticker, "?apikey=", api_key))
    last_price <- content(last_price)[[1]]$price
    prices <- rbind(prices, data.table(symbol = ticker, datetime = as.POSIXct(round.POSIXt(nytime, units = "hours")), close = last_price))
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

  # calculate exuber
  if (use_log) {
    close <- prices[, .(symbol, datetime, close)]
    close[, close := log(close)]
  } else {
    close <- prices[, .(symbol, datetime, close)]
  }

  # calculate exuber
  # close <- close[(length(close)-window+1):length(close)]
  y <- runner(x = as.data.frame(close),
              f = function(x) {
                y <- exuber::radf(x$close, lag = price_lag, minw = min_w)
                stats <- exuber::tidy(y)
                bsadf <- data.table::last(exuber::augment(y))[, 4:5]
                # max_datetime <- as.POSIXct(as.character(max_datetime), tz = 'EST')
                # attributes(max_datetime)$tzone <- 'UCT'
                result <- cbind(symbol = ticker, datetime = x$datetime[window], stats, bsadf)
                result$id <- NULL
                as.data.table(result)
                },
              k = window, na_pad = TRUE)
  y <- y[!is.na(y)]
  y <- rbindlist(y)
  return(y)
}

# set closing time
closing_time <- as.POSIXct(paste0(Sys.Date(), "16:30:00"), tz = "America/New_York")
open_time <- as.POSIXct(paste0(Sys.Date(), "08:30:00"), tz = "America/New_York")

# main function which calculates exuber aggregate indicator
s <- get_ny_time()
next_time_point <- ceiling_date(s, frequency_in_minutes)
repeat {
  s <- get_ny_time()
  print(s)
  if (s >= next_time_point) {
    print("Exuber Aggregate calculate")

    # get current market data for all stocks
    start_time <- Sys.time()
    exuber_symbol <- lapply(seq_along(symbols), function(i) { # seq_along(symbols)
      print(i)
      print(symbols[i])
      radf_point(symbols[i], exuber_length, 1, 1, APIKEY, "hour")
    })
    end_time <- Sys.time()
    end_time - start_time
    exubers <- rbindlist(exuber_symbol)
    exubers[, radf := adf + sadf + gsadf + badf + bsadf]
    std_exuber <- exubers[, lapply(.SD, sd, na.rm = TRUE), by = c('datetime'), .SDcols = colnames(exubers)[3:ncol(exubers)]]
    setorderv(std_exuber, 'datetime')
    std_exuber <- na.omit(std_exuber)
    cols <- colnames(std_exuber)
    std_exuber <- rbind(exuber_history, std_exuber, use.names = FALSE)
    colnames(std_exuber) <- cols
    std_exuber <- unique(std_exuber)
    std_exuber[, radf_sma_8 := SMA(radf, 8)]
    std_exuber <- tail(std_exuber, 1L)
    print(std_exuber)

    # send mail notification if main indicator is above threshold (we have to sell)
    if (std_exuber$radf_sma_8 > 3) {
      send_email(message = "Exuber Agg is Above Threshold",
                 sender = "mislav.sagovac@contentio.biz",
                 recipients = c("mislav.sagovac@contentio.biz"))
    }

    # save result to file locally and to M-files
    file_name_date <- file.path(radf_path, paste0("radfagg-", Sys.Date(), ".csv"))
    file_name <- file.path(radf_path, paste0("radfagg.csv"))
    file_name_one <- file.path(radf_path, paste0("radfagg_one.csv"))
    if (!file.exists(file_name)) {

      # save locally
      fwrite(std_exuber, file_name)
      fwrite(std_exuber, file_name_date)

      # upload file to dropbox if it doesnt'exists
      drop_upload(file = file_name, path = "exuber")

    } else {

      # prepare data for dropbox
      fwrite(tail(std_exuber, 1), file_name, col.names = FALSE)

      # update dropbox
      drop_upload(file = file_name, path = "exuber")
    }
  }
  if (s > closing_time) {
    print("Market is closed")
    break
  }
  Sys.sleep(5L)
  next_time_point <- ceiling_date(s, frequency_in_minutes)
}
