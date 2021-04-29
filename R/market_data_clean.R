library(data.table)
library(ggplot2)
library(stringr)
library(fmpcloudr)
library(httr)
library(rvest)
library(stringr)
source('C:/Users/Mislav/Documents/GitHub/alphar/R/import_data.R')


# set api token
APIKEY = "15cd5d0adf4bc6805a724b4417bbaafc"
fmpc_set_token(APIKEY)

# import data
stocks <- import_intraday("D:/market_data/equity/usa/hour/trades", "csv")
stocks_daily <- import_daily("D:/market_data/equity/usa/day/trades", "csv")

# sp500 symbols
SP500 = fmpc_symbols_index()
SP500_historical <- content(GET(paste0("https://financialmodelingprep.com/api/v3/historical/sp500_constituent?apikey=", APIKEY)))
SP500_historical <- rbindlist(SP500_historical)
SP500_historical <- unique(SP500_historical$removedTicker, SP500_historical$symbol)
SP500_symbols <- c(SP500$symbol, SP500_historical)
setdiff(SP500_symbols, unique(stocks$symbol))
setdiff(unique(stocks$symbol), SP500_symbols)

# Quantconnect equity scraped



# SP500 time spans from first rate data
# firstratedata <- read_html("https://firstratedata.com/b/1/sp500-historical-intraday-stocks-bundle") %>%
#   html_elements(xpath = '//*[@id="pills-tabContent"]/div[4]/text()') %>%
#   html_text()
# firstratedata <- trimws(firstratedata[5:length(firstratedata)])
# first_date <- as.Date(gsub(".*First Date:| -. Last.*", "", firstratedata), "%d-%b-%Y")
# last_date <- as.Date(gsub(".*Last Date:", "", firstratedata), "%d-%b-%Y")
# company_name <- gsub(".*\\(|\\).*", "", firstratedata)
# delisted <- grepl("DELISTED", firstratedata)
# ticker <- gsub(" .*|-DELISTED.*", "", firstratedata)
# sp500_changes <- as.data.table(cbind.data.frame(ticker, company_name, first_date, last_date, delisted))

# choose only periods defined from firsratedata
ticker <- "WMI"
datetime_ohlcv <- stocks[symbol == ticker, .(datetime, open, high, close, low, volume)]
# getSplits(ticker)
plot(datetime_ohlcv$close)


# split adjusted data
split_adjustment <- function(ticker, datetime_ohlcv) {

  # get split factor from daily data
  splits <- tryCatch(getSplits(ticker, from = "2004-01-01"), error = function(e) e$message)

  # mannualy remove wrong splits
  if (ticker == "COL") {
    splits <- NA
  }

  # check from other source if there are no pslits
  if (any(grepl("failed", splits))) {
    splits <- content(GET("https://www.splithistory.com/", query = list(symbol = ticker))) %>%
      html_nodes(xpath = "//table[@width='208' and @style='font-family: Arial; font-size: 12px']") %>%
      html_table(header = TRUE) %>%
      .[[1]]
    splits$Date <- as.Date(splits$Date, "%m/%d/%Y")
    ratio1 <- as.numeric(str_extract(splits$Ratio, "\\d+"))
    ratio2 <- as.numeric(str_extract(splits$Ratio, "\\d+$"))
    splits$Ratio <- ratio2 / ratio1
    splits <- splits[splits$Date > as.Date("2004-01-01"), ]
    if (nrow(splits) == 0) {
      returns <- datetime_ohlcv$close / shift(datetime_ohlcv$close) - 1
      if (any(abs(returns) > 0.5, na.rm = TRUE)) {
        daily_data <- fmpc_price_history(ticker, startDate = "2004-01-01")
        if (length(daily_data) == 0) {
          return(datetime_ohlcv)
        }
        first_date <- as.POSIXct(daily_data$date[1])
        datetime_ohlcv <- datetime_ohlcv[datetime %between% c(first_date, Sys.time())]
        return(datetime_ohlcv)
      } else {
        return(datetime_ohlcv)
      }
    }
  }

  # if there is no splits, check for returns and return
  if (all(is.na(splits))) {
    splits <- content(GET("https://www.splithistory.com/", query = list(symbol = ticker))) %>%
      html_nodes(xpath = "//table[@width='208' and @style='font-family: Arial; font-size: 12px']") %>%
      html_table(header = TRUE) %>%
      .[[1]]
    splits$Date <- as.Date(splits$Date, "%m/%d/%Y")
    ratio1 <- as.numeric(str_extract(splits$Ratio, "\\d+"))
    ratio2 <- as.numeric(str_extract(splits$Ratio, "\\d+$"))
    splits$Ratio <- ratio2 / ratio1
    splits <- splits[splits$Date > as.Date("2004-01-01"), ]
    if (nrow(splits) == 0) {
      returns <- datetime_ohlcv$close / shift(datetime_ohlcv$close) - 1
      if (any(abs(returns) > 0.5, na.rm = TRUE)) {
        daily_data <- fmpc_price_history(ticker, startDate = "2004-01-01")
        if (length(daily_data) == 0) {
          return(datetime_ohlcv)
        }
        first_date <- as.POSIXct(daily_data$date[1])
        datetime_ohlcv <- datetime_ohlcv[datetime %between% c(first_date, Sys.time())]
        return(datetime_ohlcv)
      } else {
        return(datetime_ohlcv)
      }
    }
  }

  # change splits data for specific stocks
  if (ticker == "AFL") {
    splits <- splits[2, ]
  }

  # calculate split factor and erge to data
  splits <- as.data.table(splits)
  setnames(splits, colnames(splits), c("date", "split_factor"))
  splits$split_factor <- rev(cumprod(rev(splits$split_factor)))
  splits$date <- splits$date - 1
  if (ticker == 'WST') {
    splits$date[1] <- splits$date[1] + 1
  }

  # adjust OHLCV
  datetime_ohlcv <- datetime_ohlcv[, date := as.Date(datetime)]
  # datetime_ohlcv <- adjustment_eod[, .(date, splitFactor)][datetime_ohlcv, on = 'date']
  datetime_ohlcv <- splits[datetime_ohlcv, on = 'date', roll = -Inf]
  datetime_ohlcv[, split_factor := na.locf(split_factor, fromLast = TRUE, na.rm = FALSE)]
  datetime_ohlcv[is.na(split_factor), split_factor := 1]
  cols <- colnames(datetime_ohlcv)[4:ncol(datetime_ohlcv)]
  adjusted_values <- datetime_ohlcv[, lapply(.SD, function(x) x * split_factor), .SDcols = cols]
  adjusted_values[, volume := as.integer(volume)]
  adjusted_values <- cbind(datetime = datetime_ohlcv$datetime, adjusted_values)

  # calulate returns. If returns in period are higher than x, check for data
  returns <- adjusted_values$close / shift(adjusted_values$close) - 1
  if (any(abs(returns) > 0.5, na.rm = TRUE)) {
    daily_data <- fmpc_price_history(ticker, startDate = "2004-01-01")
    if (length(daily_data) == 0) {
      return(adjusted_values)
    }
    first_date <- as.POSIXct(daily_data$date[1])
    adjusted_values <- adjusted_values[datetime %between% c(first_date, Sys.time())]
  }
  return(adjusted_values)
}

stocks_sample <- stocks[symbol %in% unique(stocks$symbol)[1:100]]
stocks_adjusted <- stocks[, split_adjustment(.BY[[1]], data.table(datetime, open, high, close, low, volume)), by = symbol]
setorderv(stocks_adjusted, c("symbol", "datetime"))

# remove measurement erros
stocks_adjusted[, returns := close / shift(close) - 1, by = symbol]
returns <- stocks_adjusted$returns
outlier <- vector("integer", length(returns))
for (i in seq_along(returns)) {
  if (i == 1 | is.na(returns[i]) || is.na(returns[i-1])) {
    outlier[i] <- 0
  } else if ((abs(returns[i]) > 0.4 & abs(returns[i-1]) > 0.4) | (abs(returns[i]) > 0.4 & abs(returns[i-1]) > 0.4)) {
    outlier[i-1] <- 1
  } else {
    outlier[i] <- 0
  }

}
stocks_adjusted[, outliers := outlier]
stocks_adjusted[outliers == 1]
stocks_adjusted <- stocks_adjusted[outliers == 0]

# delete series with big gaps
stocks_adjusted[, time_gap := difftime(datetime, shift(datetime), units="hours"), by = symbol]
stocks_adjusted[which.max(time_gap)]
max(stocks_adjusted$time_gap, na.rm = TRUE)
stocks_adjusted[time_gap == max(time_gap, na.rm = TRUE)]

test_plot <- stocks_adjusted[symbol == "BKR"]
plot(test_plot$datetime, test_plot$close)

# additional tets
test <- copy(stocks_adjusted)
test[, return := close / shift(close) - 1, by = symbol]
test[abs(return) > 0.5, ]
ticker <- "ANR"
plot(test[symbol == ticker, datetime], test[symbol == ticker, close])
x <- test[symbol == ticker]
x[datetime %between% c("2004-09-27", "2004-10-03")]
x <- stocks[symbol == "CHK"]
x[datetime %between% c("2004-09-27", "2004-10-03")]

# shares with measurement errors: ANR CHK DJ(?)
# shares with breaks but good: AAL ADT AIG AIV

fmpc_symbol_search('ADT', 100)
x <- fmpc_symbols_index('historical')
head(x)
head(x[x$symbol == "ADT", ])


p <- GET("https://financialmodelingprep.com/api/v3/delisted-companies?limit=5000&apikey=15cd5d0adf4bc6805a724b4417bbaafc")
x <- content(p)
x <- lapply(x, as.data.table)
x <- rbindlist(x)
x[grep("ABK", symbol)]
x[grep("AMBAC", companyName, ignore.case = TRUE)]
