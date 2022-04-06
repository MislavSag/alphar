library(data.table)
library(euber)
library(fmpcloudr)
library(equityData)
library(httr)
library(rvest)
library(stringr)
library(TTR)
require(finfeatures, lib.loc = "C:/Users/Mislav/Documents/GitHub/finfeatures/renv/library/R-4.1/x86_64-w64-mingw32")


# set up
fmpcloudr::fmpc_set_token(Sys.getenv("APIKEY-FMPCLOUD"))
save_path = file.path("D:/fundamental_data")

# get sp 500 stocks
SP500 <- content(GET(paste0('https://financialmodelingprep.com/api/v3/sp500_constituent?apikey=', Sys.getenv("APIKEY-FMPCLOUD"))))
SP500 <- rbindlist(SP500)
SP500_DELISTED <- content(GET(paste0('https://financialmodelingprep.com/api/v3/historical/sp500_constituent?apikey=', Sys.getenv("APIKEY-FMPCLOUD"))))
SP500_DELISTED <- rbindlist(SP500_DELISTED)
SP500_SYMBOLS <- unique(c(SP500$symbol, SP500_DELISTED$symbol))
SP500_SYMBOLS <- c("SPY", SP500_SYMBOLS) # manually add SPY symbol

# get ticker changes
get_ticker_changes <- function(ticker) {

  p <- RETRY("POST",
             'https://www.quantumonline.com/search.cfm',
             body = list(
               tickersymbol = ticker,
               sopt = 'symbol',
               '1.0.1' = 'Search'
             ),
             times = 8L)
  changes <- content(p) %>%
    html_elements(xpath = "//*[contains(text(),'Previous Ticker')]") %>%
    html_text() %>%
    gsub('.*Symbol:', '', .) %>%
    trimws(.)
  date <- as.Date(str_extract(changes, '\\d+/\\d+/\\d+'), '%m/%d/%Y')
  tickers <- str_extract(changes, '\\w+')
  changes <- data.table(ticker = ticker, date = date, ticker_change = tickers)
  return(changes)
}

# get changes
SP500_CHANGES <- lapply(SP500_SYMBOLS, get_ticker_changes)
SP500_CHANGES <- rbindlist(SP500_CHANGES)
SP500_SYMBOLS <- unique(c(SP500_SYMBOLS, SP500_CHANGES$ticker_change))

# import data
# get daily market data for all stocks
prices <- get_blob_file("prices.rds",
                        container = "fundamentals",
                        save_file = file.path(save_path, "prices.rds"),
                        refresh_data_old = 30,
                        overwrite = TRUE)
prices <- unique(prices, by = c("symbol", "date"))
prices <- prices[open > 0 & high > 0 & low > 0 & close > 0 & adjClose > 0] # remove rows with zero and negative prices
setorderv(prices, c("symbol", "date"))
prices[, returns := adjClose / data.table::shift(adjClose) - 1, by = symbol]
prices <- prices[returns < 1] # remove observations where returns are higher than 100%. TODO:: better outlier detection mechanism
adjust_cols <- c("open", "high", "low")
prices[, (adjust_cols) := lapply(.SD, function(x) x * (adjClose / close)), .SDcols = adjust_cols] # adjust open, high and low prices
prices[, close := adjClose]
prices <- na.omit(prices[, .(symbol, date, open, high, low, close, volume, vwap, returns)])
prices_n <- prices[, .N, by = symbol]
prices_n <- prices_n[which(prices_n$N > 1000)]  # remove prices with only 60 or less observations
prices <- prices[symbol %in% SP500_SYMBOLS]
prices <- prices[date > as.Date("2000-01-01")]

#
OhlcvInstance = Ohlcv$new(prices, id_col = "symbol", date_col = "date")
RollingExuberInit = RollingExuber$new(windows = 200,
                                      workers = 4L,
                                      at = 1:nrow(prices),
                                      lag = 1L,
                                      na_pad = TRUE,
                                      simplify = FALSE)
RollingExuberFeatures = RollingExuberInit$get_rolling_features(OhlcvInstance)

exuber_dt <- copy(RollingExuberFeatures)
colnames(exuber_dt)[3:ncol(exuber_dt)] <- c('adf', 'sadf', 'gsadf', 'badf', 'bsadf')
exuber_dt[, radf_sum := adf + sadf + gsadf + badf + bsadf]
exuber_dt <- exuber_dt[, .(symbol, date, adf, sadf, gsadf, badf, bsadf, radf_sum)]
# attributes(exuber_dt$datetime)$tzone <- "EST" ############################### VIDJETI STO S OVIM ######################

# define indicators based on exuber
radf_vars <- colnames(exuber_dt)[3:ncol(exuber_dt)]
indicators_median <- exuber_dt[, lapply(.SD, median, na.rm = TRUE), by = c('date'), .SDcols = radf_vars]
colnames(indicators_median)[2:ncol(indicators_median)] <- paste0("median_", colnames(indicators_median)[2:ncol(indicators_median)])
indicators_sd <- exuber_dt[, lapply(.SD, sd, na.rm = TRUE), by = c('date'), .SDcols = radf_vars]
colnames(indicators_sd)[2:ncol(indicators_sd)] <- paste0("sd_", colnames(indicators_sd)[2:ncol(indicators_sd)])
indicators_mean <- exuber_dt[, lapply(.SD, mean, na.rm = TRUE), by = c('date'), .SDcols = radf_vars]
colnames(indicators_mean)[2:ncol(indicators_mean)] <- paste0("mean_", colnames(indicators_mean)[2:ncol(indicators_mean)])
indicators_sum <- exuber_dt[, lapply(.SD, sum, na.rm = TRUE), by = c('date'), .SDcols = radf_vars]
colnames(indicators_sum)[2:ncol(indicators_sum)] <- paste0("sum_", colnames(indicators_sum)[2:ncol(indicators_sum)])

# merge indicators
indicators <- merge(indicators_sd, indicators_median, by = c('date'))
indicators <- merge(indicators, indicators_mean, by = c('date'))
indicators <- merge(indicators, indicators_sum, by = c('date'))
setorderv(indicators, c('date'))
indicators <- na.omit(indicators)

# save
library(AzureStor)
BLOBKEY=Sys.getenv("BLOB-KEY")
BLOBENDPOINT=Sys.getenv("BLOB-ENDPOINT")
CONTENDPOINT=storage_endpoint(BLOBENDPOINT, BLOBKEY)
CONT=storage_container(CONTENDPOINT, "qc-live")
indicators_save <- copy(indicators)
cols <- colnames(indicators_save)[2:ncol(indicators_save)]
indicators_save[, (cols) := lapply(.SD, shift), .SDcols = cols]
indicators_save <- na.omit(indicators_save)
storage_write_csv2(indicators_save, CONT, file = "exuber_daily.csv", col_names = FALSE)
plot(indicators_save[sd_radf_sum  < 10, sd_radf_sum])

# backtest data
backtest_data <- copy(indicators)
cols <- colnames(backtest_data)[c(1, grep( "sd_", colnames(backtest_data)))]
backtest_data <- backtest_data[, ..cols]
spy <- fmpcloudr::fmpc_price_history_spldiv("SPY", startDate = min(backtest_data$date), endDate = max(backtest_data$date))
spy <- as.data.table(spy[, c("symbol", "date", "adjClose")])
spy[, returns := adjClose / shift(adjClose) - 1]
backtest_data <- merge(spy, backtest_data, by = c("date"), all.x = TRUE, all.y = FALSE)
backtest_data <- backtest_data[date %between% c("2000-01-01", as.character(Sys.Date()))]
backtest_data <- na.omit(backtest_data)

# optimization params
thresholds <- c(seq(2.5, 5, 0.1))
variables <- colnames(backtest_data)[c(5:10)]
sma_window <- c(1, 2, 4, 8, 16, 32)
params <- expand.grid(thresholds, variables, sma_window, stringsAsFactors = FALSE)
colnames(params) <- c("thresholds", "variables", "sma_window")


# backtst function
backtest <- function(returns, indicator, threshold, return_cumulative = TRUE) {
  sides <- vector("integer", length(indicator))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(indicator[i-1])) {
      sides[i] <- NA
    } else if (indicator[i-1] > threshold) {
      sides[i] <- 0
    } else {
      sides[i] <- 1
    }
  }
  sides <- ifelse(is.na(sides), 1, sides)
  returns_strategy <- returns * sides
  if (return_cumulative) {
    return(PerformanceAnalytics::Return.cumulative(returns_strategy))
  } else {
    return(returns_strategy)
  }
}

# optimizations loop
returns_strategies <- list()
x <- vapply(1:nrow(params), function(i) backtest(backtest_data$returns,
                                                 SMA(backtest_data[, get(params[i, 2])], params[i, 3]),
                                                 params[i, 1]),
            numeric(1))
returns_strategies <- cbind(params, x)
setorderv(returns_strategies, "x")
tail(returns_strategies, 10)

# individual
threshold <- 3.9
strategy_returns <- backtest(backtest_data$returns, backtest_data$sd_radf_sum, threshold, return_cumulative = FALSE)
PerformanceAnalytics::charts.PerformanceSummary(xts::xts(cbind(backtest_data$returns, strategy_returns), order.by = backtest_data$date))
Performance(xts(strategy_returns, order.by = backtest_data$datetime))
Performance(xts(backtest_data$returns, order.by = backtest_data$datetime))
