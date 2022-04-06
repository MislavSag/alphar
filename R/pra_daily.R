library(data.table)
library(euber)
library(fmpcloudr)
library(equityData)
library(httr)
library(rvest)
library(stringr)
library(QuantTools)
library(AzureStor)
library(readr)
library(ggplot2)
library(PerformanceAnalytics)
library(xts)



# set up
BLOBKEY=Sys.getenv("BLOB-KEY")
BLOBENDPOINT=Sys.getenv("BLOB-ENDPOINT")
CONTENDPOINT=storage_endpoint(BLOBENDPOINT, BLOBKEY)
CONT=storage_container(CONTENDPOINT, "qc-live")
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
prices <- prices[date > as.Date("1998-01-01")]

# indicators
windows <- seq(22, 22 * 48, by = 22 * 6)
cols <- paste0("pr_", windows)
prices[, (cols) := lapply(windows, function(w) roll_percent_rank(close, w)), by = "symbol"]
cols_above <- paste0("pr_above_dummy_", windows)
prices[, (cols_above) := lapply(.SD, function(x) ifelse(x > 0.999, 1, 0)), .SDcols = cols]
cols_below <- paste0("pr_below_dummy_", windows)
prices[, (cols_below) := lapply(.SD, function(x) ifelse(x < 0.001, 1, 0)), .SDcols = cols]

# get risk measures
indicators <- prices[, lapply(.SD, sum, na.rm = TRUE), .SDcols = c(cols_above, cols_below), by = .(date)]
# plot(indicators$pr_below_dummy_66, type = "l")
# plot(indicators[date %between% c("2020-01-01", "2021-01-01"), pr_below_dummy_66], type = "l")

# save to ayure blob
cols <- colnames(indicators)[c(1, grep( "_below_", colnames(indicators)))]
indicators_save <- indicators[, ..cols]
cols <- setdiff(cols, "date")
indicators_save[, (cols) := lapply(.SD, shift), .SDcols = cols]
indicators_save <- na.omit(indicators_save)
storage_write_csv2(indicators_save, CONT, file = "pra_daily.csv", col_names = FALSE)


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


# LEVEL TRADING RULE ------------------------------------------------------

# backtest data
backtest_data <- copy(indicators)
cols <- colnames(backtest_data)[c(1, grep( "_below_", colnames(backtest_data)))]
backtest_data <- backtest_data[, ..cols]
spy <- fmpcloudr::fmpc_price_history_spldiv("SPY", startDate = min(indicators$date), endDate = max(indicators$date))
spy <- as.data.table(spy[, c("symbol", "date", "adjClose")])
spy[, returns := adjClose / shift(adjClose) - 1]
backtest_data <- merge(spy, backtest_data, by = c("date"), all.x = TRUE, all.y = FALSE)
backtest_data <- backtest_data[date %between% c("2000-01-01", as.character(Sys.Date()))]

# optimizations loop
thresholds <- c(seq(1, 30, 1))
colnames(backtest_data)
variables <- colnames(backtest_data)[grep("_below_", colnames(backtest_data))]
params <- expand.grid(thresholds, variables, stringsAsFactors = FALSE)
returns_strategies <- list()
x <- vapply(1:nrow(params), function(i) backtest(backtest_data$returns,
                                                 backtest_data[, get(params[i, 2])],
                                                 params[i, 1]),
            numeric(1))
optim_results <- cbind(params, cum_return = x)
setorderv(optim_results, "cum_return")

# inspect
optim_results
ggplot(optim_results, aes(x = cum_return)) + geom_histogram() +
  geom_vline(xintercept = 4.1, color = "red") + facet_grid(. ~ Var2)
optim_results[optim_results$Var2 == "pr_below_dummy_176", ]
optim_results[optim_results$Var2 == "pr_below_dummy_1056", ]

# backtest individual
strategy_returns <- backtest(backtest_data$returns, backtest_data$pr_below_dummy_682, 3, return_cumulative = FALSE)
charts.PerformanceSummary(xts(cbind(backtest_data$returns, strategy_returns), order.by = backtest_data$date))
Performance(xts(strategy_returns, order.by = backtest_data$date))
Performance(xts(backtest_data$returns, order.by = backtest_data$datetime))

PerformanceAnalytics::Return.cumulative(backtest_data$returns)

