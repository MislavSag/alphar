library(data.table)
library(ggplot2)
library(xts)
library(PerformanceAnalytics)
library(TTR)
library(tidyr)
source('C:/Users/Mislav/Documents/GitHub/alphar/R/import_data.R')


# import exuber data
exuber_data <- lapply(list.files("D:/risks/radf", full.names = TRUE), fread)
exuber_data <- rbindlist(exuber_data)
exuber_data[, radf_sum := adf + sadf + gsadf + badf + bsadf]
# exuber_data[, time := format(datetime, "%H:%M:%S")]
# exuber_data <- exuber_data[time %between% c("08:00:00", "16:00:00")]

# price data
sp500_stocks <- import_intraday("D:/market_data/equity/usa/hour/trades_adjusted", "csv")
sp500_stocks <- sp500_stocks[symbol %in% exuber_data$symbol]
sp500_stocks[, returns := (close / shift(close)) - 1, by = .(symbol)]
sp500_stocks[, cum_returns := frollsum(returns, 10 * 8), by = .(symbol)]
sp500_stocks[, cum_returns := shift(cum_returns, type = "lag"), by = .(symbol)]
sp500_stocks <- na.omit(sp500_stocks)
sp500_stocks <- sp500_stocks[, .(symbol, datetime, returns, cum_returns)]

# merge exuber and stocks
exuber_data <- merge(exuber_data, sp500_stocks, by = c("symbol", "datetime"), all.x = TRUE, a.y = FALSE)
exuber_data[, negative_dummy := ifelse(bsadf > 1.25 & cum_returns < 0, 1, 0)]
exuber_data[, positive_dummy := ifelse(bsadf > 1.25 & cum_returns > 0, 1, 0)]
setorderv(exuber_data, c("symbol", "datetime"))

# define indicators based on exuber
radf_vars <- colnames(exuber_data)[3:ncol(exuber_data)]
indicators_median <- exuber_data[, lapply(.SD, median, na.rm = TRUE), by = 'datetime', .SDcols = radf_vars]
colnames(indicators_median)[2:ncol(indicators_median)] <- paste0("median_", colnames(indicators_median)[2:ncol(indicators_median)])
indicators_sd <- exuber_data[, lapply(.SD, sd, na.rm = TRUE), by = 'datetime', .SDcols = radf_vars]
colnames(indicators_sd)[2:ncol(indicators_sd)] <- paste0("sd_", colnames(indicators_sd)[2:ncol(indicators_sd)])
indicators_mean <- exuber_data[, lapply(.SD, mean, na.rm = TRUE), by = 'datetime', .SDcols = radf_vars]
colnames(indicators_mean)[2:ncol(indicators_mean)] <- paste0("mean_", colnames(indicators_mean)[2:ncol(indicators_mean)])
indicators_sum <- exuber_data[, lapply(.SD, sum, na.rm = TRUE), by = 'datetime', .SDcols = radf_vars]
colnames(indicators_sum)[2:ncol(indicators_sum)] <- paste0("sum_", colnames(indicators_sum)[2:ncol(indicators_sum)])

# merge indicators
indicators <- indicators_median[indicators_sd, on = "datetime"]
indicators <- indicators_mean[indicators, on = "datetime"]
indicators <- indicators_sum[indicators, on = "datetime"]
indicators <- indicators[order(datetime)]
indicators <- indicators[order(datetime)]
indicators <- na.omit(indicators)

# merge spy and indicators
spy <- fread("D:/market_data/equity/usa/hour/trades_adjusted/SPY.csv")
spy[, datetime := as.POSIXct(formated)]
spy <- spy[, .(datetime, c)]
setnames(spy, "c", "close")
spy[, time := format(datetime, "%H:%M:%S")]
spy <- spy[time %between% c("08:00:00", "16:00:00")]
spy[, returns := close / shift(close) - 1]
spy <- indicators[spy, on = "datetime"]
spy <- na.omit(spy)
spy <- spy[order(datetime)]

# plots radf_sum
ggplot(spy, aes(x = datetime)) +
  geom_line(aes(y = sd_radf_sum)) +
  geom_line(aes(y = SMA(sd_radf_sum, 10), color = "red"))
ggplot(spy[datetime %between% c("2020-01-01", "2021-01-01")], aes(x = datetime)) +
  geom_line(aes(y = sd_radf_sum)) +
  geom_line(aes(y = SMA(sd_radf_sum, 10), color = "red"))
ggplot(spy[datetime %between% c("2008-01-01", "2010-03-01")], aes(x = datetime)) +
  geom_line(aes(y = sd_radf_sum)) +
  geom_line(aes(y = SMA(sd_radf_sum, 10), color = "red"))


# geom_line(aes(y = EMA(sum_radf_sum)), color = 'red') +
#   geom_line(aes(y = SMA(sum_radf_sum)), color = 'blue') +
#   geom_line(aes(y = SMA(sum_radf_sum, 50)), color = 'brown') +
#   geom_line(aes(y = EMA(sum_radf_sum, 50)), color = 'purple')
ggplot(spy[datetime %between% c("2020-01-01", "2021-01-01")], aes(x = datetime)) +
  geom_line(aes(y = radf_sum)) +
  geom_line(aes(y = radf_ema), color = 'red') +
  geom_line(aes(y = radf_sma), color = 'blue') +
  geom_line(aes(y = radf_sma_long), color = 'brown') +
  geom_line(aes(y = radf_ema_long), color = 'purple')
ggplot(spy[datetime %between% c("2008-01-01", "2009-01-01")], aes(x = datetime)) +
  geom_line(aes(y = radf_sum)) +
  geom_line(aes(y = radf_ema), color = 'red') +
  geom_line(aes(y = radf_sma), color = 'blue') +
  geom_line(aes(y = radf_sma_long), color = 'brown') +
  geom_line(aes(y = radf_ema_long), color = 'purple')

# plots sadf
buy_dates <- factor(spy$median_bsadf > 0)
ggplot(spy, aes(x = datetime, y = close, group = buy_dates, color = buy_dates)) + geom_line()
buy_dates <- spy[datetime %between% c("2020-01-01", "2021-01-01")]
buy_dates <- factor(buy_dates$median_bsadf > 0)
ggplot(spy[datetime %between% c("2020-01-01", "2021-01-01")], aes(x = datetime, y = close, group = buy_dates, color = buy_dates)) + geom_line()

ggplot(spy, aes(x = datetime)) +
  geom_line(aes(y = sum_positive_dummy))
ggplot(spy, aes(x = datetime)) +
  geom_line(aes(y = mean_returns))
ggplot(spy[datetime %between% c("2020-01-01", "2021-01-01")], aes(x = datetime)) +
  geom_line(aes(y = mean_returns)) +
  geom_line(aes(y = SMA(sum_bsadf, 20)), color = "red")
ggplot(spy[datetime %between% c("2008-01-01", "2009-01-01")], aes(x = datetime)) +
  geom_line(aes(y = mean_returns))

# optimization
thresholds <- c(-0.5, 0, 0.5, 1, 1.5, 2, 2.5, 3, 4, 5, 10, 50, 100, 200)
variables <- colnames(spy)[2:(ncol(spy) - 2)]
params <- expand.grid(thresholds, variables, stringsAsFactors = FALSE)
optimization_data <- na.omit(spy)
backtest <- function(returns, indicator, threshold) {
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
  PerformanceAnalytics::Return.cumulative(returns_strategy)

}
returns_strategies <- vapply(1:nrow(params), function(i) backtest(optimization_data$returns,
                                                                  # optimization_data[, get(params[i, 2])],
                                                                  SMA(optimization_data[, get(params[i, 2])], 20),
                                                                  params[i, 1]),
                             numeric(1))
returns_strategies <- cbind(params, returns_strategies)
head(returns_strategies[order(returns_strategies$returns_strategies, decreasing = TRUE), ], 20)


# individual backtests
backtest_individual <- function(returns, indicator, threshold) {
  sides <- vector("integer", length(indicator))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(indicator[i-1])) {
      sides[i] <- NA
    } else if (indicator[i-1] > threshold) { # | indicator[i-1] < -threshold) {
      sides[i] <- 0
    } else {
      sides[i] <- 1
    }
  }
  sides <- ifelse(is.na(sides), 1, sides)
  print(table(sides))
  returns_strategy <- returns * sides
  return(returns_strategy)
}
returns_strategy <- backtest(backtest_data$returns, SMA(backtest_data$sum_bsadf, 5), 8)
backtest_data[, returns_strategy := returns_strategy]
backest_xts <- xts(cbind(backtest_data$returns, returns_strategy), order.by = backtest_data$datetime)
charts.PerformanceSummary(backest_xts, plot.engine = "ggplot2")

# individual backtests
x <- backtest_data[, .(datetime, returns_strategy, SMA(sum_radf_sum, 2))]
x <- x[datetime %between% c("2008-01-01", "2010-01-01")]
x[101:200]


# save for Quantconnect
# save_data <- spy[, .(datetime, adf, sadf, gsadf, badf, bsadf, radf_sum)]
# save_data[, datetime := format(datetime, "%Y-%m-%d %H")]
fwrite(spy, "D:/risks/exuber_data.csv", col.names = FALSE)
colnames(spy)




# EVENT STUDY -------------------------------------------------------------

# prepare data
sample <- exuber_data[symbol %in% c("AAPL", "AAL", "F", "TSLA", "AMZN", "V", "T")]
event_dates <- sample[sadf > 1.5, .(symbol, datetime)]
setnames(event_dates, colnames(event_dates), c("name", "when"))
stock_price_returns <- na.omit(sample[, .(symbol, datetime, returns)])
# stock_price_returns[, returns := returns * 100]
stock_price_returns <- unique(stock_price_returns, by = c("symbol", "datetime"))
stock_price_returns <- as.data.table(pivot_wider(stock_price_returns, datetime, names_from = symbol, values_from = returns))
stock_price_returns <- as.xts.data.table(stock_price_returns)
head(stock_price_returns)

# event study
library(eventstudies)
es <- eventstudy(firm.returns = stock_price_returns,
                 event.list = event_dates,
                 event.window = 24,
                 type = "None",
                 to.remap = TRUE,
                 remap = "cumsum",
                 inference = TRUE,
                 inference.strategy = "bootstrap")
es


firm.returns = stock_price_returns
event.list = event_dates
event.window = 10
is.levels =  FALSE
type = "None"
to.remap = TRUE
remap = "cumsum"
inference = TRUE
inference.strategy = "bootstrap"
model.args = NULL


if (type == "None" && !is.null(firm.returns)) {
  outputModel <- firm.returns
  if (length(model.args) != 0) {
    warning(deparse("type"), " = ", deparse("None"),
            " does not take extra arguments, ignoring them.")
  }
}

if (!(type %in% c("None", "constantMeanReturn")) && is.null(model.args)) {
  stop("model.args cannot be NULL when 'type' is not 'None' or 'constantMeanReturn'.")
}

if (is.levels == TRUE) {
  firm.returns <- diff(log(firm.returns)) * 100
}

## handle single series
if (is.null(ncol(firm.returns))) {
  stop("firm.returns should be a zoo series with at least one column. Use '[' with 'drop = FALSE'.")
}

stopifnot(!is.null(remap))

## compute estimation and event period
## event period starts from event time + 1
event.period <- as.character((-event.window + 1):event.window)


returns.zoo <- prepare.returns(event.list = event.list,
                               event.window = event.window,
                               list(firm.returns = firm.returns))
outcomes <- do.call(c, sapply(returns.zoo, '[', "outcomes"))
names(outcomes) <- gsub(".outcomes", "", names(outcomes))
if (all(unique(outcomes) != "success")) {
  message("Error: no successful events")
  to.remap = FALSE
  inference = FALSE
  outputModel <- NULL
} else {
  returns.zoo <- returns.zoo[which(outcomes == "success")]
  outputModel <- lapply(returns.zoo, function(firm) {
    if (is.null(firm$z.e)) {
      return(NULL)
    }
    estimation.period <- attributes(firm)[["estimation.period"]]
    abnormal.returns <- firm$z.e[event.period]
    return(abnormal.returns)
  })
  null.values <- sapply(outputModel, is.null)
  if (length(which(null.values)) > 0) {
    outputModel <- outputModel[names(which(!null.values))]
    outcomes[names(which(null.values))] <- "edatamissing"
  }

  if (length(outputModel) == 0) {
    warning("None() returned NULL\n")
    outputModel <- NULL
  } else {
    outputModel <- do.call(merge.zoo,
                           outputModel[!sapply(outputModel,
                                               is.null)])
  }
}



event.list = event.list
event.window = event.window
x = list(firm.returns = firm.returns)


library(future.apply)
plan(multicore(workers = 16L))

prepare.returns <- function(event.list, event.window, ...) {
  returns <- unlist(list(x), recursive = FALSE)
  other.returns.names <- names(returns)[-match("firm.returns", names(returns))]

  if (length(other.returns.names) != 0) { # check for type = "None"
    # and "constantMeanReturn"
    returns.zoo <- lapply(1:nrow(event.list), function(i) {
      firm.name <- event.list[i, "name"]
      # to pick out the common dates
      # of data. can't work on event
      # time if the dates of data do
      # not match before converting
      # to event time.
      # all = FALSE: pick up dates
      # for which data is available
      # for all types of returns
      firm.merged <- do.call("merge.zoo",
                             c(list(firm.returns = returns$firm.returns[, firm.name]),
                               returns[other.returns.names],
                               all = FALSE, fill = NA))
      ## other.returns.names needs re-assignment here, since "returns"
      ## may have a data.frame as one of the elements, as in case of
      ## lmAMM.
      other.returns.names <- colnames(firm.merged)[-match("firm.returns",
                                                          colnames(firm.merged))]

      firm.returns.eventtime <- phys2eventtime(z = firm.merged,
                                               events = rbind(
                                                 data.frame(name = "firm.returns", when = event.list[i, "when"],
                                                            stringsAsFactors = FALSE),
                                                 data.frame(name = other.returns.names, when = event.list[i, "when"],
                                                            stringsAsFactors = FALSE)),
                                               width = event.window)

      if (any(firm.returns.eventtime$outcomes == "unitmissing")) {
        ## :DOC: there could be NAs in firm and other returns in the merged object
        return(list(z.e = NULL, outcomes = "unitmissing")) # phys2eventtime output object
      }

      if (any(firm.returns.eventtime$outcomes == "wdatamissing")) {
        return(list(z.e = NULL, outcomes = "wdatamissing")) # phys2eventtime output object
      }

      if (any(firm.returns.eventtime$outcomes == "wrongspan")) {
        ## :DOC: there could be NAs in firm and other returns in the merged object
        return(list(z.e = NULL, outcomes = "wrongspan")) # phys2eventtime output object
      }

      firm.returns.eventtime$outcomes <- "success" # keep one value

      colnames(firm.returns.eventtime$z.e) <- c("firm.returns", other.returns.names)
      ## :DOC: estimation period goes till event time (inclusive)
      attr(firm.returns.eventtime, which = "estimation.period") <-
        as.character(index(firm.returns.eventtime$z.e)[1]:(-event.window))

      return(firm.returns.eventtime)
    })
    names(returns.zoo) <- 1:nrow(event.list)

  } else {

    returns.zoo <- future_lapply(1:nrow(event.list),  function(i) {
      firm.returns.eventtime <- phys2eventtime(z = returns$firm.returns,
                                               events = event.list[i, ],
                                               width = event.window)
      if (any(firm.returns.eventtime$outcomes == "unitmissing")) {
        return(list(z.e = NULL, outcomes = "unitmissing"))
      }

      if (any(firm.returns.eventtime$outcomes == "wdatamissing")) {
        return(list(z.e = NULL, outcomes = "wdatamissing"))
      }

      if (any(firm.returns.eventtime$outcomes == "wrongspan")) {
        return(list(z.e = NULL, outcomes = "wrongspan"))
      }
      firm.returns.eventtime$outcomes <- "success"
      attr(firm.returns.eventtime, which = "estimation.period") <-
        as.character(index(firm.returns.eventtime$z.e)[1]:(-event.window))
      return(firm.returns.eventtime)
    })
    names(returns.zoo) <- 1:nrow(event.list)
  }
  return(returns.zoo)
}
