library(data.table)
library(ggplot2)
library(TTR)
library(PerformanceAnalytics)
source('C:/Users/Mislav/Documents/GitHub/alphar/R/import_data.R')


# import data
save_path <- "D:/risks"
stocks <- import_intraday('D:/market_data/equity/usa/hour/trades_adjusted', 'csv')
sp500 <- c("SPY", import_sp500())
stocks <- stocks[symbol %in% sp500]
stocks[, returns := close / shift(close) - 1, by = symbol]
stocks <- na.omit(stocks)
spy <- stocks[symbol == "SPY"]

#
DT <- stocks[abs(returns) > 0.04 & abs(returns) < 0.5]
nrow(DT) / nrow(stocks)
DT <- DT[, sum(returns, na.rm = TRUE), by = datetime]
DT <- DT[spy, on = 'datetime']
DT <- setorder(DT, datetime)
sum(is.na(DT$V1)) / nrow(DT)
DT[, indicator := ifelse(is.na(V1), 0, V1)]
DT[, indicator_sma := roll::roll_sd(indicator, 8*2)]
ggplot(DT, aes(datetime)) +
  geom_line(aes(y = indicator_sma), color = 'blue')
ggplot(DT[datetime %between% c('2020-03-01', '2020-06-01')], aes(datetime)) +
  geom_line(aes(y = indicator)) +
  geom_line(aes(y = indicator_sma), color = 'blue')
ggplot(DT[datetime %between% c('2009-01-01', '2010-01-01')], aes(datetime)) +
  geom_line(aes(y = indicator_sma), color = 'blue')


# backtest
sides <- vector('integer', nrow(DT))
ind <- DT$indicator_sma
for (i in seq_along(sides)) {
  if (i %in% 1:1 || is.na(ind[i-1])) {
    sides[i] <- NA
  } else if (ind[i-1] > 0.2) {
    sides[i] <- 0
  } else {
    sides[i] <- 1
  }
}
table(sides)
DT[, returns_strategy := returns * sides]
charts.PerformanceSummary(DT[, .(datetime, returns, returns_strategy)], plot.engine = 'ggplot2')
# charts.PerformanceSummary(DT[datetime %between% c('2009-01-01', '2010-01-01'), .(datetime, returns, returns_strategy)], plot.engine = 'ggplot2')
