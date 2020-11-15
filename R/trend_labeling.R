library(BatchGetSymbols)
library(slider)
library(tidyquant)
library(ggplot2)
library(ggpubr)
library(PerformanceAnalytics)



# DATA --------------------------------------------------------------------


# params
first.date <- Sys.Date() - 10000
freq.data <- 'daily'
tickers <- c('SPY')

# get data
l.out <- BatchGetSymbols(tickers = tickers,
                         first.date = first.date,
                         last.date = Sys.Date(),
                         freq.data = freq.data,
                         cache.folder = file.path(tempdir(),
                                                  'BGS_Cache') ) # cache in tempdir()

market_data <- l.out$df.tickers
close <- market_data[, c('ref.date', 'price.close')]
head(close)

# roll data
lambda <- 11
X <- slide_dfr(.x = close, .f = function(x) x - mean(x), .before = lambda - 1, .complete = TRUE)



# LABELING ---------------------------------------------------------

trend_labeling <- function(price, time, w) {
  # init vars
  fp <- price[1]  # represents the first price obtained by the algorithm
  xh <- price[1]  # mark the highest price
  ht <- time[1]  # mark the time when the highest price occurs
  xl <- price[1]  #  mark the lowest price
  lt <- time[1]  # mark the time when the lowest price occurs
  cid  <- 0  # mark the current direction of labeling
  fp_n  <- 1  # the index of the highest or lowest point obtained initially

  # fist loop
  for (i in 1:nrow(close)) {
    if (price[i] > (fp + (fp * w))) {
      xh <- price[i]
      ht <- time[i]
      fp_n <- i
      cid <- 1
      print(paste0("defineprice ", fp, " highest_price   break  ", i, ", time: ",
                   ht, "   value: ", xh))
      break()
    }
    if (price[i] < (fp - (fp * w))) {
      xl <- price[i]
      lt <- time[i]
      fp_n <- i
      cid <- -1
      print(paste0("defineprice ", fp, " highest_price   break  ", i, ", time: ",
                   lt, "   value: ", xl))
      break()
    }
  }

  y <- c()
  # second loop
  for (i in (fp_n + 1):nrow(close)) {
    newprice <- price[i]
    newdatetime <- time[i]

    if (cid > 0) {
      if (newprice > xh) {
        xh <- newprice
        ht <- newdatetime
      }
      if ((newprice < (xh - (xh * w))) && (lt < ht)) {
        print(paste0(newdatetime, ' 1'))
        for (j in 1:nrow(close)) {
          newdatetime_j <- time[j]
          if (newdatetime_j > lt & newdatetime_j <= ht) {
            y[j] <- 1
          }
        }
        xl <- newprice
        lt <- newdatetime
        cid <- -1
      }
    }

    if (cid < 0) {
      if (newprice < xl) {
        xl <- newprice
        lt <- newdatetime
      }
      if ((newprice > (xl + (xl * w))) && (ht < lt)) {
        print(paste0(newdatetime, ' -1'))
        for (j in 1:nrow(close)) {
          newdatetime_j <- time[j]
          if (newdatetime_j > ht & newdatetime_j <= lt) {
            y[j] <- -1
          }
        }
        xh <- newprice
        ht <- newdatetime
        cid <- 1
      }
    }
  }

  # mannually add last segmenet label
  y[(length(y) + 1):nrow(close)] <- -tail(y, 1)
  y[1] <- y[2]

  return(y)
}

y <- trend_labeling(close$price.close, close$ref.date, 0.10)

# graph
df <- cbind.data.frame(market_data, y)
candle <- ggplot2::ggplot(df, aes(x = ref.date, y = price.close)) +
  geom_candlestick(aes(open = price.open, high = price.high, low = price.low, close = price.close))
label <- ggplot2::ggplot(df, aes(x = ref.date, y = y)) +
  geom_line()
ggpubr::ggarrange(candle, label, nrow = 2)

# simple strategy
for (i in 1:nrow(strategy)) {
  if (i == 1) {
    bets <- NA
  } else if (y[i-1] == -1) {
    bets <- c(bets, 0)
  } else if (y[i-1] == 1) {
    bets <- c(bets, 1)
  }
}
perf <- cbind.data.frame(benchmark=df$ret.adjusted.prices, strategy = df$ret.adjusted.prices * bets)
perf <- xts::xts(perf, order.by = df$ref.date)
charts.PerformanceSummary(perf)
