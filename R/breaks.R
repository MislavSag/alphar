# fundamental
library(data.table)
library(purrr)
library(xts)
library(tsbox)
library(slider)
library(tidyr)
library(stringr)
library(ggplot2)
library(ggpubr)
# data
library(IBrokers)
# alpha packages
library(exuber)
library(dpseg)
library(strucchange)
library(backCUSUM)
# backtest and finance
library(quantmod)
library(PerformanceAnalytics)
# other
library(runner)
library(future.apply)
source('R/parallel_functions.R')
source('R/outliers.R')
source('R/import_data.R')



# PARAMETERS --------------------------------------------------------------

# performance
plan(multiprocess)  # for multithreading  workers = availableCores() - 8)
# market data
contract <- 'SPY5'
freq <- c('1 day')
period <- c(15)
data_length <- '15 Y'
use_logs <- TRUE
cache_path <- 'D:/algo_trading_files/ib_cache_r'
save_path <- 'D:/algo_trading_files/alphar'
# exuber
exuber_rolling_window <- c(600)
adf_lags <- c(1)
keep_last <- TRUE
# dpseg
dpseg_rolling <- c(600, 1000)
dpseg_type <- 'var'



# IMPORT DATA -------------------------------------------------------------


# HFD
market_data <- import_mysql(
  contract = contract,
  save_path = 'D:/market_data/usa/ohlcv',
  trading_days = TRUE,
  upsample = upsample,
  RMySQL::MySQL(),
  dbname = 'odvjet12_market_data_usa',
  username = 'odvjet12_mislav',
  password = 'Theanswer0207',
  host = '91.234.46.219'
)
vix <- import_mysql(
  contract = 'VIX5',
  save_path = 'D:/market_data/usa/ohlcv',
  trading_days = TRUE,
  upsample = upsample,
  RMySQL::MySQL(),
  dbname = 'odvjet12_market_data_usa',
  username = 'odvjet12_mislav',
  password = 'Theanswer0207',
  host = '91.234.46.219'
)

# LFD
market_data <- import_ib(
  contract = 'SPY',
  frequencies = '1 day',
  duration = '15 Y',
  type = 'equity',
  trading_days = FALSE
)
colnames(market_data) <- c('open', 'high', 'low', 'close', 'volume', 'WAP', 'hasGaps', 'count')
vix <- import_ib(
  contract = 'VIX',
  frequencies = '1 day',
  duration = '16 Y',
  type = 'index',
  trading_days = FALSE
)
colnames(vix) <- c('open', 'high', 'low', 'close', 'volume', 'WAP', 'hasGaps', 'count')


# PREPROCESSING -----------------------------------------------------------


# Remove outliers
market_data <- remove_outlier_median(market_data, median_scaler = 25)

# merge market data and VIX
market_data <- merge(market_data, vix[, 'close'], join = 'left')
colnames(market_data)[ncol(market_data)] <- 'vix'
market_data <- na.omit(market_data)

# Add features
market_data <- add_features(market_data)

# lags
market_data$returns_lag <- data.table::shift(market_data$returns)
market_data$vix_lag <- data.table::shift(market_data$vix)

# Remove NA values
market_data <- na.omit(market_data)



# EXUBER STRATEGY ---------------------------------------------------------


# radf roll
radf_roll <- lapply(market_data, function(x) {
  lapply(exuber_rolling_window, function(y) {
    lapply(adf_lags, function(z) {
      keep_last <- keep_last
      x_ <- as.data.frame(as.data.table(x))
      minw_ <- exuber::psy_minw(y)
      colnames(x_) <- c('time', 'open', 'high', 'low', 'close')
      slider_parallel(
        .x = x_,
        n_cores = -1,
        .f = ~ {
          radf_ <- exuber::radf(.x$close, minw = minw_, lag = z)
          radf_ <- lapply(radf_, function(x) {
            if (length(x) == 1) {
              x <- cbind.data.frame(.[nrow(.), ], x)
            } else if (length(x) > 1) {
              x <- cbind.data.frame(.[(minw_ + z + 1):nrow(.), ], x)
              if (keep_last) {x <- tail(x, 1)}
            }
            x
          })
          radf_
        },
        .before = y - 1,
        .complete = TRUE
        )
      })
    })
})
radf <- purrr::flatten(purrr::flatten(radf_roll))
radf <- lapply(radf, function(x) {
  lapply(x, function(y) {
    purrr::pluck(y, 'bsadf')
    })
  })
radf <- lapply(radf, function(x) {
  x <- na.omit(do.call(rbind, x))
  }
)

# # test
# test <- radf(as.data.fram(market_data$SPY_1_hour$SPY.Close), 50, 1)
# test_ <- cbind.data.frame(as.data.table(market_data$SPY_1_day[(50+2):nrow(market_data$SPY_1_day)]), test$bsadf)
# colnames(test_) <- c('time', 'open', 'high', 'low', 'close', 'series1')
# test_ <- alpha_apply_exuber(test_)
# PerformanceAnalytics::charts.PerformanceSummary(test_$perf)
# tidy(test)
# augment(test)
# summary(test)
# dstamp_stocks <- datestamp(test)
# dstamp_stocks
# tail(test$bsadf, 22)
# autoplot(test)
# crit <- augment_join(test, radf_crit$n600)

# save radf values
elements_names <- tidyr::expand_grid('exuber', symbols, freq, exuber_rolling_window, 'window', adf_lags, 'lag')
elements_names <- do.call(paste, c(elements_names, sep = '_'))
elements_names <- gsub(' ', '_', elements_names)
# purrr::map2(radf, paste0(elements_names, '.csv'), ~ {
#   write.csv2(.x, file.path(save_path, .y), row.names = FALSE)
# })

# alpha
alpha_apply_exuber <- function(data, use_short = FALSE,
                               critical_value = 1,
                               returns = c('close', 'open_close')) {
  returns <- match.arg(returns)
  if (returns == 'close') {
    # data$returns <- (data$close - shift(data$close, 1L)) / shift(data$close, 1L)
    data$returns <- data$close - shift(data$close, 1L)
  } else if (returns == 'open_close') {
    data$returns <- data$open - shift(data$close, 1L)
  }
  data <- na.omit(data)
  for (i in 1:nrow(data)) {
    if (i %in% (1:1)) {
      signs <- rep(NA, 1)
    } else if (data[i-1, 'series1'] < critical_value) {
      signs <- c(signs, 1)
    } else if (all(data[(i-1):(i-1), 'series1'] > critical_value)) {
    # } else if (((data[i-1, 'series1'] > critical_value) & (data[i-1, 'returns'] < 0)) |
    #            (tail(signs, 1) %in% c(NA, 0) & (data[i-1, 'series1'] > critical_value))) {
    # } else if (data[i-1, 'series1'] >= 1) {
      signs <- c(signs, 0)
    } else {
      signs <- c(signs, 1)
    }
  }
  data$signs <- signs
  data$returns_strategy <- data$returns * data$sign
  data <- na.omit(data)
  perf <- xts::xts(data[, c('returns', 'returns_strategy')], order.by = data$time)
  return(list(data = data, perf = perf))
}

# backtest
alpha_exuber <- lapply(radf, alpha_apply_exuber, critical_value = 1, returns = 'open_close')
g <- purrr::map2(lapply(alpha_exuber, '[[', 2), as.list(elements_names),
                 ~ {
                   ggplotify::as.ggplot(charts.PerformanceSummary(.x, plot.engine = 'ggplot2')) +
                     ggtitle(.y)
                 })

# print ggplots
for (i in 1:length(g)) {
  print(g[[i]])
  Sys.sleep(3L)
}

# source: https://stackoverflow.com/questions/31731611/use-browser-as-viewer-for-ggplot2-via-r
# ggplotToBrowser <- function(p) {
#   ggsave(filename = tf_img <- tempfile(fileext = ".png"), plot = p)
#   html <- sprintf('<html><body><img src="%s"></body></html>', paste0("file:///", tf_img))
#   cat(html, file = tf_html <- tempfile(fileext = ".html"))
#   shell.exec(tf_html) # or system(sprintf("open %s", tf_html))
# }
# ggplotToBrowser(g[[1]])
#
# table.Stats(alpha_exuber[[1]])
# table.Arbitrary(alpha_exuber[[1]])
# table.CalendarReturns(alpha_exuber[[1]])


# MOVE THESE TO TESTS
# result_parallel <- na.omit(do.call(c, roll_par))
# results <- na.omit(rolling_radf)
# length(results)
# length(result_parallel)
# length(results) - length(result_parallel)
#
# all(head(result_parallel, 50) == head(results, 50))
# all(result_parallel == results)
# head(result_parallel, 50)
# head(results, 50)
# tail(result_parallel[510:515])
# tail(results[510:515])



# DYNAMIC PROGRAMING SEGMENTS STRATEGY ------------------------------------


# Alpha function
dpseg_last <- function(data, ...) {
  if (is.na(p)) {
    p <- estimateP(x=data$time, y=data$price, plot=FALSE)
  }
  segs <- dpseg(data$time, data$price, jumps=TRUE, store.matrix=TRUE, verb=FALSE, ...)
  slope_last <- segs$segments$slope[length(segs$segments$slope)]
  return(slope_last)
}


# Daily roll
dpseg_roll <- function(data, rolling_window = 600, ...) {
  dpseg_data <- cbind.data.frame(time = zoo::index(data), price = Cl(data))
  colnames(dpseg_data) <- c('time', 'price')
  DT <- as.data.table(dpseg_data)
  DT[, time := as.numeric(time)]
  DT[,
     slope := runner(
       x = .SD,
       f = function(x) {
         segs <- dpseg(x$time, x$price, jumps=TRUE, store.matrix=TRUE, verb=FALSE, P = 0.5)
         slope_last <- segs$segments$slope[length(segs$segments$slope)]
         slope_last
         },
       k = rolling_window,
       na_pad = TRUE
     )]
  results <- xts::xts(DT[, c(2, 3)], order.by = dpseg_data$time)
  results <- na.omit(results)
  return(results)
}

dpseg_roll <- function(data, window_size = 100) {
  DT <- as.data.table(market_data)[, .(time = index, price = close)][, time := as.numeric(time)]
  dpseg_last <- slider_parallel(
    .x = DT,
    .f =   ~ {
      library(dpseg)
      segs <- dpseg(.x$time, .x$price, jumps=FALSE, store.matrix=TRUE, verb=FALSE, P = 0.1)
      slope_last <- segs$segments$slope[length(segs$segments$slope)]
      slope_last
    },
    .before = window_size - 1,
    .after = 0L,
    .complete = TRUE,
    n_cores = -1
  )
  dpseg_last <- unlist(dpseg_last)
  dpseg_last <- c(rep(NA, window_size - 1), dpseg_last)
  return(dpseg_last)
}
results_dpseg <- dpseg_roll(market_data, 400)
quantile(results_dpseg, seq(0, 1, .05), na.rm = TRUE)

results <- cbind(market_data, slope = resultsdpseg)
results <- cut()
results$sign <- ifelse(results$slope < -0.000005, 0L, 1)
# results$sign <- ifelse(results$slope < -0.0002, 0L, 1)
# results$sign <- ifelse(is.na(results$sign), 0L, results$sign)
results$sign <- shift(results$sign, 1L, type = 'lag')
results$returns_strategy <- results$returns * results$sign
PerformanceAnalytics::charts.PerformanceSummary(results[, c('returns', "returns_strategy")], plot.engine = 'ggplot2')



# FRACDIFF ----------------------------------------------------------------

library(fracdiff)

test <- slider::slide(
  .x = market_data[[1]]$SPY.Close,
  .f = ~ {
    d <- fracdiff::fdSperio(.)
  },
  .before = 600L - 1L,
  .after = 0L,
  .complete = TRUE
)
d <- lapply(test, purrr::pluck, 'd')
d <- unlist(d)

# results
close <- market_data[[1]]$SPY.Close[600:length(close <- market_data[[1]]$SPY.Close)]
results <- cbind.data.frame(d = d, close = close)
results$return <- (close - shift(close))
results <- na.omit(results)
critical_value <- 1.3
for (i in 1:nrow(results)) {
  if (i == 1) {
    signs <- NA
  } else if (results[i-1, 'd'] < 0.8) {
    # if (results$return[i-1] > 0) {
    #   signs <- c(signs, 1)
    # } else {
    #   signs <- c(signs, 0)
    # }
    signs <- c(signs, 0)
  } else {
    signs <- c(signs, 1)
  }
}
results$signs <- signs
results$returns_strategy <- results$return * results$sign
results <- na.omit(results)
time <- zoo::index(close)
perf <- xts::xts(results[, c('return', 'returns_strategy')], order.by = time[3:length(time)])

PerformanceAnalytics::charts.PerformanceSummary(perf)

