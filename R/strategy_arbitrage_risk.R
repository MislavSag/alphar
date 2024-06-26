library(data.table)
library(xts)
library(partialCI)
library(ggplot2)
library(BatchGetSymbols)
library(future.apply)
library(TTR)
library(runner)
library(reticulate)
reticulate::use_condaenv("C:\\ProgramData\\Anaconda3\\python.exe")
sktime <- reticulate::import("sktime")
sklearn <- reticulate::import("sklearn")
pd <- import("pandas")
main <- import_main()

# get daily market data for sp500 stocks
sp500_stocks <- GetSP500Stocks()
plan(multicore(workers = 8L))
sp500 <- BatchGetSymbols(c('SPY', sp500_stocks$Tickers),
                         first.date = as.Date("1998-01-01"),
                         last.date = Sys.Date(),
                         do.parallel = TRUE)
prices <- as.data.table(sp500$df.tickers)
head(prices)

# clean daily market data
setorderv(prices, c("ticker", "ref.date")) # order
prices <- prices[, .(ticker, ref.date, price.adjusted)] # choose columns wee need
prices <- prices[!duplicated(prices[, .(ticker, ref.date)])] # remove duplicates
prices <- data.table::dcast(prices, ref.date ~ ticker, value.var = "price.adjusted") # long to wide reshape
prices <- as.xts.data.table(prices) # convert to xts

# estimate std and r2M
refit <- 25
train_length <- 500
sd_last <- c()
r <- c()
sample_ <- NULL
spy_hedge <- NULL
spread_difflast <- NULL
for (i in 1:nrow(prices)) {
  print(i)
  # i = 250
  # esimaate spreads
  if (i >= train_length && i %% refit == 0) {
    sample_ <- prices[(i-train_length):i]
    sample_ <- sample_[, colSums(!is.na(sample_)) == max(colSums(!is.na(sample_)))]
    spy_index <- which(colnames(sample_) == 'SPY')
    spy_hedge <- hedge.pci(sample_[, spy_index], sample_[, -spy_index], maxfact = 10)
  }

  #
  if (i > train_length) {
    sample_ <- prices[(i-train_length):i]
    spy_index <- which(colnames(sample_) == 'SPY')
    fit_pci <- fit.pci(sample_[, spy_index], sample_[, spy_hedge$index_names])
    print(fit_pci$target_name)
    hidden_states <- statehistory.pci(fit_pci)
    spread <- xts(hidden_states[, 4], as.Date(rownames(hidden_states)))
    eps_M <- xts(hidden_states[, 6], as.Date(rownames(hidden_states)))
    rollsd <- roll::roll_sd(spread, 10)
    sd_last <- c(sd_last, tail(rollsd, 1))
    r <- c(r, fit_pci$pvmr)
    spread_difflast <- c(spread_difflast, zoo::coredata(tail(spread, 1)))
  } else {
    sd_last <- c(sd_last, NA)
    r <- c(r, NA)
    spread_difflast <- c(spread_difflast, NA)
  }
}

# plots
ind <- xts(spread_difflast, order.by = zoo::index(prices))
ind_m <- roll::roll_mean(ind, 10)
ind_me <- roll::roll_median(ind, 10)
ind_sd <- roll::roll_sd(ind, 10)
plot(cbind(ind, ind_m, ind_me))
plot(cbind(ind, ind_m, ind_me, ind_s)["2020-01-01/2021-01-01"])
plot(cbind(ind, ind_m, ind_me, ind_s)["2007-06-01/2010-01-01"])
plot(ind)

# spread prediction
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
  print(table(sides))
  returns_strategy <- returns * sides
  if (return_cumulative) {
    return(PerformanceAnalytics::Return.cumulative(returns_strategy))
  } else {
    return(returns_strategy)
  }
}

# backtest
returns <- diff(log(prices$SPY))
returns_strategy <- backtest(returns, SMA(ind_sd, 10), 0.5, FALSE)
PerformanceAnalytics::charts.PerformanceSummary(cbind(returns, returns_strategy))
PerformanceAnalytics::charts.PerformanceSummary(cbind(returns["2007-06-01/2010-01-01"], returns_strategy["2007-06-01/2010-01-01"]))
PerformanceAnalytics::charts.PerformanceSummary(cbind(returns["2015-01-01/2016-01-01"], returns_strategy["2015-01-01/2016-01-01"]))
PerformanceAnalytics::charts.PerformanceSummary(cbind(returns["2020-01-01/2021-01-01"], returns_strategy["2020-01-01/2021-01-01"]))
PerformanceAnalytics::charts.PerformanceSummary(cbind(returns["2021-01-01/2021-07-01"], returns_strategy["2021-01-01/2021-07-01"]))



# ML MODEL ----------------------------------------------------------------

# create X
X <- cbind(as.data.table(prices[, "SPY"]), as.data.table(spread_difflast))
X[, returns := SPY / shift(SPY) - 1]
X[, returns_week := SPY / shift(SPY, 5) - 1]
X[, returns_month := SPY / shift(SPY, 22) - 1]
X <- X[, .(spread_difflast, returns_month)]
X <- na.omit(X)
X[, sign := ifelse(X[, 2] > 0, "1", "0")]
X <- X[, .(spread_difflast, sign)]
table(X$sign)

library(kerasgenerator)
# generate 3d data dim (WRONG OUTPUT FOR Y)
timestep_len = 50
data_gen <- flow_series_from_dataframe(
  data = as.data.frame(X),
  x = "spread_difflast",
  y = "sign",
  length_out = 1,
  stride = 1,
  lookback = 1,
  timesteps = timestep_len,
  batch_size = nrow(X),
  mode = "training"
)
Xy <- data_gen()
x <- Xy[[1]]
dim(x)
x <- array(x, dim = c(dim(x)[1], 1, timestep_len))
x <- sktime$utils$data_container$from_3d_numpy_to_nested(x)
y <- as.matrix(X[(timestep_len+1):nrow(X), 2])

test = sktime$forecasting$model_selection$temporal_train_test_split(x, y)
classifier = sktime$classification$interval_based$TimeSeriesForest()
classifier$fit(test[[1]], test[[3]])
y_pred_prob = classifier$predict_proba(test[[2]])
y_pred = ifelse(y_pred_prob[, 1] > 0.60, "0", "1")
sklearn$metrics$accuracy_score(test[[4]], y_pred)

head(classifier$predict(test[[2]]))
head(y_pred)

# backtest
returns <- diff(log(prices$SPY))
returns_strategy <- returns[(nrow(returns) - length(y_pred) + 1):nrow(returns)] * as.integer(y_pred)
PerformanceAnalytics::charts.PerformanceSummary(cbind(returns[(nrow(returns) - length(y_pred) + 1):nrow(returns)],
                                                      returns_strategy))


# library(tsclassification)
#
# data = data.frame(matrix(rnorm(500), nrow = 100))
# data$target = sample(c(1, 0), 100, replace = TRUE)
# train_set = sample(c(TRUE, FALSE), 100, replace = TRUE)
#
#
# # Instantiate the model
# tsc = TSClassifier$new("timeseriesweka.classifiers.BOSS")
# # Call train and predict function on the different data splits
# tsc$train(data[train_set, ],target = "target")
# tsc$predict(data[!train_set, ])


# library(tsclassification)
# library(mlr3)
# tsk = mlr_tasks$get("iris")
# lrn = LearnerClassifTSClassification$new()
#
# # Train the classifier
# lrn$train(tsk)
#
# # Predict
# prds = lrn$predict(tsk)
