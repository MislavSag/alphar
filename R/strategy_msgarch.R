library(data.table)
library(Rcpp)
library(purrr)
library(ggplot2)
library(highfrequency)
library(quantmod)
library(future.apply)
library(PerformanceAnalytics)
library(MSGARCH)
library(BatchGetSymbols)
source('C:/Users/Mislav/Documents/GitHub/alphar/R/parallel_functions.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/outliers.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/import_data.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/features.R')



# PARAMETERS --------------------------------------------------------------

# before backcusum function
contract = 'SPY'
upsample = 60



# IMPORT DATA -------------------------------------------------------------

# HFD
market_data <- import_mysql(
  symbols = contract,
  trading_hours = TRUE,
  upsample = upsample,
  use_cache = TRUE,
  save_path = 'D:/market_data/usa/ohlcv',
  drv = RMySQL::MySQL(),
  dbname = 'odvjet12_market_data_usa',
  username = 'odvjet12_mislav',
  password = 'Theanswer0207',
  host = '91.234.46.219'
)[[1]]


# LFD
market_data <- BatchGetSymbols(tickers = 'SPY', first.date = '1990-01-01')
market_data <- market_data$df.tickers


# PREPROCESSING -----------------------------------------------------------

# Remove outliers
market_data <- remove_outlier_median(market_data, median_scaler = 25)

# Add features
# market_data <- add_features(market_data)
market_data$returns <- Return.calculate(Cl(market_data))
market_data$returns_lag <- data.table::shift(market_data$returns)

# Remove NA values
market_data <- na.omit(market_data)



# MARKOV SWITCHING GARCH --------------------------------------------------


msgarch <- CreateSpec(variance.spec = list(model = "eGARCH"),
                      distribution.spec = list(distribution = "std"),
                      switch.spec = list(K = 2),
                      constraint.spec = list(regime.const = "nu"))
models <- list(msgarch)

n.its <- 1500  # number of insample observations
n.ots <- length(market_data$ret.adjusted.prices) - n.its  # number of outofsample observations
alpha <- 0.05  # alpha in VaR
k.update <- 25  # how often to reestimate the model


VaR <- matrix(NA, nrow = n.ots, ncol = length(models))
VaR_5 <- matrix(NA, nrow = n.ots, ncol = length(models))
y.ots <- matrix(NA, nrow = n.ots, ncol = 1)
y_predictions <- matrix(NA, nrow = n.ots, ncol = 1)
model.fit <- vector(mode = "list", length = length(models))
for (i in 1:n.ots) {
  cat("Backtest - Iteration: ", i, "\n")
  y.its <- market_data$ret.adjusted.prices[i:(n.its + i - 1)] * 100
  y.ots[i] <- market_data$ret.adjusted.prices[n.its + i] * 100
  for (j in 1:length(models)) {
    if (k.update == 1 || i %% k.update == 1) {
      cat("Model", j, "is reestimated\n")
      model.fit[[j]] <- FitML(spec = models[[j]], data = y.its,
                              ctr = list(do.se = FALSE))
    }
    # y_predictions[i, j] <- predict(model.fit[[j]], nahead = 5, do.return.draw = FALSE)$vol[5]
    VaR[i, j] <- Risk(model.fit[[j]]$spec, par = model.fit[[j]]$par,
                      data = y.its, n.ahead = 5, alpha = alpha, do.es = FALSE,
                      do.its = FALSE)$VaR
    # VaR_5[i, j] <- Risk(model.fit[[j]]$spec, par = model.fit[[j]]$par,
    #                   data = y.its, n.ahead = 10, alpha = alpha, do.es = FALSE,
    #                   do.its = FALSE)$VaR
  }
}

# merge market data and outputs
market_data$var_forecast <- c(rep(NA, n.its), VaR)
var_results <- na.omit(market_data)

# Statistics of predictions
qplot(x = zoo::index(var_results), y = var_results$var_forecast, geom = 'line')
quantile(var_results$var_forecast, seq(0, 1, 0.05))
table(var_results$var_forecast > -2)


# BACKTEST -----------------------------------------------------------------

# merge predistions and market data
var_results$sides <- ifelse(var_results$var_forecast < -2.5, 0, 1)
var_results$returns_strategy <- var_results$returns * var_results$sides
results_xts <- var_results[, c('returns', 'returns_strategy')]
results_xts <- na.omit(results_xts)
charts.PerformanceSummary(results_xts, plot.engine = 'ggplot2')



library(OpVaR)
data(lossdat)

head(lossdat[[2]], 10)
table(lossdat[[1]])
all(lossdat[[1]] > 0)

opriskmodel=list()
for(i in 1:length(lossdat)){
  opriskmodel[[i]]=list()
}
opriskmodel[[1]]$freqdist=fitFreqdist(lossdat[[1]],"pois")
