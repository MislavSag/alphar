library(data.table)
library(ggplot2)
library(quantmod)
library(future.apply)
library(anytime)
library(PerformanceAnalytics)
library(roll)
library(TTR)
library(simfinapi)
library(gridExtra)
library(GAS)
library(parallel)         # for the GASS package
library(BatchGetSymbols)  # import data
source('C:/Users/Mislav/Documents/GitHub/alphar/R/outliers.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/import_data.R')
source('C:/Users/Mislav/Documents/GitHub/alphar/R/features.R')



# parameters
contracts = c('SPY', 'QQQ')
distributions <- c('sstd')
scale_type <- c("Identity", "Inv", "InvSqrt")
insample_length <- c(600, 800, 1200, 1500, 2000)
quantile_widths <- 255 * 1:8
q <- c(0.001, seq(0.01, 0.05, 0.01))

# import data
market_data <- BatchGetSymbols(tickers = contracts, first.date = '1994-01-01')[[2]]
market_data$returns <- market_data$ret.adjusted.prices
market_data <- na.omit(market_data)

# specification of general autoregresive scoring model
estimate <- function(data, ticker = 'SPY', target = 'returns',
                     dist = 'sstd', scale_type = 'Identity',
                     insample_length = 1000) {

  # choose ticker
  data <- data[data$ticker == ticker, ]

  # gas apec
  GASSpec = UniGASSpec(
    Dist = dist,
    ScalingType = scale_type,
    GASPar = list(location = TRUE, scale = TRUE, skewness = TRUE, shape = TRUE))

  # Perform 1-step ahead rolling forecast with refit
  cluster <- makeCluster(25)
  Roll <- UniGASRoll(
    xts::xts(data[, target], order.by = data$ref.date),
    GASSpec,
    Nstart = insample_length,
    RefitEvery = 5,
    RefitWindow = c("moving"),
    cluster = cluster)
  stopCluster(cluster)
  rm("cluster")

  # forecasts_gas <- getForecast(Roll)
  alphas <- c(0.01, 0.05)
  var_gas <- quantile(Roll, probs = alphas)  # VaR
  colnames(var_gas) <- paste0('var_gas_', as.character(alphas * 100))
  es_gas <- GAS::ES(Roll, probs = alphas)
  colnames(es_gas) <- paste0('es_gas_', as.character(alphas * 100))

  # merge to market_data
  data <- cbind(data, rbind(matrix(NA, insample_length, length(alphas)), var_gas))
  data <- cbind(data, rbind(matrix(NA, insample_length, length(alphas)), es_gas))
  data
}

# estimate over all prameters
vectors <- expand.grid(contracts, distributions, scale_type, insample_length, stringsAsFactors = FALSE)
estimated <- purrr::pmap(
  .l = vectors,
  .f = ~ {estimate(market_data, ticker = ..1, dist = ..2, scale_type = ..3, insample_length = ..4)}
)
names(estimated) <- apply(vectors, 1, paste, collapse = '_')

# backtest
gas_backtest <- function(data, quantile_width = 255*4, quantile_p = 0.05) {

  # trading rule
  data <- na.omit(data)
  indicator <- as.vector(zoo::coredata(data$es_gas_1))

  # execute
  indicator_quantile <- roll::roll_quantile(indicator,
                                            width = quantile_width,
                                            p = quantile_p,
                                            min_obs = 10)
  # indicator_2 <- as.vector(zoo::coredata(backtest_data$forecasts_gas_skewness))
  side <- vector(mode = 'integer', length = length(indicator))
  for (i in 1:length(indicator)) {
    if (i == 1 || is.na(indicator_quantile[i-1])) {
      side[i] <- NA
    } else if (indicator[i-1] < indicator_quantile[i-1]) {
      side[i] <- 0
    } else {
      side[i] <- 1
    }
  }
  table(side, useNA = 'ifany')
  returns_strategy <- xts::xts(data$returns * side, order.by = zoo::index(data))
  perf <- na.omit(merge(data[, c('returns')], returns_strategy = returns_strategy))
  # p <- charts.PerformanceSummary(perf, plot.engine = 'ggplot2')
  return(perf)
}

# backtest
gas_estimated_xts <- lapply(estimated, function(x) xts::xts(x[, -c(7, 8)], order.by = x$ref.date))
argumenta_backtest <- expand.grid(names(gas_estimated_xts), quantile_widths, q, stringsAsFactors = FALSE)
backtested <- purrr::pmap(
  .l = argumenta_backtest,
  .f = ~ {gas_backtest(gas_estimated_xts[[..1]], quantile_width = as.numeric(..2), quantile_p = as.numeric(..3))}
)
names(backtested) <- gsub('\\.', '', apply(argumenta_backtest, 1, paste, collapse = '_'))

# save cumulative returns
cum_returns <- do.call(cbind, backtested)
cum_returns <- cum_returns[, c(1, seq(2, ncol(cum_returns), 2))]
colnames(cum_returns) <- c('returns', names(backtested))
cum_returns <- t(PerformanceAnalytics::Return.cumulative(cum_returns))
vars <- data.frame(data.table::tstrsplit(rownames(cum_returns), split = '_'))
colnames(vars) <- c('ticker', 'dist', 'type', 'window', 'quantile_win', 'p')
cum_returns <- cbind(cum_returns, vars)
cum_returns %>%
  filter(ticker == 'SPY') %>%
  arrange(desc(`Cumulative Return`))

# heatmap
ggplot(cum_returns[2:nrow(cum_returns), ], aes(quantile_win, p, fill= `Cumulative Return`)) +
  geom_tile()
ggplot(cum_returns[2:nrow(cum_returns), ], aes(window, quantile_win, fill= `Cumulative Return`)) +
  geom_tile()
ggplot(cum_returns[2:nrow(cum_returns), ], aes(dist, type, fill= `Cumulative Return`)) +
  geom_tile()

# save plots
for (i in 1:length(backtested)) {
  ggsave(paste0('./gas/', names(backtested)[i], '.png'), charts.PerformanceSummary(backtested[[i]], plot.engine = 'ggplot2'))
}
