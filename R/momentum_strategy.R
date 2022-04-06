library(portfolioBacktest)


# get data
data(SP500_symbols)
SP500 <- stockDataDownload(stock_symbols = SP500_symbols,
                           from = "2008-12-01", to = "2018-12-01")

# resample 10 times from SP500, each with 50 stocks and 2-year consecutive data
my_dataset_list <- financialDataResample(SP500,
                                         N_sample = 50, T_sample = 252*5,
                                         num_datasets = 5)


# inspect data
names(my_dataset_list[[1]])
my_dataset_list[[1]]$adjusted

# add macd
for (i in 1:length(dataset10)) {
  my_dataset_list[[i]]$MACD <- apply(dataset10[[i]]$adjusted, 2,
                                     function(x) { TTR::MACD(x)[ , "macd"] })
}

# define quintile portfolio
quintile_portfolio_fun <- function(dataset, w_current) {
  X <- diff(log(dataset$adjusted))[-1]  # compute log returns
  N <- ncol(X)
  # design quintile portfolio
  ranking <- sort(colMeans(X), decreasing = TRUE, index.return = TRUE)$ix
  w <- rep(0, N)
  w[ranking[1:round(N/5)]] <- 1/round(N/5)
  return(w)
}
X <- diff(log(my_dataset_list$`dataset 1`$adjusted))[-1]
N <- ncol(X)
ranking <- sort(colMeans(X), decreasing = TRUE, index.return = TRUE)$ix
w <- rep(0, N)
w[ranking[1:round(N/5)]] <- 1/round(N/5)

# backtesting
portfolios <- list("Quintile"  = quintile_portfolio_fun)
bt <- portfolioBacktest(portfolios, my_dataset_list, benchmark = c("1/N", "index"), lookback = 252, rebalance_every = 14)

# results
res_sum <- backtestSummary(bt)
names(res_sum)
res_sum$performance_summary



# QA
data("dataset10")
quintile_portfolio_fun <- function(dataset, w_current) {
  X <- diff(log(dataset$adjusted))[-1]  # compute log returns
  N <- ncol(X)
  # design quintile portfolio
  ranking <- sort(colMeans(X), decreasing = TRUE, index.return = TRUE)$ix
  w <- rep(0, N)
  w[ranking[1:round(N/5)]] <- 1/round(N/5)
  return(w)
}
portfolios <- list("Quintile"  = quintile_portfolio_fun)
bt <- portfolioBacktest(portfolios,
                        my_dataset_list,
                        benchmark = c("1/N", "index"),
                        lookback = 252 / 2,
                        rebalance_every = 22)
