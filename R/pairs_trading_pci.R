library(data.table)
library(xts)
  library(partialCI)
library(ggplot2)
library(BatchGetSymbols)
library(future.apply)


# get daily market data for sp500 stocks
sp500_stocks <- GetSP500Stocks()
plan(multicore(workers = 8L))
sp500 <- BatchGetSymbols(sp500_stocks$Tickers,
                         first.date = as.Date("2015-01-01"),
                         last.date = as.Date("2018-01-01"),
                         do.parallel = TRUE)
prices <- as.data.table(sp500$df.tickers)
head(prices)

# clean daily market data
setorderv(prices, c("ticker", "ref.date")) # order
prices[, returns := price.adjusted / data.table::shift(price.adjusted) - 1, by = ticker] # calculate returns
prices <- prices[, .(ticker, ref.date, price.adjusted)] # choose columns wee need
prices <- prices[!duplicated(prices[, .(ticker, ref.date)])] # remove duplicates
prices <- data.table::dcast(prices, ref.date ~ ticker, value.var = "price.adjusted") # long to wide reshape
prices <- as.xts.data.table(prices) # convert to xts

# split in train and test set
train <- prices["2015-01-01/2017-01-01"]
test <- prices["2017-01-01/2018-01-01"]

# keep only stocks with no missing values
X <- train[, colSums(!is.na(train)) == max(colSums(!is.na(train)))]
X_test <- test[, colSums(!is.na(test)) == max(colSums(!is.na(test)))]

# choose best pairs using hedge.pci fucntoin and maxfact = 1 (only one facotr possible)
# THIS TAKES FEW MINUTES
pci_tests_i <- list()
for (i in 1:ncol(X)) {

  # quasi multivariate pairs
  hedge <- hedge.pci(X[, i], X[, -i], maxfact = 1)

  # pci fit
  test_pci <- test.pci(X[, i], hedge$pci$basis)

  # summary table
  results <- data.table(series1 = hedge$pci$target_name, series2 = hedge$index_names)
  metrics <- c(hedge$pci$beta, hedge$pci$rho, hedge$pci$sigma_M, hedge$pci$sigma_R,
               hedge$pci$M0, hedge$pci$R0, hedge$pci$beta.se, hedge$pci$rho.se,
               hedge$pci$sigma_M.se, hedge$pci$sigma_R.se, hedge$pci$M0.se,
               hedge$pci$R0.se, hedge$pci$negloglik, hedge$pci$pvmr)
  names(metrics)[c(1, 7:12)] <- c("beta", "beta_se", "rho_se", "sigma_M_se", "sigma_R_se",
                                  "M0_se", "R0_se")
  metrics <- as.data.table(as.list(metrics))
  results <- cbind(results, metrics)
  pci_tests_i[[i]] <- cbind(results, as.data.table(as.list(test_pci$p.value)))
}
pci_tests <- rbindlist(pci_tests_i, fill = TRUE)

# Apply restrictions to the universe
# 1) pairs with a half-life of mean-reversion of one day or less - thereby avoiding to select
#    pairs where trading gains are largely attributable to bid-ask bounce
# 2) pvmr > 0.5 ensures more reliable parameter estimates
# 3) my condition: MR p value should be lower than 0.05 because this test confirms mean reverting component
# 4) restriction to same sector I WANT APPLY THIS FOR  NOW
# 5) 25% lowest  by neLog
pci_tests_eligible <- pci_tests[pvmr > 0.5 & rho > 0.5 & MR < 0.05 &
                                  negloglik <= quantile(pci_tests$negloglik, probs = 0.25)] # 1), 2), 3) and 4)

# example of partially cointegrated pairs
plot(X[, unlist(pci_pairs[1, 1:2])]) # prices series of first pair
plot(log(X[, unlist(pci_pairs[1, 1:2])]))  # log prices series of first pair
plot(log(X_test[, unlist(pci_pairs[1, 1:2])])) # prices series of first pair on test time


# EXAMPLE FOR ONE PAIR ----------------------------------------------------

# get spreads
series1 <- X[, unlist(pci_pairs[1, 1])]
series2 <- X[, unlist(pci_pairs[1, 2])]
fit_pci <- fit.pci(series1, series2)
gamma <- fit_pci$beta # people call it gamma often
hidden_states <- statehistory.pci(fit_pci)
spread <- xts(hidden_states[, 4], as.Date(rownames(hidden_states)))

# calculate Z score from the spread
spread_var <- as.numeric(var(spread))
Z_score_M <- spread/sqrt(spread_var)
threshold <- 1 # in the paper authors uses theta = 1 (probbably -1 also) as the entry threshold
threshold_long <- threshold_short <- Z_score_M
threshold_short[] <- threshold
threshold_long[] <- -threshold

# plot spread and thresholds
ggplot(as.data.table(Z_score_M), aes(x = index, y = V1)) + geom_line() +
  geom_hline(yintercept = 0, color = "red") +
  geom_hline(yintercept = threshold_long, color = "blue") +
  geom_hline(yintercept = threshold_short, color = "blue")

# trading signal
generate_signal <- function(Z_score_M, threshold_long, threshold_short) {
  signal <- Z_score_M
  colnames(signal) <- "signal"
  signal[] <- NA

  # initial position
  signal[1] <- 0
  if (Z_score_M[1] <= threshold_long[1]) {
    signal[1] <- 1
  } else if (Z_score_M[1] >= threshold_short[1])
    signal[1] <- -1

  # loop
  for (t in 2:nrow(Z_score_M)) {
    if (signal[t-1] == 0) {  # if we were in no position
      if (Z_score_M[t] <= threshold_long[t]) {
        signal[t] <- 1
      } else if(Z_score_M[t] >= threshold_short[t]) {
        signal[t] <- -1
      } else signal[t] <- 0
    } else if (signal[t-1] == 1) {  # if we were in a long position
      if (Z_score_M[t] >= 0.5) signal[t] <- 0
      else signal[t] <- signal[t-1]
    } else {  # if we were in a short position
      if (Z_score_M[t] <= -0.5) signal[t] <- 0
      else signal[t] <- signal[t-1]
    }
  }
  return(signal)
}

# get and plot signals
signal <- generate_signal(Z_score_M, threshold_long, threshold_short)
ggplot(as.data.table(cbind(Z_score_M, signal)), aes(x = index)) +
  geom_line(aes(y = Z_score_M)) +
  geom_line(aes(y = signal), color = "red")


######### I AM NOT SURE HO TO PROCEED HERE, HOW TO TRADE AND SHOW PNL BASED ON SIGNALS #######

# my try
# hedge ratio
hedge_ratio <- (gamma * series1)/series2
plot(hedge_ratio)
dollar_inv_1 <- 1 / series1
dollar_inv_2 <- gamma / series2
dollar_inv <- cbind(dollar_inv_1, dollar_inv_2)
plot(dollar_inv)

# spread portfolio
Y <- cbind(series1, series2)
price1 <- 1 / series1
price2 <- gamma / series2
w_spread <- cbind(price1, price2)
colnames(w_spread) <- c("w1", "w2")
spread_X <- rowSums(Y * w_spread)
spread_X <- xts(spread_X, index(Y))
colnames(spread_X) <- paste0(colnames(Y)[1], "-", colnames(Y)[2])
spread_var_X <- as.numeric(var(spread_X))
Z_score_X <- (spread_X)/sqrt(spread_var_X)
plot(cbind(Z_score_M, Z_score_X))


Y <- cbind(series1, series2)
bla <- matrix(c(1, -gamma)/(1+gamma), nrow(Y), 2, byrow = TRUE)
w_spread <- cbind(bla[, 2], bla[, 1])
w_spread <- xts(w_spread, index(X))
colnames(w_spread) <- c("w1", "w2")
spread_X <- rowSums(Y * w_spread)
spread_X <- xts(spread_X, index(Y))
colnames(spread_X) <- paste0(colnames(Y)[1], "-", colnames(Y)[2])
spread_var_X <- as.numeric(var(spread_X))
Z_score_X <- (spread_X)/sqrt(spread_var_X)
plot(cbind(Z_score_M, Z_score_X))

# combine the ref portfolio with trading signal
w_portf <- w_spread * matrix(stats::lag(signal), nrow(Y), 2)   # NOTE THE LAG!!

# now compute the PnL from the log-prices and the portfolio
X <- diff(Y)  #compute log-returns from log-prices
portf_return <- xts(rowSums(X * w_portf), index(Y))
portf_return[is.na(portf_return)] <- 0
PerformanceAnalytics::charts.PerformanceSummary(portf_return)

# plots
tmp <- cbind(Z_score_M, signal)
colnames(tmp) <- c("Z-score", "signal")
par(mfrow = c(2, 1))
{ plot(tmp, legend.loc = legend_loc,
       main = paste("Z-score and trading signal for", colnames(Y)[1], "vs", colnames(Y)[2]))
  lines(threshold_short, lty = 2)
  lines(threshold_long, lty = 2) }
{ plot(cumprod(1 + portf_return), main = "Cum P&L") }
