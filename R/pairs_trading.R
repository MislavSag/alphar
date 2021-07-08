library(data.table)
library(httr)
library(TTR)
library(partialCI)
library(fmpcloudr)
library(ggplot2)
library(xts)



# set fmpcloudr api token
API_KEY = "15cd5d0adf4bc6805a724b4417bbaafc"
fmpc_set_token(API_KEY)

# sp500 stocks
url <- "https://financialmodelingprep.com/api/v3/sp500_constituent?apikey=15cd5d0adf4bc6805a724b4417bbaafc"
sp500_stocks <- rbindlist(httr::content(GET(url)))
sp500_symbols <- unique(sp500_stocks$symbol)

# get industrial classification
get_industry <- function(symbol = "AAPL") {
  url <- paste0("https://financialmodelingprep.com/api/v4/standard_industrial_classification")
  results <- httr::content(GET(url, query = list(symbol = symbol, apikey=API_KEY)))
  return(rbindlist(results))
}
symbol_industry <- rbindlist(lapply(sp500_symbols, get_industry))

# get daily market data for all stocks
prices <- fread("D:/fundamental_data/daily_data/daily_prices.csv")
prices[, returns := adjClose / data.table::shift(adjClose) - 1, by = symbol]
prices <- prices[, .(symbol, date, adjClose)]
prices <- prices[!duplicated(prices[, .(symbol, date)])]
prices_sp <- prices[symbol %in% sp500_symbols]
prices_sp <- data.table::dcast(prices_sp, date ~ symbol, value.var = "adjClose")
prices_sp <- as.xts.data.table(prices_sp)
prices_sp <- prices_sp[, which(colMeans(!is.na(prices_sp)) > 0.5)]
spy <- as.data.table(fmpc_price_history("SPY", startDate = as.Date("1998-01-01"), ))
spy <- as.xts.data.table(spy[, .(date, adjClose)])
colnames(spy) <- "SPY"
prices_sp <- merge(spy, prices_sp)

# split in train and test set
train <- prices_sp["2015-01-01/2020-01-01"]
test <- prices_sp["2020-01-01/2021-01-01"]

# remove missing values
X <- train[, colSums(!is.na(train)) == 1258]
X_test <- test[, colSums(!is.na(test)) == 253]



# FORMATION PERIOD --------------------------------------------------------

# partial cointegration tests
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
pci_tests <- rbindlist(pci_tests_i,fill = TRUE)
pci_tests <- merge(pci_tests, symbol_industry[, .(symbol, sicCode, industryTitle)],
                   by.x = "series2", by.y = "symbol")
data.table::setnames(pci_tests, c("sicCode", "industryTitle"), c("sicCode2", "industryTitle2"))

pci_tests <- merge(pci_tests, symbol_industry[, .(symbol, sicCode, industryTitle)],
                   by.x = "series1", by.y = "symbol", all.x = TRUE, all.y = FALSE)
data.table::setnames(pci_tests, c("sicCode", "industryTitle"), c("sicCode1", "industryTitle1"))

# Apply restrictions to the universe
# 1) pairs with a half-life of mean-reversion of one day or less - thereby avoiding to select
#    pairs where trading gains are largely attributable to bid-ask bounce
# 2) pvmr > 0.5 ensures more reliable parameter estimates
# 3) my condition: MR p value should be lower than 0.05 because this test confirms mean reverting component
# 4) restriction to same sector
# 5) 20 lowest  by neLog
pci_tests_eligible <- pci_tests[pvmr > 0.5 & rho > 0.5 & MR < 0.05 &
                                  negloglik <= quantile(pci_tests$negloglik, probs = 0.25)] # 1), 2), 3) and 4)
pci_tests_eligible[sicCode1 == sicCode2]
pci_tests_eligible[substr(sicCode1, 1, 3) == substr(sicCode2, 1, 3)]
pci_tests_eligible[substr(sicCode1, 1, 2) == substr(sicCode2, 1, 2)]
pci_tests_eligible[substr(sicCode1, 1, 1) == substr(sicCode2, 1, 1)]
pci_pairs <- pci_tests_eligible[substr(sicCode1, 1, 2) == substr(sicCode2, 1, 2)] # 4)

# example plots
plot(X[, unlist(pci_pairs[10, 1:2])])
plot(log(X[, unlist(pci_pairs[10, 1:2])]))
plot(log(X_test[, unlist(pci_pairs[10, 1:2])]))

# get spreads
series1 <- X[, unlist(pci_pairs[1, 1])]
series2 <- X[, unlist(pci_pairs[1, 2])]
fit_pci <- fit.pci(series1, series2)
gamma <- fit_pci$beta
hidden_states <- statehistory.pci(fit_pci)
spread <- xts(hidden_states[, 4], as.Date(rownames(hidden_states)))
plot(spread)
Y <- cbind(series1, series2)
pct_training = 0.9
threshold = 1

# set up
if(anyNA(Y)) {
  print("There are NA values int the Y. They will be approximated.")
  Y <- na.approx(Y)
}
T_obs <- nrow(Y)
T_trn <- round(pct_training*T_obs)

# fit PCI and extract beta (gamma)
fit_pci <- fit.pci(Y[, 1], Y[, 2])
gamma <- fit_pci$beta

# spread portfolio
w_spread <- matrix(c(1, -gamma)/(1+gamma), T_obs, 2, byrow = TRUE)
w_spread <- xts(w_spread, index(Y))
colnames(w_spread) <- c("w1", "w2")

# spread
hidden_states <- statehistory.pci(fit_pci)
spread <- xts(hidden_states[, 4], as.Date(rownames(hidden_states)))
spread_var <- as.numeric(var(spread[1:T_trn]))
Z_score_M <- spread/sqrt(spread_var)
threshold_long <- threshold_short <- Z_score_M
threshold_short[] <- threshold
threshold_long[] <- -threshold

# stocks spread
w_spread <- matrix(c(1, -gamma)/(1+gamma), T_obs, 2, byrow = TRUE)
w_spread <- xts(w_spread, index(Y))
colnames(w_spread) <- c("w1", "w2")
spread <- rowSums(Y * w_spread)
spread <- xts(spread, index(Y))
colnames(spread) <- paste0(colnames(Y)[1], "-", colnames(Y)[2])
spread_mean <- mean(spread[1:T_trn])
spread_var <- as.numeric(var(spread[1:T_trn]))
Z_score <- (spread-spread_mean)/sqrt(spread_var)

hedge_ratio <- (gamma * Y[, 1])/Y[, 2]
plot(hedge_ratio)
plot(cbind(spread, hedge_ratio))

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

# now just invoke the function
signal <- generate_signal(Z_score_M, threshold_long, threshold_short)
ggplot(as.data.table(cbind(Z_score_M, signal)), aes(x = index)) +
  geom_line(aes(y = Z_score_M)) +
  geom_line(aes(y = signal), color = "red")

w_portf <- w_spread * matrix(lag(signal), T_obs, 2)   # NOTE THE LAG!!

# now compute the PnL from the log-prices and the portfolio
X <- diff(Y)  #compute log-returns from log-prices
portf_return <- xts(rowSums(X * w_portf), index(X))
portf_return[is.na(portf_return)] <- 0

# plots
tmp <- cbind(Z_score_M, signal)
colnames(tmp) <- c("Z-score", "signal")
par(mfrow = c(2, 1))
{ plot(tmp, legend.loc = legend_loc,
       main = paste("Z-score and trading signal for", colnames(Y)[1], "vs", colnames(Y)[2]))
  lines(threshold_short, lty = 2)
  lines(threshold_long, lty = 2)
  print(addEventLines(xts("", index(Y[T_trn])), lwd = 2, col = "blue")) }
{ plot(cumprod(1 + portf_return), main = "Cum P&L")
  print(addEventLines(xts("", index(Y[T_trn])), lwd = 2, col = "blue")) }


# let's compute the PnL directly from the signal and spread
spread_return <- diff(spread)
traded_return <- spread_return * lag(signal)   # NOTE THE LAG!!
traded_return[is.na(traded_return)] <- 0
colnames(traded_return) <- "traded spread"
PerformanceAnalytics::Return.cumulative(traded_return)

{ plot(traded_return, main = "Return of traded spread")}
{ plot(1 + cumsum(traded_return), main = "Cum P&L of traded spread (no reinvestment)")}
{ plot(cumprod(1 + traded_return), main = "Cum P&L of traded spread (w/ reinvestment)")}

