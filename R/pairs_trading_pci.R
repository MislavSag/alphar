library(data.table)
library(xts)
library(partialCI)
library(ggplot2)
library(AzureStor)
library(pins)
library(checkmate)
library(RcppQuantuccia)



# SET UP ------------------------------------------------------------------
# checks
assert_choice("BLOB-ENDPOINT", names(Sys.getenv()))
assert_choice("BLOB-KEY", names(Sys.getenv()))
assert_choice("APIKEY-FMPCLOUD", names(Sys.getenv()))

# global vars
ENDPOINT = storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), key=Sys.getenv("BLOB-KEY"))
CONT = storage_container(ENDPOINT, "fmpcloud")
CONTINVESTINGCOM = storage_container(ENDPOINT, "investingcom")
CACHEDIR = "D:/findata"
setCalendar("UnitedStates::NYSE")



# IMPORT MARKET DATA ------------------------------------------------------
# get daily market data for sp500 stocks
board <- board_azure(
  container = storage_container(ENDPOINT, "fmpcloud-daily"),
  path = "",
  n_processes = 6L,
  versioned = FALSE,
  cache = CACHEDIR
)
files_ <- pin_list(board)
files_ <- files_[as.Date(files_) > as.Date("2010-01-01")]
files_ <- lapply(file.path(CACHEDIR, files_), list.files, recursive = TRUE, pattern = "\\.csv", full.names = TRUE)
files_ <- unlist(files_)
prices_dt <- lapply(files_, fread)
prices_dt <- prices_dt[vapply(prices_dt, function(x) nrow(x) > 0, FUN.VALUE = logical(1))]
prices_dt <- rbindlist(prices_dt)


# CLEAN MARKET DATA -------------------------------------------------------
# cleaning steps with explanations
prices_dt <- prices_dt[isBusinessDay(date)] # keep only trading days
prices_dt <- prices_dt[open > 0 & high > 0 & low > 0 & close > 0 & adjClose > 0] # remove rows with prices <= 0
prices_dt <- prices_dt[, .(symbol, date, adjClose )] # keep only columns we need
prices_dt <- unique(prices_dt, by = c("symbol", "date")) # remove duplicates
prices_dt <- na.omit(prices_dt) # remove missing values
setorder(prices_dt, "symbol", "date") # order for returns calculation
prices_dt[, returns := adjClose / data.table::shift(adjClose, 1, type = "lag") - 1, by = symbol]
prices_dt <- prices_dt[returns < 1] # TODO:: better outlier detection mechanism



# PREPARE DATA ------------------------------------------------------------
# take sample of all prices for test
symbols <- unique(prices_dt$symbol)
sample_symbols <- sample(symbols, 150)
prices <- prices_dt[symbol %in% sample_symbols]
prices <- data.table::dcast(prices, date ~ symbol, value.var = "adjClose") # long to wide reshape

# remove columns woth lots of NA values
keep_cols <- names(which(colMeans(!is.na(prices)) > 0.8))
prices <- prices[, .SD, .SDcols = keep_cols]
X <- as.xts.data.table(prices) # convert to xts
X <- X[, colSums(!is.na(X)) == max(colSums(!is.na(X)))]
X <- log(X)
dim(X)

# split in train and test set
train <- X["2015-01-01/2017-01-01"]
test <- X["2017-01-01/2018-01-01"]

# choose best pairs using hedge.pci function and maxfact = 1 (only one factor possible)
# THIS TAKES FEW MINUTES
pci_tests_i <- list()
for (i in 1:ncol(X)) {

  # DEBUG
  print(i)

  # quasi multivariate pairs
  hedge <- tryCatch(hedge.pci(train[, i], train[, -i], maxfact = 1, use.multicore = FALSE),
                    error = function(e) NULL)
  if (is.null(hedge)) {
    pci_tests_i[[i]] <- NULL
    next()
    }

  # pci fit
  test_pci <- test.pci(train[, i], hedge$pci$basis)

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
  pci_tests_i[[i]] <- cbind(results, p_rw = test_pci$p.value[1], p_ar = test_pci$p.value[2])
}
pci_tests <- rbindlist(pci_tests_i, fill = TRUE)

# Apply restrictions to the universe
# 1) pairs with a half-life of mean-reversion of one day or less - thereby avoiding to select
#    pairs where trading gains are largely attributable to bid-ask bounce
# 2) pvmr > 0.5 ensures more reliable parameter estimates
# 3) my condition: MR p value should be lower than 0.05 because this test confirms mean reverting component
# 4) restriction to same sector I WANT APPLY THIS FOR  NOW
# 5) 25% lowest  by neLog
pci_tests_eligible <- pci_tests[pvmr > 0.5  & rho > 0.5 &
                                  negloglik <= quantile(pci_tests$negloglik, probs = 0.25)] # 1), 2), 3), 5)
# pci_tests_eligible <- pci_tests[pvmr > 0.5 & rho > 0.5 & MR < 0.05 &
#                                   negloglik <= quantile(pci_tests$negloglik, probs = 0.25)] # 1), 2), 3) and 5)

# example of partially cointegrated pairs
plot(train[, unlist(pci_tests_eligible[1, 1:2])]) # prices series of first pair
plot(log(train[, unlist(pci_tests_eligible[1, 1:2])]))  # log prices series of first pair
plot(log(test[, unlist(pci_tests_eligible[1, 1:2])])) # prices series of first pair on test time


# EXAMPLE FOR ONE PAIR ----------------------------------------------------

# get spreads
series1 <- train[, unlist(pci_tests_eligible[1, 1])]
series2 <- train[, unlist(pci_tests_eligible[1, 2])]
fit_pci <- fit.pci(series1, series2)
gamma <- fit_pci$beta # people call it gamma often
hidden_states <- statehistory.pci(fit_pci)
spread <- xts(hidden_states[, 4], as.Date(rownames(hidden_states)))
plot(spread)




# spread portfolio
gamma <- fit_pci$beta # people call it gamma often
w_spread <- matrix(c(1, -gamma)/(1+gamma), nrow(series1), 2, byrow = TRUE)
w_spread <- xts(w_spread, index(train))
colnames(w_spread) <- c("w1", "w2")

# spread
spread <- rowSums(series1 * w_spread)
spread <- xts(spread, index(Y))
colnames(spread) <- paste0(colnames(Y)[1], "-", colnames(Y)[2])




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
