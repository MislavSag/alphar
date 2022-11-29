library(tiledb)
library(data.table)
library(xts)
library(partialCI)
library(ggplot2)
library(AzureStor)
library(pins)
library(checkmate)
library(RcppQuantuccia)
library(findata)
library(rvest)



# SET UP ------------------------------------------------------------------
# check if we have all necessary env variables
assert_choice("AWS-ACCESS-KEY", names(Sys.getenv()))
assert_choice("AWS-SECRET-KEY", names(Sys.getenv()))
assert_choice("AWS-REGION", names(Sys.getenv()))

# set credentials
config <- tiledb_config()
config["vfs.s3.aws_access_key_id"] <- Sys.getenv("AWS-ACCESS-KEY")
config["vfs.s3.aws_secret_access_key"] <- Sys.getenv("AWS-SECRET-KEY")
config["vfs.s3.region"] <- Sys.getenv("AWS-REGION")
context_with_config <- tiledb_ctx(config)

# NY date
setCalendar("UnitedStates::NYSE")

# parameters



# IMPORT MARKET DATA ------------------------------------------------------
# sp 100 current universe
sp100 <- read_html("https://en.wikipedia.org/wiki/S%26P_100") |>
  html_elements(x = _, "table") |>
  (`[[`)(3) |>
  html_table(x = _, fill = TRUE) |>
  (`[[`)(1)

# sp500 universe
fmp = FMP$new()
symbols <- fmp$get_sp500_symbols()

# get daily market data for sp500 stocks
arr <- tiledb_array("D:/equity-usa-daily-fmp",
                    as.data.frame = TRUE,
                    query_layout = "UNORDERED",
                    selected_ranges = list(date = cbind(as.Date("2019-01-01"), Sys.Date() - 1),
                                           symbol = cbind(symbols, symbols))
)
system.time(prices <- arr[])
tiledb_array_close(arr)
prices_dt <- as.data.table(prices)



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
sample_symbols <- sample(symbols, 200)
sample_dt <- prices_dt[symbol %in% sample_symbols]
sample_dt <- dcast(sample_dt, date ~ symbol, value.var = "adjClose")

# remove columns with NA values
keep_cols <- names(which(colMeans(!is.na(sample_dt)) > 0.99))
sample_dt <- sample_dt[, .SD, .SDcols = keep_cols]
X <- as.xts.data.table(sample_dt) # convert to xts
X <- X[, colSums(!is.na(X)) == max(colSums(!is.na(X)))]
X <- log(X)
dim(X)



# FIND BEST PAIRS ---------------------------------------------------------
# split in train and test set
train <- X["2019-01-01/2021-01-01"]
test <- X["2021-01-01/2022-01-01"]

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
#where trading gains are largely attributable to bid-ask bounce
# 2) pvmr > 0.5 ensures    pairs  more reliable parameter estimates
# 3) p_rw < 0.05 & p_ar < 0.05. A time series is classified as partially cointegrated,
#    if and only if the random walk as well as the AR(1)-hypotheses are rejected
# 3) my condition: MR p value should be lower than 0.05 because this test confirms mean reverting component
# 4) restriction to same sector I DON'T WANT APPLY THIS FOR  NOW
# 5) 25% lowest  by neLog
# 6) possible to add additional fundamental matching
pci_tests_eligible <- pci_tests[pvmr > 0.5  & rho > 0.5 & p_rw < 0.05 & p_ar < 0.05 &
                                  negloglik <= quantile(negloglik, probs = 0.25)] # 1), 2), 3), 5)

# example of partially cointegrated pairs
pairs <- unlist(pci_tests_eligible[1, 1:2])
plot(log(train[, pairs]), main = paste0(pairs[1], " - ", pairs[2])) # log prices series of first pair
# EQIX - Equinix, Inc. je američka multinacionalna tvrtka sa sjedištem u Redwood Cityju, Kalifornija,
#        specijalizirana za internetske veze i podatkovne centre.
# AMT - American Tower Corporation je američka zaklada za ulaganje u nekretnine te vlasnik i operater bežične i
#       televizijske komunikacijske infrastrukture u nekoliko zemalja
pairs <- unlist(pci_tests_eligible[2, 1:2])
plot(log(train[, pairs]), main = paste0(pairs[1], " - ", pairs[2])) # log prices series of second pair
# DOV - američki je konglomerat proizvođača industrijskih proizvod
# APH - glavni proizvođač elektroničkih i optičkih konektora, kabelskih i interkonektnih sustava kao što su koaksijalni kabeli
pairs <- unlist(pci_tests_eligible[3, 1:2])
plot(log(train[, pairs]), main = paste0(pairs[1], " - ", pairs[2])) # log prices series of third pair
# O - investicijski fond za nekretnine
# CPT - investicijski fond za nekretnine



# TRADING STRATEGY FOR ONE PAIR ----------------------------------------------------
# preparedata for pci estimation
n_ <- 1 # GOOD: 1,3,4,6,7,10  FLAT: 2,8,9  BAD: 5
cat("symbols are", unlist(pci_tests_eligible[n_, 1]), " and ",
    unlist(pci_tests_eligible[n_, 2]))
y1_train <- train[, unlist(pci_tests_eligible[n_, 1])]
y2_train <- train[, unlist(pci_tests_eligible[n_, 2])]
y1_test <- test[, unlist(pci_tests_eligible[n_, 1])]
y2_test <- test[, unlist(pci_tests_eligible[n_, 2])]
y1 <- X["2019-01-01/2022-01-01", unlist(pci_tests_eligible[n_, 1])]
y2 <- X["2019-01-01/2022-01-01", unlist(pci_tests_eligible[n_, 2])]

# get spreads
fit_pci <- fit.pci(y1_train, y2_train)
plot(fit_pci)
hs_train <- statehistory.pci(fit_pci)
hs_test <- statehistory.pci(fit_pci, data = y1_test, basis = y2_test)
hs <- rbind(hs_train, hs_test)
spread_train <- xts(hs_train[, 4], as.Date(rownames(hs_train)))
spread <- xts(hs[, 4], as.Date(rownames(hs)))
ggplot(as.data.table(spread), aes(x = index, y = V1)) +
  geom_line() +
  geom_vline(xintercept = as.Date("2021-01-01"), color = "blue")

# understand calculation
cat("Price data ", head(hs_test$Y))
cat("Price X1 ", head(y1_test)) # AEE
cat("Price X0 ", head(y2_test)) # XEL
cat("Beta ", fit_pci$beta)
cat("Y hat calculation", head(y2_train %*% fit_pci$beta))
print("Hidden states:")
head(hs_train)

# calculate std of spread an plot it
spread_var <- sd(spread_train)
ggplot(as.data.table(spread), aes(x = index)) +
  geom_line(aes(y = V1)) +
  geom_hline(aes(yintercept = spread_var), color = "red") +
  geom_hline(aes(yintercept = -spread_var), color = "red") +
  geom_hline(aes(yintercept = spread_var*2), color = "green") +
  geom_hline(aes(yintercept = -spread_var*2), color = "green") +
  geom_vline(xintercept = as.Date("2017-01-01"), color = "blue")

# plot spread and thresholdswith z-score
Z_score_M <- spread/sd(spread_train)
plot(Z_score_M)
ggplot(as.data.table(Z_score_M), aes(x = index, y = V1)) + geom_line() +
  geom_hline(yintercept = 0, color = "red") +
  geom_hline(yintercept = 1, color = "blue") +
  geom_hline(yintercept = -1, color = "blue") +
  geom_vline(xintercept = as.Date("2017-01-01"), color = "blue")

# trading signal
threshold <- spread_var # in the paper authors uses theta = 1 (probbably -1 also) as the entry threshold
threshold_long <- threshold_short <- spread
threshold_short[] <- spread_var
threshold_long[] <- -spread_var
# Z_score_M <- spread
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
      if (Z_score_M[t] >= (0.5 * spread_var)) signal[t] <- 0
      else signal[t] <- signal[t-1]
    } else {  # if we were in a short position
      if (Z_score_M[t] <= -(0.5 * spread_var)) signal[t] <- 0
      else signal[t] <- signal[t-1]
    }
  }
  return(signal)
}

# get and plot signals
signal <- generate_signal(spread, threshold_long, threshold_short)
ggplot(as.data.table(cbind(spread, signal)), aes(x = index)) +
  geom_line(aes(y = spread)) +
  geom_line(aes(y = signal), color = "red")

# generate signals with z scored
threshold_long <- threshold_short <- Z_score_M
threshold_short[] <- 1
threshold_long[] <- -1
generate_signal_zscore <- function(Z_score, threshold_long, threshold_short) {
  signal <- Z_score
  colnames(signal) <- "signal"
  signal[] <- NA

  #initial position
  signal[1] <- 0
  if (Z_score[1] <= threshold_long[1]) {
    signal[1] <- 1
  } else if (Z_score[1] >= threshold_short[1])
    signal[1] <- -1

  # loop
  for (t in 2:nrow(Z_score)) {
    if (signal[t-1] == 0) {  #if we were in no position
      if (Z_score[t] <= threshold_long[t]) {
        signal[t] <- 1
      } else if(Z_score[t] >= threshold_short[t]) {
        signal[t] <- -1
      } else signal[t] <- 0
    } else if (signal[t-1] == 1) {  #if we were in a long position
      if (Z_score[t] >= 0) signal[t] <- 0
      else signal[t] <- signal[t-1]
    } else {  #if we were in a short position
      if (Z_score[t] <= 0) signal[t] <- 0
      else signal[t] <- signal[t-1]
    }
  }
  return(signal)
}

# get and plot signals
signal <- generate_signal(Z_score_M, threshold_long, threshold_short)
ggplot(as.data.table(cbind(Z_score_M, signal)), aes(x = index)) +
  geom_line(aes(y = Z_score_M)) +
  geom_line(aes(y = signal), color = "red") +
  geom_vline(xintercept = as.Date("2021-01-01"), color = "blue")


# let's compute the PnL directly from the signal and spread
spread_return <- diff(Z_score_M)
traded_return <- spread_return * lag(signal)   # NOTE THE LAG!!
traded_return[is.na(traded_return)] <- 0
colnames(traded_return) <- "traded spread"

# plot Pnl
data_plot <- as.data.table(traded_return)
setnames(data_plot, c("date", "spread"))
ggplot(data_plot, aes(date, y = 1 + cumsum(spread))) +
  geom_line() +
  geom_vline(xintercept = as.Date("2021-01-01"), color = "blue")



# SAVE FOR QC BACKTEST ----------------------------------------------------
#
qc_data <- cbind(date = rownames(hs_test), as.data.table(hs_test))
qc_data[, Z_score_M := M / sd(spread_train)]
cols <- colnames(qc_data)[2:ncol(qc_data)]
qc_data[, (cols) := lapply(.SD, shift), .SDcols = cols]
qc_data <- na.omit(qc_data)

SNP_KEY = "0M4WRlV0/1b6b3ZpFKJvevg4xbC/gaNBcdtVZW+zOZcRi0ZLfOm1v/j2FZ4v+o8lycJLu1wVE6HT+ASt0DdAPQ=="
SNP_ENDPOINT = "https://snpmarketdata.blob.core.windows.net/"
bl_endp_key <- storage_endpoint(SNP_ENDPOINT, key=SNP_KEY)
cont <- storage_container(bl_endp_key, "qc-backtest")
time_ <- format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
file_name <- paste0("pairs_pci_", time_, ".csv")
storage_write_csv(qc_data, cont, file_name, col_names = FALSE)
print(file_name)
print(fit_pci$beta)

# simulate quantities and prices
A = 10000
aee = 4.2
xel = 3.8
share = 0.2
A_ = 2000
q_xel = floor(A_ / xel)
q_aee = q_xel * fit_pci$beta

q_xel * xel
q_aee * aee

q_aee = A_ / (aee %*% fit_pci$beta)
q_xel * fit_pci$beta

(xel / aee) * fit_pci$beta


61.657137504 * 323
69.145082055 * 312

# TODO:
# replicirati spread i trgovanje na QC.
# Parametri:
# 1) Frekventnost podataka: mnut / sat/ dan
# 2) Klase imovine: equity, commodity, crypto
# 3) trening / testing period
# 4) fundamental matching (sector matching)
