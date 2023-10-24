library(tiledb)
library(data.table)
library(xts)
library(partialCI)
library(ggplot2)
library(checkmate)
library(findata)
library(rvest)
library(patchwork)
library(AzureStor)
library(qlcal)



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

# set NYSE
qlcal::setCalendar("UnitedStates/NYSE")



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
                    selected_ranges = list(date = cbind(as.Date("2018-01-01"), Sys.Date() - 1),
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
prices_dt <- prices_dt[returns < 1] # TODO: better outlier detection mechanism



# PREPARE DATA ------------------------------------------------------------
# take sample of all prices for test
symbols <- unique(prices_dt$symbol)
# sample_symbols <- sample(symbols, 200)
# sample_dt <- prices_dt[symbol %in% sample_symbols]
sample_dt <- dcast(prices_dt, date ~ symbol, value.var = "adjClose")

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

# remove same pairs
pci_tests_eligible <- pci_tests_eligible[!(series2 %in% series1)]

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



# PAIRS STRATEGY FUNCTION -------------------------------------------------
# main function to analyse pairs
pairs_trading_pci <- function(y1_train, y2_train, y1_test, y2_test,
                              std_entry = 1, plot_pnl = TRUE) {

  # symbols
  symb1 <- colnames(y1_train)
  symb2 <- colnames(y2_train)

  # get spreads
  fit_pci <- fit.pci(y1_train, y2_train)
  hs_train <- statehistory.pci(fit_pci)
  hs_test <- statehistory.pci(fit_pci, data = y1_test, basis = y2_test)
  hs <- rbind(hs_train, hs_test)
  spread_train <- xts(hs_train[, 4], as.Date(rownames(hs_train)))
  spread <- xts(hs[, 4], as.Date(rownames(hs)))

  # z-score
  spread_var <- sd(spread_train)
  Z_score_M <- spread/spread_var

  # generate signals with z scored
  threshold_long <- threshold_short <- Z_score_M
  threshold_short[] <- std_entry
  threshold_long[] <- -std_entry
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
  signal <- generate_signal_zscore(Z_score_M, threshold_long, threshold_short)

  # let's compute the PnL directly from the signal and spread
  spread_return <- diff(Z_score_M)
  traded_return <- spread_return * lag(signal)   # NOTE THE LAG!!
  traded_return[is.na(traded_return)] <- 0
  colnames(traded_return) <- "traded spread"

  # plot PnL
  if (plot_pnl) {
    # plot Pnl
    data_plot <- as.data.table(traded_return)
    setnames(data_plot, c("date", "spread"))
    g1 <- ggplot(as.data.table(Z_score_M), aes(x = index, y = V1)) + geom_line() +
      geom_hline(yintercept = 0, color = "red") +
      geom_hline(yintercept = 1, color = "blue") +
      geom_hline(yintercept = -1, color = "blue") +
      geom_vline(xintercept = as.Date("2017-01-01"), color = "blue")
    g2 <- ggplot(data_plot, aes(date, y = 1 + cumsum(spread))) +
      geom_line() +
      geom_vline(xintercept = as.Date("2021-01-01"), color = "blue")
    print(g1 / g2)
  }

  # data for QC
  qc_data <- cbind(date = rownames(hs_test), as.data.table(hs_test))
  qc_data[, Z_score_M := M / spread_var]
  qc_data[, symbol_1 := symb1]
  qc_data[, symbol_2 := symb2]
  qc_data[, beta := fit_pci$beta]
  cols <- colnames(qc_data)[2:ncol(qc_data)]
  qc_data[, (cols) := lapply(.SD, shift), .SDcols = cols]
  qc_data <- na.omit(qc_data)

  return(qc_data)
}

# example
n_ = 5
pairs_trading_pci(
  y1_train = train[, unlist(pci_tests_eligible[n_, 1])],
  y2_train = train[, unlist(pci_tests_eligible[n_, 2])],
  y1_test = test[, unlist(pci_tests_eligible[n_, 1])],
  y2_test = test[, unlist(pci_tests_eligible[n_, 2])],
  std_entry = 1,
  plot_pnl = TRUE
)

# all pairs
pairs_results_l <- lapply(1:nrow(pci_tests_eligible), function(n_) {
  pairs_trading_pci(
    y1_train = train[, unlist(pci_tests_eligible[n_, 1])],
    y2_train = train[, unlist(pci_tests_eligible[n_, 2])],
    y1_test = test[, unlist(pci_tests_eligible[n_, 1])],
    y2_test = test[, unlist(pci_tests_eligible[n_, 2])],
    std_entry = 1,
    plot_pnl = FALSE
  )
})
pairs_results <- rbindlist(pairs_results_l)



# SAVE FOR QC BACKTEST ----------------------------------------------------
# save data to QC
bl_endp_key <- storage_endpoint(Sys.getenv("BLOB-ENDPOINT-SNP"),
                                Sys.getenv("BLOB-KEY-SNP"))
cont <- storage_container(bl_endp_key, "qc-backtest")
time_ <- format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
lapply(unique(pairs_results$symbol_1), function(s) {
  sample_ <- pairs_results[symbol_1 == s]
  file_name <- paste0("pairs_pci_", s, "_", sample_[1, symbol_2], "_", time_, ".csv")
  storage_write_csv(sample_, cont, file_name, col_names = FALSE)
})

# # simulate quantities and prices
# A = 10000
# aee = 4.2
# xel = 3.8
# share = 0.2
# A_ = 2000
# q_xel = floor(A_ / xel)
# q_aee = q_xel * fit_pci$beta
#
# q_xel * xel
# q_aee * aee
#
# q_aee = A_ / (aee %*% fit_pci$beta)
# q_xel * fit_pci$beta
#
# (xel / aee) * fit_pci$beta
#
#
# 61.657137504 * 323
# 69.145082055 * 312




# PAPER TRADING ----------------------------------------------------------
# sp500 universe
fmp = FMP$new()
symbols <- fmp$get_sp500_symbols()

# get daily market data for sp500 stocks
start_date_test <- as.Date(format.Date(Sys.Date(), format = "%Y-%m-01")) - 30
start_date <- start_date_test  - (365 * 3)
arr <- tiledb_array("D:/equity-usa-daily-fmp",
                    as.data.frame = TRUE,
                    query_layout = "UNORDERED",
                    selected_ranges = list(symbol = cbind(symbols, symbols))
                    # selected_ranges = list(date = cbind(start_date, Sys.Date()),
                                           # symbol = cbind(symbols, symbols))
)
system.time(prices_new <- arr[])
tiledb_array_close(arr)
prices_dt_new <- as.data.table(prices_new)
setorder(prices_dt_new, symbol, date)

# cleaning steps with explanations
prices_dt_new <- prices_dt_new[isBusinessDay(date)] # keep only trading days
prices_dt_new <- prices_dt_new[open > 0 & high > 0 & low > 0 & close > 0 & adjClose > 0] # remove rows with prices <= 0
prices_dt_new <- prices_dt_new[, .(symbol, date, adjClose )] # keep only columns we need
prices_dt_new <- unique(prices_dt_new, by = c("symbol", "date")) # remove duplicates
prices_dt_new <- na.omit(prices_dt_new) # remove missing values
setorder(prices_dt_new, "symbol", "date") # order for returns calculation
prices_dt_new[, returns := adjClose / data.table::shift(adjClose, 1, type = "lag") - 1, by = symbol]
prices_dt_new <- prices_dt_new[returns < 1] # TODO:: better outlier detection mechanism
prices_dt_new <- dcast(prices_dt_new, date ~ symbol, value.var = "adjClose")

# remove columns with NA values
keep_cols <- names(which(colMeans(!is.na(prices_dt_new)) > 0.99))
prices_dt_new <- prices_dt_new[, .SD, .SDcols = keep_cols]
X <- as.xts.data.table(prices_dt_new)
X <- X[, colSums(!is.na(X)) == max(colSums(!is.na(X)))]
X <- log(X)
dim(X)

# split in train and test set
train_dates <- paste(start_date, start_date_test, sep = "/")
train <- X[train_dates]
test_dates <- paste(start_date_test, Sys.Date(), sep = "/")
test <- X[test_dates]

# choose best pairs using hedge.pci function and maxfact = 1 (only one factor possible)
pci_tests_new_i <- list()
for (i in 1:ncol(train)) {

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
  pci_tests_new_i[[i]] <- cbind(results, p_rw = test_pci$p.value[1], p_ar = test_pci$p.value[2])
}
pci_tests_new <- rbindlist(pci_tests_new_i, fill = TRUE)

# Apply restrictions to the universe
pci_tests_eligible_new <- pci_tests_new[pvmr > 0.5 &
                                          rho > 0.5 &
                                          p_rw < 0.05 &
                                          p_ar < 0.05 &
                                          negloglik <= quantile(negloglik, probs = 0.25)]

# remove same pairs
pci_tests_eligible_new <- pci_tests_eligible_new[!(series2 %in% series1)]

# example of partially cointegrated pairs
pairs <- unlist(pci_tests_eligible_new[1, 1:2])
plot(log(train[, pairs]), main = paste0(pairs[1], " - ", pairs[2]))
# ACN  - IT usluge i savjete
# GOOG - Google!
pairs <- unlist(pci_tests_eligible_new[2, 1:2])
plot(log(train[, pairs]), main = paste0(pairs[1], " - ", pairs[2])) # log prices series of second pair
# AEP - električna tvrtka
# ED  - energetskih tvrtki
pairs <- unlist(pci_tests_eligible_new[3, 1:2])
plot(log(train[, pairs]), main = paste0(pairs[1], " - ", pairs[2])) # log prices series of second pair
# ALL - osiguravajuća tvrtka
# AFL - pružatelj dopunskog osiguranja

# all pairs
pairs_results_l <- lapply(1:nrow(pci_tests_eligible_new), function(n_) {
  pairs_trading_pci(
    y1_train = train[, unlist(pci_tests_eligible_new[n_, 1])],
    y2_train = train[, unlist(pci_tests_eligible_new[n_, 2])],
    y1_test = test[, unlist(pci_tests_eligible_new[n_, 1])],
    y2_test = test[, unlist(pci_tests_eligible_new[n_, 2])],
    std_entry = 1,
    plot_pnl = FALSE
  )
})
pairs_results_new <- rbindlist(pairs_results_l)
pairs_results_new[, date := as.Date(date)]

# spread all plots
plots <- list()
symbols_1 <- unique(pairs_results_new$symbol_1)
for (i in seq_along(symbols_1)) { # seq_along(symbols_1)
  data_ <- pairs_results_new[symbol_1 == symbols_1[i]]
  plots[[i]] <- ggplot(data_, aes(x = date, y = Z_score_M)) +
    geom_line() +
    geom_hline(yintercept = c(1, -1), color = "red") +
    ggtitle(paste0(data_$symbol_1[1], " - ", data_$symbol_2[2])) +
    theme(plot.title = element_text(size = 10),
          axis.title.y = element_blank())
}
wrap_plots(plots)

# spread plots to trade
plots <- list()
# symbols_1 <- pairs_results_new[, tail(.SD, 1), by = symbol_1][Z_score_M >= 1 | Z_score_M <= -1, symbol_1]
symbols_1 <- unique(pairs_results_new[, any(Z_score_M >= 1) | any(Z_score_M <= -1), by = symbol_1][V1 == TRUE][[1]])
for (i in seq_along(symbols_1)) { # seq_along(symbols_1)
  data_ <- pairs_results_new[symbol_1 == symbols_1[i]]
  plots[[i]] <- ggplot(data_, aes(x = date, y = Z_score_M)) +
    geom_line() +
    geom_hline(yintercept = c(1, -1), color = "red") +
    ggtitle(paste0(data_$symbol_1[1], " - ", data_$symbol_2[2])) +
    theme(plot.title = element_text(size = 10),
          axis.title.y = element_blank())
}
wrap_plots(plots)




# PORTFOLIO ---------------------------------------------------------------
# sp500 universe
fmp = FMP$new()
symbols <- fmp$get_sp500_symbols()

# import daily market data
arr <- tiledb_array("D:/equity-usa-daily-fmp",
                    as.data.frame = TRUE,
                    query_layout = "UNORDERED",
                    selected_ranges = list(symbol = cbind(symbols, symbols))
)
system.time(prices <- arr[])
tiledb_array_close(arr)
prices_dt <- as.data.table(prices)

# cleaning steps with explanations
prices_dt <- prices_dt[isBusinessDay(date)] # keep only trading days
prices_dt <- prices_dt[open > 0 & high > 0 & low > 0 & close > 0 & adjClose > 0] # remove rows with prices <= 0
prices_dt <- prices_dt[, .(symbol, date, adjClose )] # keep only columns we need
prices_dt <- unique(prices_dt, by = c("symbol", "date")) # remove duplicates
prices_dt <- na.omit(prices_dt) # remove missing values
setorder(prices_dt, "symbol", "date") # order for returns calculation
prices_dt[, returns := adjClose / data.table::shift(adjClose, 1, type = "lag") - 1, by = symbol]
prices_dt <- prices_dt[returns < 1] # TODO: better outlier detection mechanism

# define train period
test_period_start <- seq.Date(Sys.Date() - 7000, Sys.Date(), by = 1)
test_period_start <- unique(format.Date(test_period_start, format = "%Y-%m-01"))
test_period_start <- as.Date(test_period_start)
train_start <- test_period_start - (365 * 3)
train_stop <- test_period_start - 1

# find best pairs for every trainset
bl_endp_key <- storage_endpoint(Sys.getenv("BLOB-ENDPOINT-SNP"),
                                Sys.getenv("BLOB-KEY-SNP"))
cont <- storage_container(bl_endp_key, "qc-backtest")
train_start = train_start[which(test_period_start == as.Date("2004-06-01")):length(test_period_start)]
train_stop = train_stop[which(test_period_start == as.Date("2004-06-01")):length(test_period_start)]
test_period_start = test_period_start[which(test_period_start == as.Date("2004-06-01")):length(test_period_start)]
# best_pairs <- list()
for (i in seq_along(train_start)) {

  # sample
  sample_dt <- prices_dt[date %between% c(train_start[i], train_stop[i])]
  sample_dt <- dcast(sample_dt, date ~ symbol, value.var = "adjClose")

  # remove columns with NA values
  X <- as.xts.data.table(sample_dt)
  X <- X[,colSums(!(is.na(X))) == nrow(X)]
  X <- log(X)

  # choose best pairs using hedge.pci function and maxfact = 1 (only one factor possible)
  pci_tests_i <- list()
  for (j in 1:ncol(X)) {

    # quasi multivariate pairs
    print(j)
    hedge <- tryCatch(hedge.pci(X[, j], X[, -j], maxfact = 1, use.multicore = FALSE),
                      error = function(e) NULL)
    if (is.null(hedge)) {
      pci_tests_i[[j]] <- NULL
      next()
    }

    # pci fit
    test_pci <- test.pci(X[, j], hedge$pci$basis)

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
    pci_tests_i[[j]] <- cbind(results, p_rw = test_pci$p.value[1], p_ar = test_pci$p.value[2])
  }

  # save to azure for qc
  best_pairs <- rbindlist(pci_tests_i, fill = TRUE)
  file_name_ <- paste0(test_period_start[i], ".csv")
  storage_write_csv(as.data.frame(best_pairs), cont, file_name_)
  # lapply(seq_along(best_pairs_eligible), function(i) {
  #   file_name_ <- paste0(names(best_pairs_eligible[i]), ".csv")
  #   print(file_name_)
  #   storage_write_csv(as.data.frame(best_pairs_eligible[[i]][, 1:3]), cont, file_name_)
  # })
  # best_pairs[[i]] <- rbindlist(pci_tests_i, fill = TRUE)
}


######## TEST ########
fit.pci(X[, "ABC"], X[, "MCK"])
fit.pci(X[, "MCK"], X[, "ABC"])
######## TEST ########

# Apply restrictions to the universe
best_pairs_eligible <- copy(best_pairs)
names(best_pairs_eligible) <- test_period_start
best_pairs_eligible <- lapply(best_pairs_eligible, function(x) {
  x <- x[pvmr > 0.5 & rho > 0.5 & p_rw < 0.05 & p_ar < 0.05 &
      negloglik <= quantile(negloglik, probs = 0.25)]
  x[!(series2 %in% series1)]
})

# save to azure for qc
bl_endp_key <- storage_endpoint(Sys.getenv("BLOB-ENDPOINT-SNP"),
                                Sys.getenv("BLOB-KEY-SNP"))
cont <- storage_container(bl_endp_key, "qc-backtest")
lapply(seq_along(best_pairs_eligible), function(i) {
  file_name_ <- paste0(names(best_pairs_eligible[i]), ".csv")
  print(file_name_)
  storage_write_csv(as.data.frame(best_pairs_eligible[[i]][, 1:3]), cont, file_name_)
})


# TODO:
# replicirati spread i trgovanje na QC.
# Parametri:
# 1) Frekventnost podataka: mnut / sat/ dan
# 2) Klase imovine: equity, commodity, crypto
# 3) trening / testing period
# 4) fundamental matching (sector matching)



# compare QC and this script results
bl_endp_key <- storage_endpoint(Sys.getenv("BLOB-ENDPOINT-SNP"),
                                Sys.getenv("BLOB-KEY-SNP"))
cont <- storage_container(bl_endp_key, "qc-backtest")
pairs_202101 <- storage_read_csv(cont, "2021-01-01.csv")

# ABC - MCK
symbols_ <- c("ABC", "MCK")
arr <- tiledb_array("D:/equity-usa-daily-fmp",
                    as.data.frame = TRUE,
                    query_layout = "UNORDERED",
                    selected_ranges = list(date = cbind(as.Date("2018-01-01"), Sys.Date() - 1),
                                           symbol = cbind(symbols_, symbols_))
)
system.time(prices_ <- arr[])
tiledb_array_close(arr)
prices_dt_ <- as.data.table(prices_)
prices_dt_ <- prices_dt_[isBusinessDay(date)] # keep only trading days
prices_dt_ <- prices_dt_[open > 0 & high > 0 & low > 0 & close > 0 & adjClose > 0] # remove rows with prices <= 0
prices_dt_ <- prices_dt_[, .(symbol, date, adjClose )] # keep only columns we need
prices_dt_ <- unique(prices_dt_, by = c("symbol", "date")) # remove duplicates
prices_dt_ <- na.omit(prices_dt_) # remove missing values
setorder(prices_dt_, "symbol", "date") # order for returns calculation
prices_dt_[, returns := adjClose / data.table::shift(adjClose, 1, type = "lag") - 1, by = symbol]
prices_dt_ <- prices_dt_[returns < 1] # TODO: better outlier detection mechanism
test_period_start <- seq.Date(Sys.Date() - 700, Sys.Date(), by = 1)
test_period_start <- unique(format.Date(test_period_start, format = "%Y-%m-01"))
test_period_start <- as.Date(test_period_start)
train_start <- test_period_start - (365 * 3)
train_stop <- test_period_start - 1
y1_train <- prices_dt_[symbol == "ABC" & date %between% c(train_start[1], train_stop[1]), adjClose]
y2_train <- prices_dt_[symbol == "MCK" & date %between% c(train_start[1], train_stop[1]), adjClose]
y1_test <- prices_dt_[symbol == "ABC" & date %between% c(test_period_start[1], test_period_start[1] + 30), adjClose]
y2_test <- prices_dt_[symbol == "MCK" & date %between% c(test_period_start[1], test_period_start[1] + 30), adjClose]
library(httr)
library(jsonlite)
res <- POST("https://cgsalpha.azurewebsites.net/pairs",
            body = list(
              y1_train = toJSON(y1_train),
              y2_train = toJSON(y2_train),
              y1_test = toJSON(y1_test),
              y2_test = toJSON(y2_test)
            ))
spreads <- content(res)
train_spread <- unlist(spreads$spread_train)
test_spread <- unlist(spreads$spread_test)
spread_ <- c(train_spread, test_spread)
spread_z_ <- (spread_ - mean(train_spread)) / sd(train_spread)
dates_ <- prices_dt_[date %between% c(train_start[1], test_period_start[1] + 30), unique(date)]
spread_final <- cbind.data.frame(date = dates_, spread = spread_z_)
ggplot(spread_final, aes(x = date, y = spread)) +
  geom_line() +
  geom_hline(yintercept = c(1, -1), color = "blue") +
  geom_vline(xintercept = c(train_stop[1]), color = "red")
ggplot(spread_final[spread_final$date > as.Date(test_period_start[1]),], aes(x = date, y = spread)) +
  geom_line() +
  geom_hline(yintercept = c(1, -1), color = "blue") +
  geom_vline(xintercept = c(train_stop[1]), color = "red")


# tet if API endpoinr works as expected
# y1_train <- rnorm(500)
# y2_train <- rnorm(500)
# y1_test <- rnorm(500)
# y2_test <- rnorm(500)
#
#
# res <- POST("http://127.0.0.1:8000/pairs",
#             body = list(
#               y1_train = toJSON(rnorm(500)),
#               y2_train = toJSON(rnorm(500)),
#               y1_test = toJSON(rnorm(500)),
#               y2_test = toJSON(rnorm(500))
#             ))
# x <- content(res)
# unlist(x$spread_train)
