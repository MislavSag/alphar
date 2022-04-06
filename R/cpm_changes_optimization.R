library(data.table)
library(AzureStor)
library(roll)
library(TTR)
library(future.apply)
library(ggplot2)
library(PerformanceAnalytics)
library(runner)
library(cpm)
require(finfeatures, lib.loc = "C:/Users/Mislav/Documents/GitHub/finfeatures/renv/library/R-4.1/x86_64-w64-mingw32")




# SET UP ------------------------------------------------------------------

# get data from azure
ENDPOINT = storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), key=Sys.getenv("BLOB-KEY"))
CONT = storage_container(ENDPOINT, "equity-usa-hour-fmpcloud-adjusted")
CONTMIN = storage_container(ENDPOINT, "equity-usa-minute-fmpcloud")
fmpcloudr::fmpc_set_token(Sys.getenv("APIKEY-FMPCLOUD"))


# IMPORT DATA -------------------------------------------------------------

# import all data from Azure storage
azure_blobs <- list_blobs(CONT)
market_data_list <- lapply(azure_blobs$name, function(x) {
  print(x)
  y <- tryCatch(storage_read_csv2(CONT, x), error = function(e) NA)
  if (is.null(y) | all(is.na(y))) return(NULL)
  y <- cbind(symbol = x, y)
  return(y)
})
market_data <- rbindlist(market_data_list)
market_data[, symbol := toupper(gsub("\\.csv", "", symbol))]
market_data[, returns := close / shift(close) - 1, by = .(symbol)]
market_data <- na.omit(market_data)
market_data$datetime <- as.POSIXct(as.numeric(market_data$datetime),
                                   origin=as.POSIXct("1970-01-01", tz="EST"),
                                   tz="EST")
market_data <- market_data[close > 1e-005 & open > 1e-005 & high > 1e-005 & low > 1e-005]
market_data <- unique(market_data, by = c("symbol", "datetime"))
market_data_n <- market_data[, .N, by = symbol]
market_data_n <- market_data_n[which(market_data_n$N > 8 * 5 * 22 * 12)]  # remove prices with only 60 or less observations
market_data <- market_data[symbol %in% market_data_n$symbol]


returns <- market_data[symbol == "AAPL", "returns"][[1]]
returns <- returns[1:306]
method <- "Mood"
arl0 <- 500 # 370, 500, 600, 700, ..., 1000, 2000, 3000, ..., 10000, 20000, ..., 50000

get_changepoints <- function(returns, method, arl0) {

  # change points roll
  detectiontimes <- numeric()
  changepoints <- numeric()
  cpm <- makeChangePointModel(cpmType=method, ARL0=arl0, startup=200)
  i <- 0
  while (i < length(returns)) {

    i <- i + 1

    # if returns is na returns FALSE
    if (is.na(returns[i])) {
      next()
    }

    # process each observation in turn
    cpm <- processObservation(cpm, returns[i])

    # if a change has been found, log it, and reset the CPM
    if (changeDetected(cpm) == TRUE) {
      detectiontimes <- c(detectiontimes,i)

      # the change point estimate is the maximum D_kt statistic
      Ds <- getStatistics(cpm)
      tau <- which.max(Ds)
      if (length(changepoints) > 0) {
        tau <- tau + changepoints[length(changepoints)]
      }
      changepoints <- c(changepoints,tau)

      # reset the CPM
      cpm <- cpmReset(cpm)

      # resume monitoring from the observation following the change point
      i <- tau
    }
  }
  points <- cbind.data.frame(detectiontimes, changepoints)
  breaks <- rep(FALSE, length(returns))
  breaks[detectiontimes] <- TRUE
  change <- rep(FALSE, length(returns))
  change[changepoints] <- TRUE
  return(cbind.data.frame(breaks, change))
}

test_ <- get_changepoints(returns, method, 500) # 370, 500, 600, 700, ..., 1000, 2000, 3000, ..., 10000, 20000, ..., 50000
which(test_$breaks == TRUE)
which(test_$change == TRUE)

results <- processStream(returns[1:308], cpmType = "Mood", ARL0 = 500, startup = 20)
results

data("ForexData", package = "cpm")
ForexData
head(ForexData)

x <- ForexData[, 3]
plot(x, type = "l", xlab = "Observation", ylab = "log(Exchange Rates)", bty = "l")
diffs <- diff(x)
results <- processStream(diffs, cpmType = "Mood", ARL0 = 5000, startup = 20)
length(results$changePoints)



# INDICATORS --------------------------------------------------------------

#
Return.cumulative(backtest_data$returns)






# MINMAX INDICATORS -------------------------------------------------------

# calculate rolling quantiles
market_data[, p_999_4year := roll_quantile(returns, 255*8*4, p = 0.999), by = .(symbol)]
market_data[, p_001_4year := roll_quantile(returns, 255*8*4, p = 0.001), by = .(symbol)]
market_data[, p_999_2year := roll_quantile(returns, 255*8*2, p = 0.999), by = .(symbol)]
market_data[, p_001_2year := roll_quantile(returns, 255*8*2, p = 0.001), by = .(symbol)]
market_data[, p_999_year := roll_quantile(returns, 255*8, p = 0.999), by = .(symbol)]
market_data[, p_001_year := roll_quantile(returns, 255*8, p = 0.001), by = .(symbol)]
market_data[, p_999_halfyear := roll_quantile(returns, 255*4, p = 0.999), by = .(symbol)]
market_data[, p_001_halfyear := roll_quantile(returns, 255*4, p = 0.001), by = .(symbol)]

market_data[, p_99_4year := roll_quantile(returns, 255*8*4, p = 0.99), by = .(symbol)]
market_data[, p_01_4year := roll_quantile(returns, 255*8*4, p = 0.01), by = .(symbol)]
market_data[, p_99_2year := roll_quantile(returns, 255*8*2, p = 0.99), by = .(symbol)]
market_data[, p_01_2year := roll_quantile(returns, 255*8*2, p = 0.01), by = .(symbol)]
market_data[, p_99_year := roll_quantile(returns, 255*8, p = 0.99), by = .(symbol)]
market_data[, p_01_year := roll_quantile(returns, 255*8, p = 0.01), by = .(symbol)]
market_data[, p_99_halfyear := roll_quantile(returns, 255*4, p = 0.99), by = .(symbol)]
market_data[, p_01_halfyear := roll_quantile(returns, 255*4, p = 0.01), by = .(symbol)]

market_data[, p_97_4year := roll_quantile(returns, 255*8*4, p = 0.97), by = .(symbol)]
market_data[, p_03_4year := roll_quantile(returns, 255*8*4, p = 0.03), by = .(symbol)]
market_data[, p_97_2year := roll_quantile(returns, 255*8*2, p = 0.97), by = .(symbol)]
market_data[, p_03_2year := roll_quantile(returns, 255*8*2, p = 0.03), by = .(symbol)]
market_data[, p_97_year := roll_quantile(returns, 255*8, p = 0.97), by = .(symbol)]
market_data[, p_03_year := roll_quantile(returns, 255*8, p = 0.03), by = .(symbol)]
market_data[, p_97_halfyear := roll_quantile(returns, 255*4, p = 0.97), by = .(symbol)]
market_data[, p_03_halfyear := roll_quantile(returns, 255*4, p = 0.03), by = .(symbol)]

market_data[, p_95_4year := roll_quantile(returns, 255*8*4, p = 0.95), by = .(symbol)]
market_data[, p_05_4year := roll_quantile(returns, 255*8*4, p = 0.05), by = .(symbol)]
market_data[, p_95_2year := roll_quantile(returns, 255*8*2, p = 0.95), by = .(symbol)]
market_data[, p_05_2year := roll_quantile(returns, 255*8*2, p = 0.05), by = .(symbol)]
market_data[, p_95_year := roll_quantile(returns, 255*8, p = 0.95), by = .(symbol)]
market_data[, p_05_year := roll_quantile(returns, 255*8, p = 0.05), by = .(symbol)]
market_data[, p_95_halfyear := roll_quantile(returns, 255*4, p = 0.95), by = .(symbol)]
market_data[, p_05_halfyear := roll_quantile(returns, 255*4, p = 0.05), by = .(symbol)]

# exrtreme returns
cols <- colnames(market_data)[grep("^p_9", colnames(market_data))]
cols_new <- paste0("above_", cols)
market_data[, (cols_new) := lapply(.SD, function(x) ifelse(returns > x, returns - x, 0)), by = .(symbol), .SDcols = cols]
cols <- colnames(market_data)[grep("^p_0", colnames(market_data))]
cols_new <- paste0("below_", cols)
market_data[, (cols_new) := lapply(.SD, function(x) ifelse(returns < x, abs(returns - x), 0)), by = .(symbol), .SDcols = cols]

# crate dummy variables
cols <- colnames(market_data)[grep("^p_9", colnames(market_data))]
cols_new <- paste0("above_dummy_", cols)
market_data[, (cols_new) := lapply(.SD, function(x) ifelse(returns > x, 1, 0)), by = .(symbol), .SDcols = cols]
cols <- colnames(market_data)[grep("^p_0", colnames(market_data))]
cols_new <- paste0("below_dummy_", cols)
market_data[, (cols_new) := lapply(.SD, function(x) ifelse(returns < x, 1, 0)), by = .(symbol), .SDcols = cols]

# get tail risk mesures with sum
cols <- colnames(market_data)[grep("below_p|above_p|dummy", colnames(market_data))]
indicators <- market_data[, lapply(.SD, function(x) sum(x, na.rm = TRUE)), by = .(datetime), .SDcols = cols]
colnames(indicators) <- c("datetime", paste0("sum_", cols))
setorder(indicators, datetime)
above_sum_cols <- colnames(indicators)[grep("above", colnames(indicators))]
below_sum_cols <- colnames(indicators)[grep("below", colnames(indicators))]
excess_sum_cols <- gsub("above", "excess", above_sum_cols)
indicators[, (excess_sum_cols) := indicators[, ..above_sum_cols] - indicators[, ..below_sum_cols]]

# get tail risk mesures with sd
cols <- colnames(market_data)[grep("below_p|above_p|dummy", colnames(market_data))]
indicators_sd <- market_data[, lapply(.SD, function(x) sd(x, na.rm = TRUE)), by = .(datetime), .SDcols = cols]
colnames(indicators_sd) <- c("datetime", paste0("sd_", cols))
setorder(indicators_sd, datetime)
above_sum_cols <- colnames(indicators_sd)[grep("above", colnames(indicators_sd))]
below_sum_cols <- colnames(indicators_sd)[grep("below", colnames(indicators_sd))]
excess_sum_cols <- gsub("above", "excess", above_sum_cols)
indicators_sd[, (excess_sum_cols) := indicators_sd[, ..above_sum_cols] - indicators_sd[, ..below_sum_cols]]

# merge indicators
indicators <- merge(indicators, indicators_sd, by = c("datetime"), all.x = TRUE, all.y = FALSE)

# merge spy
spy <- market_data[symbol == "SPY", .(datetime, returns)]
indicators <- merge(indicators, spy, by = "datetime")




# OPTIMIZE STRATEGY -------------------------------------------------------

# params for returns
sma_width <- 1:60
threshold <- seq(-0.1, 0, by = 0.002)
vars <- colnames(indicators)[grep("sum_excess_p", colnames(indicators))]
paramset <- expand.grid(sma_width, threshold, vars, stringsAsFactors = FALSE)
colnames(paramset) <- c('sma_width', 'threshold', "vars")

# params for returns
rsi_width <- 10:50
threshold <- seq(30, 70, by = 1)
vars <- colnames(indicators)[grep("sum_excess_p", colnames(indicators))]
paramset_rsi <- expand.grid(rsi_width, threshold, vars, stringsAsFactors = FALSE)
colnames(paramset_rsi) <- c('rsi_width', 'threshold', "vars")

# params for dummies
threshold <- seq(0, 60, by = 1)
vars <- colnames(indicators)[grep("sum_excess_dumm", colnames(indicators))]
paramset_dummy <- expand.grid(threshold, vars, stringsAsFactors = FALSE)
colnames(paramset_dummy) <- c('threshold', "vars")

# params for dummies sd
sma_width <- 1:60
threshold <- seq(0.001, 0.10, by = 0.001)
vars <- colnames(indicators)[grep("sd_excess_dumm", colnames(indicators))]
paramset_dummy_sd <- expand.grid(sma_width, threshold, vars, stringsAsFactors = FALSE)
colnames(paramset_dummy_sd) <- c("sma_width", 'threshold', "vars")


# backtst function
backtest <- function(returns, indicator, threshold, return_cumulative = TRUE) {
  sides <- vector("integer", length(indicator))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(indicator[i-1])) {
      sides[i] <- NA
    } else if (indicator[i-1] < threshold) {
      sides[i] <- 0
    } else {
      sides[i] <- 1
    }
  }
  sides <- ifelse(is.na(sides), 1, sides)
  returns_strategy <- returns * sides
  if (return_cumulative) {
    return(PerformanceAnalytics::Return.cumulative(returns_strategy))
  } else {
    return(returns_strategy)
  }
}

backtest_dummy <- function(returns, indicator, threshold, return_cumulative = TRUE) {
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
  returns_strategy <- returns * sides
  if (return_cumulative) {
    return(PerformanceAnalytics::Return.cumulative(returns_strategy))
  } else {
    return(returns_strategy)
  }
}

# backtset
# plan(multiprocess(workers = 4L))
cum_returns_f <- function(paramset) {
  cum_returns <- vapply(1:nrow(paramset), function(x) {
    excess_sma <- SMA(indicators[, get(paramset[x, 3])], paramset[x, 1])
    results <- backtest(indicators$returns, excess_sma, paramset[x, 2])
    return(results)
  }, numeric(1))
  results <- as.data.table(cbind(paramset, cum_returns))
}
cum_returns_dt <- cum_returns_f(paramset)
setorder(cum_returns_dt, cum_returns)

# backtset rsi
cum_returns_rsi <- function(paramset) {
  cum_returns <- vapply(1:nrow(paramset), function(x) {
    excess_sma <- RSI(indicators[, get(paramset[x, 3])], paramset[x, 1])
    results <- backtest_dummy(indicators$returns, excess_sma, paramset[x, 2])
    return(results)
  }, numeric(1))
  results <- as.data.table(cbind(paramset, cum_returns))
}
cum_returns_rsi_dt <- cum_returns_rsi(paramset_rsi)
setorder(cum_returns_rsi_dt, cum_returns)

# backtest optimization for dummy indicators
cum_returns_dummy <- function(paramset) {
  cum_returns <- vapply(1:nrow(paramset), function(x) {
    results <- backtest_dummy(indicators$returns, indicators[, get(paramset[x, 2])], paramset[x, 1])
    return(results)
  }, numeric(1))
  results <- as.data.table(cbind(paramset, cum_returns))
}
cum_returns_dummy_dt <- cum_returns_dummy(paramset_dummy)
setorder(cum_returns_dummy_dt, cum_returns)

# backtest optimization for dummy sd indicators
cum_returns_sd <- function(paramset) {
  cum_returns <- vapply(1:nrow(paramset), function(x) {
    excess_sma <- SMA(na.omit(abs(indicators[, get(paramset[x, 3])])), paramset[x, 1])
    results <- backtest_dummy(indicators$returns, excess_sma, paramset[x, 2])
    return(results)
  }, numeric(1))
  results <- as.data.table(cbind(paramset, cum_returns))
}
cum_returns_sd_dt <- cum_returns_sd(paramset_dummy_sd)
setorder(cum_returns_sd_dt, cum_returns)

# n best
head(cum_returns_dt, 50)
tail(cum_returns_dt, 100)
head(cum_returns_rsi_dt, 50)
tail(cum_returns_rsi_dt, 50)
head(cum_returns_dummy_dt, 50)
tail(cum_returns_dummy_dt, 50)
head(cum_returns_sd_dt, 50)
tail(cum_returns_sd_dt, 50)

# summary statistics
dcast(cum_returns_dt, vars ~ ., value.var = "cum_returns", fun.aggregate = median)
dcast(cum_returns_dt, vars ~ ., value.var = "cum_returns", fun.aggregate = mean)
dcast(cum_returns_dt, vars ~ ., value.var = "cum_returns", fun.aggregate = quantile, p = 0.10)
dcast(cum_returns_dt, vars ~ ., value.var = "cum_returns", fun.aggregate = quantile, p = 0.90)
dcast(cum_returns_dt, vars ~ ., value.var = "cum_returns", fun.aggregate = quantile, p = 0.80)
skimr::skim(cum_returns_dt)

# plots
ggplot(cum_returns_dt, aes(cum_returns)) +
  geom_histogram() +
  geom_vline(xintercept = PerformanceAnalytics::Return.cumulative(indicators$returns), color = "red")
ggplot(cum_returns_dt, aes(x = sma_width, y = threshold, fill = cum_returns)) +
  geom_tile()
ggplot(cum_returns_dt[sma_width %in% 1:10 & grepl("excess_p_97", vars)],
       aes(x = sma_width, y = threshold, fill = cum_returns)) +
  geom_tile() +
  facet_grid(cols = vars(vars))
ggplot(cum_returns_dt[sma_width %in% 1:10 & grepl("excess_p_95", vars)],
       aes(x = sma_width, y = threshold, fill = cum_returns)) +
  geom_tile() +
  facet_grid(cols = vars(vars))
ggplot(cum_returns_dt[sma_width %in% 1:10 & grepl("excess_p_99_", vars)],
       aes(x = sma_width, y = threshold, fill = cum_returns)) +
  geom_tile() +
  facet_grid(cols = vars(vars))
ggplot(cum_returns_dt[sma_width %in% 1:10 & grepl("excess_p_999", vars)],
       aes(x = sma_width, y = threshold, fill = cum_returns)) +
  geom_tile() +
  facet_grid(cols = vars(vars))
ggplot(cum_returns_dt, aes(x = vars, y = sma_width, fill = cum_returns)) +
  geom_tile() +
  theme(text = element_text(size=12),
        axis.text.x = element_text(angle=90, hjust=1))
ggplot(cum_returns_dt[sma_width %in% 1:10 & threshold %between% c(-0.01, -0.001)],
       aes(x = sma_width, y = threshold, fill = cum_returns)) +
  geom_tile()
ggplot(cum_returns_dt[sma_width %in% 1:10 & threshold %between% c(-0.01, -0.001) & vars == "sum_excess_p_97_halfyear"],
       aes(x = sma_width, y = threshold, fill = cum_returns)) +
  geom_tile()

# best vriable
best_var <- "sum_excess_p_99_year"
ggplot(cum_returns_dt[vars == best_var], aes(cum_returns)) +
  geom_histogram() +
  geom_vline(xintercept = PerformanceAnalytics::Return.cumulative(indicators$returns), color = "red")
ggplot(cum_returns_dt[vars == best_var],
       aes(x = sma_width, y = threshold, fill = cum_returns)) +
  geom_tile() +
  facet_grid(cols = vars(vars))

# best var and sma width
best_var <- "sum_excess_p_99_year"
sma_width_best <- 6
ggplot(cum_returns_dt[vars == best_var & sma_width == sma_width_best], aes(cum_returns)) +
  geom_histogram() +
  geom_vline(xintercept = PerformanceAnalytics::Return.cumulative(indicators$returns), color = "red")
ggplot(cum_returns_dt[vars == best_var],
       aes(x = sma_width, y = threshold, fill = cum_returns)) +
  geom_tile() +
  facet_grid(cols = vars(vars))


# plots rsi
ggplot(cum_returns_rsi_dt, aes(cum_returns)) +
  geom_histogram() +
  geom_vline(xintercept = PerformanceAnalytics::Return.cumulative(indicators$returns), color = "red")
ggplot(cum_returns_rsi_dt, aes(x = rsi_width, y = threshold, fill = cum_returns)) +
  geom_tile()
ggplot(cum_returns_rsi_dt[rsi_width %in% 46:54], aes(x = rsi_width, y = threshold, fill = cum_returns)) +
  geom_tile()

# plots for dummies
ggplot(cum_returns_dummy, aes(cum_returns)) +
  geom_histogram() +
  geom_vline(xintercept = PerformanceAnalytics::Return.cumulative(indicators$returns), color = "red")

# best backtest
sma_width_param <- 5
threshold_param <- -0.01
vars_param <- "sum_excess_p_99_year"
excess_sma <- SMA(indicators[, get(vars_param)], sma_width_param)
strategy_returns <- backtest(indicators$returns, excess_sma, threshold_param, return_cumulative = FALSE)
charts.PerformanceSummary(xts(cbind(indicators$returns, strategy_returns), order.by = indicators$datetime))
charts.PerformanceSummary(xts(cbind(indicators$returns[28000:nrow(indicators)], strategy_returns[28000:nrow(indicators)]),
                              order.by = indicators$datetime[28000:nrow(indicators)]))

# best backtest for dummy
threshold_param <- 10
vars_param <- "sum_excess_dummy_p_99_2year"
strategy_returns <- backtest_dummy(indicators$returns, indicators[, get(vars_param)], threshold_param, return_cumulative = FALSE)
charts.PerformanceSummary(xts(cbind(indicators$returns, strategy_returns), order.by = indicators$datetime))

# best backtest for dummy sd
sma_width_param <- 24
threshold <- 0.05
vars_param <- "sd_excess_dummy_p_97_halfyear"
excess_sma <- SMA(na.omit(abs(indicators[, get(paramset[x, 3])])), paramset[x, 1])
strategy_returns <- backtest_dummy(indicators$returns, excess_sma, threshold_param, return_cumulative = FALSE)
charts.PerformanceSummary(xts(cbind(indicators$returns, strategy_returns), order.by = indicators$datetime))


# save best strategy to azure
keep_cols <- colnames(indicators)[grep("datetime|sum_excess_p", colnames(indicators))]
indicators_azure <- indicators[, ..keep_cols]
cols <- colnames(indicators_azure)[2:ncol(indicators_azure)]
indicators_azure[, (cols) := lapply(.SD, shift), .SDcols = cols]
indicators_azure <- na.omit(indicators_azure)
file_name <- "D:/risks/minmax/minmax.csv"
fwrite(indicators_azure, file_name, col.names = FALSE, dateTimeAs = "write.csv")
bl_endp_key <- storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), key=Sys.getenv("BLOB-KEY"))
cont <- storage_container(bl_endp_key, "qc-backtest")
storage_upload(cont, file_name, basename(file_name))
# https://contentiobatch.blob.core.windows.net/qc-backtest/minmax.csv?sp=r&st=2022-01-01T11:15:54Z&se=2023-01-01T19:15:54Z&sv=2020-08-04&sr=b&sig=I0Llnk3ELMOJ7%2FJ2i2VzQOxCFEOTbmFaEHzXnzLJ7ZQ%3D



# VAR
library(vars)
keep_cols_var <- colnames(indicators)[grepl("sd_excess_dum.*2ye", colnames(indicators))]
keep_cols_var <- c("datetime", "returns", keep_cols_var)
X <- indicators[, ..keep_cols_var]
X <- na.omit(X)
# X <- X[datetime %between% c("2015-01-01", "2020-02-26")]
# X <- as.xts.data.table(X)
# res <- VAR(as.xts.data.table(X[1:1000]), p = 8, type = "both")
# preds <- predict(res, n.ahead = 8, ci = 0.95)
# p <- preds$fcst$returns


# predictions for every period
roll_var <- runner(
  x = X,
  f = function(x) {
    res <- VAR(as.xts.data.table(x), p = 8, type = "both")
    p <- predict(res, n.ahead = 8, ci = 0.95)
    p <- p$fcst$returns
    data.frame(first = p[1, 1], median = median(p[, 1]), mean = mean(p[, 1]), sd = sd(p[, 1]), min = min(p[, 1]))
  },
  k = 1500,
  lag = 0L,
  na_pad = TRUE
)
predictions_var <- lapply(roll_var, as.data.table)
predictions_var <- rbindlist(predictions_var, fill = TRUE)
predictions_var[, V1 := NULL]
predictions_var <- cbind(datetime = X[, datetime], predictions_var)
predictions_var <- merge(spy, predictions_var, by = "datetime", all.x = TRUE, all.y = FALSE)
predictions_var <- na.omit(predictions_var)
tail(predictions_var, 10)

# backtest apply
Return.cumulative(predictions_var$returns)
backtest(predictions_var$returns, predictions_var$first, -0.005)
backtest(predictions_var$returns, predictions_var$median, -0.003)
backtest(predictions_var$returns, predictions_var$mean, -0.005)
backtest(predictions_var$returns, predictions_var$min, -0.04)
backtest_dummy(predictions_var$returns, predictions_var$sd, 0.002)
x <- backtest(predictions_var$returns, predictions_var$first, -0.006, FALSE)
charts.PerformanceSummary(as.xts(cbind(predictions_var$returns, x), order.by = predictions_var$datetime))
x <- backtest_dummy(predictions_var$returns, predictions_var$sd, 0.002, FALSE)
charts.PerformanceSummary(as.xts(cbind(predictions_var$returns, x), order.by = predictions_var$datetime))



# BigVAR
library(BigVAR)
data(Y)
# Create a Basic VAR-L (Lasso Penalty) with maximum lag order p=4, 10 gridpoints with lambda optimized according to rolling validation of 1-step ahead MSFE
mod1 <- constructModel(Y,p=4,"Basic",gran=c(150,10),RVAR=FALSE,h=1,cv="Rolling",MN=FALSE,verbose=FALSE,IC=TRUE)
results=cv.BigVAR(mod1)
results
nrow(Y)
predict(results,n.ahead=1)

model <- constructModel(as.xts.data.table(X[1:100, ]),p=4,"Basic",gran=c(150,10),RVAR=FALSE,h=1,cv="Rolling",MN=FALSE,verbose=FALSE,IC=TRUE)
results=cv.BigVAR(model)
predict(results,n.ahead=1)

# predictions for every period
x <- X[1:500]
roll_var <- runner(
  x = X,
  f = function(x) {
    model <- constructModel(as.matrix(x[, -1]), p = 8, "Basic",gran=c(150,10),RVAR=FALSE,h=1,cv="Rolling",MN=FALSE,verbose=FALSE,IC=TRUE)
    model_results <- cv.BigVAR(model)
    p <- predict(model_results, n.ahead=1)
    data.frame(first = p[1])
  },
  k = 5000,
  lag = 0L,
  na_pad = TRUE
)
predictions_var <- lapply(roll_var, as.data.table)
predictions_var <- rbindlist(predictions_var, fill = TRUE)
predictions_var[, V1 := NULL]
predictions_var <- cbind(datetime = X[, datetime], predictions_var)
predictions_var <- merge(spy, predictions_var, by = "datetime", all.x = TRUE, all.y = FALSE)
predictions_var <- na.omit(predictions_var)
tail(predictions_var, 10)
