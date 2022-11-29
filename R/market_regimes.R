library(HistDAWass)
library(data.table)
library(ggplot2)
library(QuantTools)
library(future.apply)
library(PerformanceAnalytics)
library(runner)
library(tiledb)
library(highfrequency)
library(lubridate)
library(timechange)
library(patchwork)
library(MASS)
library(univariateML)



# HELP FUNCTIONS ----------------------------------------------------------
# backtst function
backtest <- function(returns, indicator, buy_cluster, return_cumulative = TRUE) {
  sides <- vector("integer", length(indicator))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(indicator[i-1])) {
      sides[i] <- NA
    } else if (indicator[i-1] %in% buy_cluster) {
      sides[i] <- 1
    } else {
      sides[i] <- 0
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

# predict clusters (source: https://github.com/Airpino/HistDAWass/issues/4)
predict_WH_kmeans<- function(res, test, clu = 3){
  ID_pred<-rep(0,nrow(test@M))
  for (i in 1:length(ID_pred)){
    mindist<- Inf

    for (j in 1:clu){
      tmpdist<- 0
      for (v in 1:get.MatH.ncols(test)){
        tmpdist<-tmpdist+WassSqDistH(test@M[i,v][[1]], #the test point
                                     res$solution$centers@M[j,v][[1]]) #the center of cluster j
      }
      if (tmpdist<mindist){
        mindist<- tmpdist
        ID_pred[i]<-j
      }
    }

  }
  names(ID_pred)<-get.MatH.rownames(test)
  return(ID_pred)
}

# configure s3
config <- tiledb_config()
config["vfs.s3.aws_access_key_id"] <- Sys.getenv("AWS-ACCESS-KEY")
config["vfs.s3.aws_secret_access_key"] <- Sys.getenv("AWS-SECRET-KEY")
config["vfs.s3.region"] <- Sys.getenv("AWS-REGION")
context_with_config <- tiledb_ctx(config)


# DUKASCOPY SPY DATA ----------------------------------------------------------
# import data
arr <- tiledb_array("D:/equity-usa-quotes-dukascopy",
                    as.data.frame = TRUE,
                    selected_ranges = list(symbol = cbind("SPYUSUSD", "SPYUSUSD")))
system.time(quotes_raw <- arr[])
tiledb_array_close(arr)

# prepare data for highfrequency package
quotes <- as.data.table(quotes_raw)
setnames(quotes,
         c("symbol", "bidp", "bidv", "askp", "askv", "date"),
         c("SYMBOL", "BID", "BIDSIZ", "OFR", "OFRSIZ", "DT"))
quotes[, EX := "P"]
cols <- colnames(sampleQDataRaw)
quotes <- quotes[, ..cols]
quotes[, DT := as.nanotime(DT, tz = "GMT")]

# clean quotes data
dim(quotes)
quotes <- noZeroQuotes(quotes)
dim(quotes)

# keep only exchange hours (my method because highfrequency approach is too slow)
dates_ <- as.nanotime(unique(nano_floor(quotes$DT, nanoperiod(days = 1), tz = "UTC")))
dates_start <- dates_ + as.nanoduration("14:30:00")
dates_end <- dates_ + as.nanoduration("21:00:00")
intervlas_ <- nanoival(dates_start, dates_end)
quotes <- quotes[DT %in% intervlas_]
dim(quotes)

# remove negative spreads
quotes <- rmNegativeSpread(quotes)
dim(quotes)

# remove large spreads
quotes <- rmLargeSpread(quotes)
dim(quotes)

# # merge quotes time stamp with selection criteria
# # here I decided to use POSIXct instead of nanotime, but could keep nanotime...
# quotes[, DT := nano_ceiling(DT, as.nanoduration("00:00:01"))]
# quotes[, DT := as.POSIXct(DT, tz = "UTC")]
# system.time(quotes <- mergeQuotesSameTimestamp(test, "median"))
# dim(quotes)

# remove outliers
quotes[, DT := nano_ceiling(DT, as.nanoduration("00:00:01"))]
quotes[, DT := as.POSIXct(DT, tz = "UTC")]
system.time(quotes <- rmOutliersQuotes(quotes, type = "advanced"))
dim(quotes)

test <- quotes[, as.POSIXct(DT)]
test <- as.ITime(test)
test_dt <- as.data.table(test)[, .N, by = test]
setorder(test_dt, N)
tset <- highfrequency::aggregateQuotes(quotes, "minutes", 5, )



# FINAM TICK DATA ---------------------------------------------------------
# import from s3
arr <- tiledb_array("D:/equity-usa-tick-finam",
                    as.data.frame = TRUE,
                    selected_ranges = list(symbol = cbind("AAPL", "AAPL")))
system.time(tick_raw <- arr[])
tiledb_array_close(arr)

# prepare data for highfrequency package
tick_raw <- as.data.table(tick_raw)
setnames(tick_raw,
         c("symbol", "volume", "price", "time"),
         c("SYMBOL", "SIZE", "PRICE", "DT"))
tick_raw[, EX := "P"]
tick_raw[, COND := "I"]
tick_raw[, CORR := 0]
cols <- colnames(sampleTDataRaw)
tick_raw <- tick_raw[, ..cols]
attr(tick_raw$DT, "tz") <- "UTC"
tick_raw[, DT := with_tz(DT, tzone = "America/New_York")]

# # clean tick data
# dim(tick_raw)
# tick_dt <- noZeroPrices(tick_raw)
# dim(tick_dt)
#
# # keep only exchange hours (my method because highfrequency approach is too slow)
# tick_dt[, time := as.ITime(DT)]
# tick_dt <- tick_dt[time %between% c("09:30:00", "16:00:00")]
# dim(tick_dt)
#
# # merge trades with same timestamp
# tick_dt <- mergeTradesSameTimestamp(tick_dt)
# dim(tick_dt)



# VOLUME PROFILE ----------------------------------------------------------
# calculate volume profile
tick_dt <- tick_raw[, .(DT, PRICE, SIZE)]
setnames(tick_dt, c("time", "price", "volume"))
tick_dt <- tick_dt[volume > 0]
tick_dt[, volume := log(volume)]
vp = roll_volume_profile(tick_dt, 60 * 60 * 4, 0.01, 0.98, 100)
length(vp)
length(vp[[1]])
length(vp[[2]])
vp[[1]][1:3]
vp[[2]][1:3]

# visualize
data_plot <- as.data.frame(vp[[2]][[1]])
ggplot(data_plot, aes(price)) + geom_bar(aes(weight = volume))

# create histograms from real data
volumes <- lapply(vp[[2]], `[[`, 3)
volumes_dist <- lapply(volumes, data2hist)
plot(volumes_dist[[66]])

# create MatH object
new_mat<-MatH(x=volumes_dist,
              nrows=length(volumes_dist),
              ncols = 1,
              rownames = vp[[1]],
              varnames = "volumep")
new_mat<-MatH(x=tail(volumes_dist, 1000),
              nrows = 1000,
              ncols = 1,
              rownames = tail(vp[[1]], 1000),
              varnames = "volumep")

# save object
# saveRDS(new_mat, "D:/features/volumes_dist_vol_h4.rds")

# 1. Kmeans clustering
k_ <- 2
res_cluster <- WH_kmeans(new_mat, k = k_, rep = 10, simplify = TRUE, qua = 20, standardize = TRUE)
dates <- as.POSIXct(as.integer(rownames(new_mat@M)), origin = as.POSIXct("1970-01-01 00:00:00"), tz = "America/New_York")
clusters_1 <- data.table(date = dates, cluster_1 = res_cluster$solution$IDX)

# 2. Hierarchical clustering
# res_hcluster <- WH_hclust(new_mat, simplify = TRUE, method = "complete")
# plot(res_hcluster) # it plots the dendrogram
# clusters_2 <- cutree(res_hcluster, k = 3) # it returns the labels for 5 clusters

# 3. Fuzzy c-means of a dataset of histogram-valued data
res_fcmeans <- WH_fcmeans(new_mat, k = k_, m = 1.5, rep = 10, simplify = TRUE, qua = 20, standardize = TRUE)
clusters_3 <- clusters_3[, .(date = dates, cluster_3 = res_fcmeans$solution$IDX)]

# 4. K-means of a dataset of histogram-valued data using adaptive Wasserstein distances
res_kmeans <- WH_adaptive.kmeans(new_mat, k = k_, rep = 10, simplify = TRUE, qua = 20, standardize = TRUE)
clusters_4 <- data.table(date = dates, cluster_4 = res_kmeans$IDX)

# combine clusteres results and dates
clusters_data <- Reduce(function(x, y) merge(x, y, by = "date", all.x = TRUE, all.y = FALSE),
                        list(clusters_1, clusters_3, clusters_4))

# get daily spy data
# arr <- tiledb_array("D:/equity-usa-hour-fmpcloud-adjusted", as.data.frame = TRUE,
#                     selected_ranges = list(symbol = cbind("AAPL", "AAPL")))
# spy_daily <- arr[]
# tiledb_array_close(arr)
# spy_daily <- as.data.table(spy_daily)
# setnames(spy_daily, "time", "date")

# merge spy and clusters
clusters_data <- merge(clusters_data, spy_daily, by = "date", all.x = TRUE, all.y = FALSE)

# create returns
clusters_data[, returns := close / shift(close) - 1]
clusters_data <- na.omit(clusters_data)

# plot data
ggplot(clusters_data, aes(x = date, y = close, colour = factor(cluster_1), group = 1)) +
  geom_line()
ggplot(clusters_data[date %between% c("2020-01-01", "2021-01-01")], aes(x = date, y = close, colour = factor(cluster_1), group = 1)) +
  geom_line()

# show good distributions
n_ <- 120
g1 <- plot(new_mat[which(clusters_data$cluster_4 == 1)[n_]])
g2 <- plot(new_mat[which(clusters_data$cluster_4 == 2)[n_]])
g1 / g2
table(clusters_data$cluster_4)
plot(new_mat[which(clusters_data$cluster_4 == 1)[22:23]])
plot(new_mat[which(clusters_data$cluster_4 == 2)[22:23]])

# test for exponential dist
cluster_pvalues_exp <- list()
for (cluster_ in unique(clusters_data$cluster_4)) {
  sample_ <- clusters_data[cluster_4 == cluster_]
  x <- new_mat[which(sample_$cluster_4 == cluster_)]
  p_values <- c()
  for (i in 1:nrow(x@M)) {
    y <- ks.test(x@M[[i]]@x, "punif", fit1$estimate)
    p_values <- c(p_values, y$p.value)
  }
  cluster_pvalues_exp[[cluster_]] <- p_values
}
mean(cluster_pvalues_exp[[1]])
mean(cluster_pvalues_exp[[2]])

# estiamte distribution
cluster_dist_est <- list()
for (cluster_ in sort(unique(clusters_data$cluster_4))) {
  sample_ <- clusters_data[cluster_4 == cluster_]
  x <- new_mat[which(sample_$cluster_4 == cluster_)]
  dist_est <- list()
  for (i in 1:nrow(x@M)) {
    y <- model_select(x@M[[i]]@x)
    dist_est[[i]] <- attributes(y)
  }
  cluster_dist_est[[cluster_]] <- dist_est
}
dists_1 <- lapply(cluster_dist_est[[1]], function(x) x$model)
dists_2 <- lapply(cluster_dist_est[[2]], function(x) x$model)
table(unlist(dists_1))
table(unlist(dists_2))

# estimate power distribution
power_est <- list()
for (cluster_ in sort(unique(clusters_data$cluster_4))) {
  sample_ <- clusters_data[cluster_4 == cluster_]
  x <- new_mat[which(sample_$cluster_4 == cluster_)]
  power_est_ <- list()
  for (i in 1:nrow(x@M)) {
    y <- mlgumbel(x@M[[i]]@x)
    power_est_[[i]] <- as.vector(y)
  }
  power_est[[cluster_]] <- power_est_
}
power_est[[1]]
beat_1 <- lapply(power_est[[1]], function(x) x[2])
beat_2 <- lapply(power_est[[2]], function(x) x[2])
beat_3 <- lapply(power_est[[3]], function(x) x[2])
beat_4 <- lapply(power_est[[4]], function(x) x[2])
hist(unlist(beat_1))
hist(unlist(beat_2))
hist(unlist(beat_3))
hist(unlist(beat_4))
mean(unlist(beat_1))
mean(unlist(beat_2))
mean(unlist(beat_3))
mean(unlist(beat_4))


# backtst function
backtest <- function(returns, clusters, sell_clusters, return_cumulative = TRUE) {
  sides <- vector("integer", length(clusters))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(clusters[i-1])) {
      sides[i] <- NA
    } else if (clusters[i-1] %in% sell_clusters) {
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

# backtest
backtest(clusters_data$returns, clusters_data$cluster_4, c(2))
res <- backtest(clusters_data$returns, clusters_data$cluster_4, c(2), FALSE)
charts.PerformanceSummary(as.xts.data.table(cbind(clusters_data[, .(date, returns)], res)))
charts.PerformanceSummary(as.xts.data.table(cbind(clusters_data[4000:nrow(clusters_data), .(date, returns)], res[4000:nrow(clusters_data)])))


# SPY DATA ----------------------------------------------------------------
# minute SPY data
arr <- tiledb_array("D:/equity-usa-minute-fmpcloud-adjusted",
                    as.data.frame = TRUE,
                    selected_ranges = list(symbol = cbind("AAPL", "AAPL")))
system.time(spy <- arr[])
tiledb_array_close(arr)
attr(spy$time, "tz") <- "UTC"
spy <- as.data.table(spy)
spy[, time := with_tz(time, tzone = "America/New_York")]

# keep only trading hours
spy <- spy[format.POSIXct(time, format = "%H:%M:%S") %between% c("09:30:00", "16:30:00")]

# calculate returns
spy[, returns := close / shift(close) - 1]

# remove missing vlaues
spy <- na.omit(spy)

# prepare data for HistDAWass package
spy[, day := as.Date(time, format = "%Y-%m-%d", tz = "America/New_York")]
system.time(list_of_t_spy <- spy[ , list(list(data2hist(returns))), by = c("day")])

# create MatH object
new_mat<-MatH(x=list_of_t_spy$V1,
              nrows=nrow(list_of_t_spy),
              ncols = 1,
              rownames = unique(list_of_t_spy$day),
              varnames = "returns")
plot(new_mat[150:170], type="DENS") # to see the data

# rolling cluster predictions
estimated_clusters <- list()
for (i in 1:nrow(new_mat@M)) {
  if (i < 500) {
    estimated_clusters[[i]] <- NA_integer_
  } else {
    estimated_clusters[[i]] <- WH_kmeans(new_mat[1:i], k = 2, rep = 10, simplify = TRUE, qua = 40)
  }
}

# get predictions
dates <- lapply(estimated_clusters, function(x) {
  dates_ <- as.integer(names(x$solution$IDX))
  tail(dates_, 1)
})
dates <- as.Date(unlist(dates), origin = "1970-01-01")
last_clusters <- lapply(estimated_clusters, function(x) tail(x$solution$IDX, 1))
last_clusters <- unlist(last_clusters)
buy_cluster <- lapply(estimated_clusters, function(x) {
  if (is.null(x)) {
    return(NA)
  } else if (x$solution$centers@M[[1]]@m > x$solution$centers@M[[2]]@m) {
    return(1L)
  } else {
    return(2L)
  }
})
buy_cluster <- unlist(buy_cluster)
buy_cluster <- buy_cluster[!is.na(buy_cluster)]
predictions <- ifelse(last_clusters == buy_cluster, 1, 0)
predictions <- cbind.data.frame(date = dates, predictions)

# get daily spy data
arr <- tiledb_array("s3://equity-usa-daily-fmpcloud", as.data.frame = TRUE,
                    selected_ranges = list(symbol = cbind("SPY", "SPY")))
spy_daily <- arr[]
tiledb_array_close(arr)
spy_daily <- as.data.table(spy_daily)

# merge spy and predictions
clusters_data <- merge(spy_daily, predictions, by = "date", all.x = TRUE, all.y = FALSE)

# create returns
clusters_data[, returns := close / shift(close) - 1]
clusters_data <- na.omit(clusters_data)

# summary stats
table(clusters_data$predictions)

# plot data
ggplot(clusters_data, aes(x = date, y = close, colour = factor(predictions), group = 1)) +
  geom_line()

# backtest
backtest(clusters_data$returns, clusters_data$predictions, 0)
res <- backtest(clusters_data$returns[500:nrow(clusters_data)], clusters_data$predictions[500:nrow(clusters_data)], 0, FALSE)
charts.PerformanceSummary(as.xts.data.table(cbind(clusters_data[500:nrow(clusters_data), .(date, returns)], res)))
charts.PerformanceSummary(as.xts.data.table(cbind(clusters_data[4000:nrow(clusters_data), .(date, returns)], res[4000:nrow(clusters_data)])))





# cluster analysis
res_cluster <- WH_kmeans(new_mat, k=3)
clusters_1 <- data.table(date = unique(spy$day), cluster_1 = res_cluster$solution$IDX)
# 2. Hierarchical clustering
res_hcluster <- WH_hclust(new_mat, simplify = TRUE, method = "complete")
plot(res_hcluster) # it plots the dendrogram
clusters_2 <- cutree(res_hcluster, k = 3) # it returns the labels for 5 clusters
# 3. Fuzzy c-means of a dataset of histogram-valued data
res <- WH_fcmeans(new_mat, k = 3, m = 1.5, rep = 10, simplify = TRUE, qua = 10, standardize = TRUE)
clusters <- data.table(1:length(res$solution$IDX), clusters = res$solution$IDX)
clusters$date <- rownames(res$solution$membership)
clusters <- clusters[, .(date, clusters)]
# 4. K-means of a dataset of histogram-valued data using adaptive Wasserstein distances
res_kmeans <- WH_adaptive.kmeans(new_mat, k = 3, rep = 10, simplify = TRUE, qua = 10, standardize = TRUE)
clusters_4 <- data.table(date = unique(spy$day), cluster_4 = res_kmeans$IDX)

# combine clusteres results and dates
clusters_data <- merge(clusters_1, clusters_4, by = "date", all.x = TRUE, all.y = FALSE)

# get daily spy data
arr <- tiledb_array("s3://equity-usa-daily-fmpcloud", as.data.frame = TRUE,
             selected_ranges = list(symbol = cbind("SPY", "SPY")))
spy_daily <- arr[]
tiledb_array_close(arr)
spy_daily <- as.data.table(spy_daily)
clusters_data <- merge(clusters_data, spy_daily, by = "date", all.x = TRUE, all.y = FALSE)

# create returns
clusters_data[, returns := close / shift(close) - 1]
clusters_data <- na.omit(clusters_data)

# summary stats
table(clusters_data$cluster)

# plot data
ggplot(clusters_data, aes(x = date, y = close, colour = factor(cluster_4), group = 1)) +
  geom_line()
ggplot(clusters_data[date %between% c("2004-01-01", "2007-01-01")], aes(x = date, y = close, colour = factor(cluster_1), group = 1)) +
  geom_line()
ggplot(clusters_data[date %between% c("2007-01-01", "2008-01-01")], aes(x = date, y = close, colour = factor(cluster_1), group = 1)) +
  geom_line()
ggplot(clusters_data[date %between% c("2008-01-01", "2009-01-01")], aes(x = date, y = close, colour = factor(cluster_1), group = 1)) +
  geom_line()
ggplot(clusters_data[date %between% c("2009-01-01", "2010-01-01")], aes(x = date, y = close, colour = factor(cluster), group = 1)) +
  geom_line()
ggplot(clusters_data[date %between% c("2010-01-01", "2013-01-01")], aes(x = date, y = close, colour = factor(cluster), group = 1)) +
  geom_line()
ggplot(clusters_data[date %between% c("2013-01-01", "2016-01-01")], aes(x = date, y = close, colour = factor(cluster), group = 1)) +
  geom_line()
ggplot(clusters_data[date %between% c("2016-01-01", "2020-01-01")], aes(x = date, y = close, colour = factor(cluster), group = 1)) +
  geom_line()
ggplot(clusters_data[date %between% c("2020-01-01", "2021-01-01")], aes(x = date, y = close, colour = factor(cluster), group = 1)) +
  geom_line()
ggplot(clusters_data[date %between% c("2021-01-01", "2021-11-01")], aes(x = date, y = close, colour = factor(cluster), group = 1)) +
  geom_line()

# backtst function
backtest <- function(returns, clusters, sell_clusters, return_cumulative = TRUE) {
  sides <- vector("integer", length(clusters))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(clusters[i-1])) {
      sides[i] <- NA
    } else if (clusters[i-1] %in% sell_clusters) {
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

# backtest
backtest(clusters_data$returns, clusters_data$cluster_4, c(2))
res <- backtest(clusters_data$returns, clusters_data$cluster_4, c(2), FALSE)
charts.PerformanceSummary(as.xts.data.table(cbind(clusters_data[, .(date, returns)], res)))
charts.PerformanceSummary(as.xts.data.table(cbind(clusters_data[4000:nrow(clusters_data), .(date, returns)], res[4000:nrow(clusters_data)])))

# backtest_data <- clusters_data[, .(date, cluster, adjClose)]
# backtest_data[, returns := (adjClose / shift(adjClose)) - 1]
# backtest_data <- na.omit(backtest_data)
# backtest_data[, indicator := ifelse(cluster == 1, 1, 0)]
# results <- backtest(returns = backtest_data$returns, indicator = backtest_data$cluster, c(3), return_cumulative = FALSE)
# results_xts <- cbind(as.xts.data.table(backtest_data[, .(date, returns)]), results)
# Return.cumulative(results)
# charts.PerformanceSummary(results_xts)
# charts.PerformanceSummary(results_xts["2007-01/2010-01"])
# charts.PerformanceSummary(results_xts["2020-01/2021-01"])
# SharpeRatio(cbind(as.xts.data.table(backtest_data[, .(date, returns)]), results))



# CROSS SECTION CLUSTERING ------------------------------------------------
# import hour ohlcv
arr <- tiledb_array("D:/equity-usa-hour-fmpcloud-adjusted", as.data.frame = TRUE)
hour_data <- arr[]
tiledb_array_close(arr)
hour_data_dt <- as.data.table(hour_data)
attr(hour_data_dt$time, "tz") <- Sys.getenv("TZ")
hour_data_dt[, time := with_tz(time, "America/New_York")]

# prepare data for
hour_data_dt[, returns := close / shift(close) - 1]
hour_data_dt <- na.omit(hour_data_dt)

# keep trading hours
DT <- hour_data_dt[as.ITime(time) %between% c(as.ITime("10:00:00"), as.ITime("16:00:00"))]
DT[, N := .N, by = time]
DT <- DT[N > 250]

# prepare data for HistDAWass package
list_of_t <- DT[, list(list(data2hist(returns))), by = time]

# create MatH object
new_mat<-MatH(x=list_of_t$V1,
              nrows=nrow(list_of_t),
              ncols = 1,
              rownames = list_of_t$day,
              varnames = "returns")
plot(new_mat[100:115], type="DENS") # to see the data

# cluster analysis
# 1. K MEANS
res_kmeans <- WH_kmeans(new_mat, k = 3, rep = 10, simplify = TRUE, qua = 100, standardize = TRUE)
clusters_kmeans <- data.table(date = list_of_t$time, cluster_kmeans = res_kmeans$solution$IDX)
# 2. Hierarchical clustering
# res <- WH_hclust(new_mat, simplify = TRUE, method = "complete")
# plot(res) # it plots the dendrogram
# clusters <- cutree(res, k = 3) # it returns the labels for 5 clusters
# # 3. Fuzzy c-means of a dataset of histogram-valued data
# res <- WH_fcmeans(new_mat, k = 2, m = 1.5, rep = 10, simplify = TRUE, qua = 10, standardize = TRUE)
# clusters <- data.table(1:length(res$solution$IDX), clusters = res$solution$IDX)
# 4. K-means of a dataset of histogram-valued data using adaptive Wasserstein distances
res_akmeans <- WH_adaptive.kmeans(new_mat, k = 3, rep = 10, simplify = TRUE, qua = 100, standardize = TRUE)
clusters_akmeans <- data.table(date = list_of_t$time, cluster_akmeans = res_akmeans$IDX)

# combine clusteres results and dates
clusters_data <- merge(clusters_kmeans, clusters_akmeans,
                       by = "date", all.x = TRUE, all.y = FALSE)

# get daily spy data
arr <- tiledb_array("D:/equity-usa-hour-fmpcloud-adjusted", as.data.frame = TRUE,
                    selected_ranges = list(symbol = cbind("SPY", "SPY")))
spy_daily <- arr[]
tiledb_array_close(arr)
spy_daily <- as.data.table(spy_daily)
setnames(spy_daily, "time", "date")
attr(spy_daily$date, "tz") <- Sys.getenv("TZ")
spy_daily[, date := with_tz(date, "America/New_York")]
clusters_data <- merge(clusters_data, spy_daily, by = "date", all.x = TRUE, all.y = FALSE)

# create returns
clusters_data <- na.omit(clusters_data)
clusters_data[, returns := close / shift(close) - 1]
clusters_data <- na.omit(clusters_data)

# plot data
ggplot(clusters_data, aes(x = date, y = close, colour = factor(cluster_kmeans), group = 1)) +
  geom_line()
ggplot(clusters_data[30000:nrow(clusters_data)], aes(x = date, y = close, colour = factor(cluster_kmeans), group = 1)) +
  geom_line()


# backtst function
backtest <- function(returns, clusters, sell_clusters, return_cumulative = TRUE) {
  sides <- vector("integer", length(clusters))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(clusters[i-1])) {
      sides[i] <- NA
    } else if (clusters[i-1] %in% sell_clusters) {
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

# backtest
backtest(clusters_data$returns, clusters_data$cluster_kmeans, c(2, 3))
res <- backtest(clusters_data$returns, clusters_data$cluster_kmeans, c(2, 3), FALSE)
charts.PerformanceSummary(as.xts.data.table(cbind(clusters_data[, .(date, returns)], res)))
charts.PerformanceSummary(as.xts.data.table(cbind(clusters_data[4000:nrow(clusters_data), .(date, returns)], res[4000:nrow(clusters_data)])))





# plot clusters results
clusters_data <- as.data.table(clusters, keep.rownames = TRUE)
clusters_data$datetime <- unique(sample$day)
clusters_data <- clusters_data[, .(datetime, clusters)]
setnames(clusters_data, c("date", "cluster"))
# spy_hourly <- sample[symbol == "SPY" & datetime %in% clusters_data$datetime]
spy_daily <- as.data.table(fmpc_price_history("SPY", min(clusters_data$date), max(clusters_data$date)))
clusters_data <- merge(clusters_data, spy_daily, by = "date", all.x = TRUE, all.y = FALSE)

# summary stats
table(clusters_data$cluster)

# plot data
ggplot(clusters_data, aes(x = date, y = close, colour = factor(cluster), group = 1)) +
  geom_line()
ggplot(clusters_data[date %between% c("2004-01-01", "2007-01-01")], aes(x = date, y = close, colour = factor(cluster), group = 1)) +
  geom_line()
ggplot(clusters_data[date %between% c("2007-01-01", "2008-01-01")], aes(x = date, y = close, colour = factor(cluster), group = 1)) +
  geom_line()
ggplot(clusters_data[date %between% c("2008-01-01", "2009-01-01")], aes(x = date, y = close, colour = factor(cluster), group = 1)) +
  geom_line()
ggplot(clusters_data[date %between% c("2009-01-01", "2010-01-01")], aes(x = date, y = close, colour = factor(cluster), group = 1)) +
  geom_line()
ggplot(clusters_data[date %between% c("2010-01-01", "2013-01-01")], aes(x = date, y = close, colour = factor(cluster), group = 1)) +
  geom_line()
ggplot(clusters_data[date %between% c("2013-01-01", "2016-01-01")], aes(x = date, y = close, colour = factor(cluster), group = 1)) +
  geom_line()
ggplot(clusters_data[date %between% c("2016-01-01", "2020-01-01")], aes(x = date, y = close, colour = factor(cluster), group = 1)) +
  geom_line()
ggplot(clusters_data[date %between% c("2020-01-01", "2021-01-01")], aes(x = date, y = close, colour = factor(cluster), group = 1)) +
  geom_line()
ggplot(clusters_data[date %between% c("2021-01-01", "2021-11-01")], aes(x = date, y = close, colour = factor(cluster), group = 1)) +
  geom_line()




# SPY HOUR ROLLING ------------------------------------------------------------

# hour spy
spy_hour <- market_data[symbol == "SPY"]

# prepare data for HistDAWass package
h1 = 35
h2 = 8
l = runner(x = spy_hour$returns,
           f = function(x)tryCatch(data2hist(x), error = function(e) NA),
           k = h1,
           at = seq(1, nrow(spy_hour), h2),
           na_pad = TRUE
)
l <- l[!is.na(l)]
l_dates = runner(x = spy_hour$datetime,
                 f = function(x) tail(x, 1),
                 k = h1,
                 at = seq(1, nrow(spy_hour), h2),
                 na_pad = TRUE
)
l_dates <- l_dates[!is.na(l)]
l_dates <- as.POSIXct(unlist(l_dates), origin = "1970-01-01", tz = "EST")

# create MatH object
new_mat <- MatH(x=l,
              nrows=length(l),
              ncols = 1,
              rownames = l_dates,
              varnames = "returns")
plot(new_mat[1:15], type="DENS") # to see the data



#prediction


ID_test<-predict_WH_kmeans(res,test) #named vector with the ID of the corresponding cluster




# cluster analysis
# 1. K MEANS
res<-WH_kmeans(new_mat,k=3)
clusters <- res$solution$IDX
# # 2. Hierarchical clustering
# res <- WH_hclust(new_mat, simplify = TRUE, method = "complete")
# plot(res) # it plots the dendrogram
# clusters <- cutree(res, k = 3) # it returns the labels for 5 clusters
# # 3. Fuzzy c-means of a dataset of histogram-valued data
# res <- WH_fcmeans(new_mat, k = 2, m = 1.5, rep = 10, simplify = TRUE, qua = 10, standardize = TRUE)
# clusters <- data.table(1:length(res$solution$IDX), clusters = res$solution$IDX)
# # 4. K-means of a dataset of histogram-valued data using adaptive Wasserstein distances
# res <- WH_adaptive.kmeans(new_mat, k = 2, rep = 10, simplify = TRUE, qua = 10, standardize = TRUE)

# plot clusters results
clusters_data <- as.data.table(clusters, keep.rownames = TRUE)
setnames(clusters_data, c("datetime", "cluster"))
clusters_data$datetime <- as.POSIXct(as.numeric(clusters_data$datetime), origin = "1970-01-01", tz = "EST")
clusters_data <- merge(spy_hour, clusters_data, by = "datetime", all.x = TRUE, all.y = FALSE)

# summary stats
table(clusters_data$cluster)

# plot data
ggplot(clusters_data, aes(x = datetime, y = close, colour = factor(cluster), group = 1)) +
  geom_line()
ggplot(clusters_data[datetime %between% c("2004-01-01", "2007-01-01")], aes(x = datetime, y = close, colour = factor(cluster), group = 1)) +
  geom_line()
ggplot(clusters_data[datetime %between% c("2007-01-01", "2008-01-01")], aes(x = datetime, y = close, colour = factor(cluster), group = 1)) +
  geom_line()
ggplot(clusters_data[datetime %between% c("2008-01-01", "2009-01-01")], aes(x = datetime, y = close, colour = factor(cluster), group = 1)) +
  geom_line()
ggplot(clusters_data[datetime %between% c("2009-01-01", "2010-01-01")], aes(x = datetime, y = close, colour = factor(cluster), group = 1)) +
  geom_line()
ggplot(clusters_data[datetime %between% c("2010-01-01", "2013-01-01")], aes(x = datetime, y = close, colour = factor(cluster), group = 1)) +
  geom_line()
ggplot(clusters_data[datetime %between% c("2013-01-01", "2016-01-01")], aes(x = datetime, y = close, colour = factor(cluster), group = 1)) +
  geom_line()
ggplot(clusters_data[datetime %between% c("2016-01-01", "2020-01-01")], aes(x = datetime, y = close, colour = factor(cluster), group = 1)) +
  geom_line()
ggplot(clusters_data[datetime %between% c("2020-01-01", "2020-01-01")], aes(x = datetime, y = close, colour = factor(cluster), group = 1)) +
  geom_line()
ggplot(clusters_data[date %between% c("2021-01-01", "2021-11-01")], aes(x = datetime, y = close, colour = factor(cluster), group = 1)) +
  geom_line()

# backtst function
backtest <- function(returns, indicator, buy_cluster, return_cumulative = TRUE) {
  sides <- vector("integer", length(indicator))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(indicator[i-1])) {
      sides[i] <- NA
    } else if (indicator[i-1] %in% buy_cluster) {
      sides[i] <- 1
    } else {
      sides[i] <- 0
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

# backtest
backtest_data <- clusters_data[, .(datetime, cluster, close)]
backtest_data[, returns := (close / shift(close)) - 1]
backtest_data[, cluster := na.locf(cluster, na.rm = FALSE)]
backtest_data <- na.omit(backtest_data)
results <- backtest(returns = backtest_data$returns, indicator = backtest_data$cluster, c(1), return_cumulative = FALSE)
results_xts <- cbind(as.xts.data.table(backtest_data[, .(datetime, returns)]), results)
Return.cumulative(results)
charts.PerformanceSummary(results_xts)
charts.PerformanceSummary(results_xts["2007-01/2010-01"])
charts.PerformanceSummary(results_xts["2020-01/2021-01"])
SharpeRatio(cbind(as.xts.data.table(backtest_data[, .(date, returns)]), results))



# CROSS SECTION MINUTE CLUSTERING AGGREGATION -----------------------------

# choose symbols
symbols = c("SPY", "AAPL", "GM", "AMZN", "AAL")

# import minute data
market_data_list <- lapply(symbols, function(x) {
  print(x)
  y <- tryCatch(storage_read_csv(CONTMIN, paste0(x, ".csv")), error = function(e) NA)
  if (is.null(y) | all(is.na(y))) return(NULL)
  y <- cbind(symbol = x, y)
  return(y)
})
market_data <- rbindlist(market_data_list)

# change timezone
market_data[, datetime := as.POSIXct(as.numeric(formated), origin = as.POSIXct("1970-01-01", tz = "EST"), tz = "EST")]
market_data[, formated := NULL]
market_data[, t := NULL]

# keep only trading hours
market_data <- market_data[format.POSIXct(datetime, format = "%H:%M:%S") %between% c("09:30:00", "16:30:00")]

# adjust market data
market_data <- adjust(data = market_data, symbol_col = "symbol", ohlc_cols = c("o", "h", "c", "l"))
setnames(market_data, c("symbol", "datetime", "open", "high", "close", "low", "volume"))

# calculate returns
market_data[, returns := close / shift(close) - 1]

# remove missing vlaues
market_data <- na.omit(market_data)


# choose parameters
freq = "minute"
n_clusters <- 2
minimal_number_of_clusters = 1000

# prepare data for HistDAWass package
market_data[, day := as.Date(datetime)]
list_of_ts <- market_data[ , list(list(data2hist(returns))), by = c("symbol", "day")]

### LOOP STARTS HERE

# create MatH object
s <- unique(list_of_ts$symbol)[1]
sample_ <- list_of_ts[symbol == s]

# inner loop to calculate clusters
for (i in minimal_number_of_cluster:1005) { # nrow(sample_)
  sample_cluster <- sample_[1:i]
  new_mat<-MatH(x=sample_cluster$V1,
                nrows=nrow(sample_cluster),
                ncols = 1,
                rownames = unique(sample_cluster$day),
                varnames = "returns")

  # rolling k means
  res <- WH_kmeans(new_mat, k=n_clusters)
  kmean_clusters <- res$solution$IDX
  kmean_clusters <- as.data.table(kmean_clusters, keep.rownames = TRUE)
  kmean_clusters[, date := as.Date(as.numeric(rn))]
  kmean_clusters <- kmean_clusters[, .(date, kmean_clusters)]

}

# rolling clusters
runner(x = 1:nrow(sample_),
       f = function(x) {
         new_mat <- MatH(x=x$V1,
                         nrows=nrow(x),
                         ncols = 1,
                         rownames = unique(x$day),
                         varnames = "returns")
         as.data.table(x = new_mat)
       },
       at = minimal_number_of_clusters:1010)
new_mat<-MatH(x=sample_$V1,
              nrows=nrow(sample_),
              ncols = 1,
              rownames = unique(sample_$day),
              varnames = "returns")
# plot(new_mat[1:15], type="DENS") # to see the data

# rolling k means
runner(x = new_mat)
res <- WH_kmeans(new_mat,k=n_clusters)
kmean_clusters <- res$solution$IDX
kmean_clusters <- as.data.table(kmean_clusters, keep.rownames = TRUE)
kmean_clusters[, date := as.Date(as.numeric(rn))]
kmean_clusters <- kmean_clusters[, .(date, kmean_clusters)]

# # Hierarchical clustering
# res_hclust <- WH_hclust(new_mat, simplify = TRUE, method = "complete")
# hclust_clusters <- cutree(res_hclust, k = n_clusters)
# hclust_clusters <- as.data.table(hclust_clusters, keep.rownames = TRUE)
# hclust_clusters[, date := as.Date(as.numeric(rn))]
# hclust_clusters <- hclust_clusters[, .(date, hclust_clusters)]
#
# # Fuzzy c-means
# res_fuzzy_cmeans <- WH_fcmeans(new_mat, k = n_clusters, m = 1.5, rep = 10, simplify = TRUE, qua = 10, standardize = TRUE)
# fuzzy_cmeans_clusters <- data.table(date = as.Date(as.numeric(rownames(new_mat@M))),
#                                     clusters_fuzzy_cmeans = res_fuzzy_cmeans$solution$IDX)
#
# # K-means using adaptive Wasserstein distances
# res_kmeans_adaptive <- WH_adaptive.kmeans(new_mat, k = n_clusters, rep = 10, simplify = TRUE, qua = 10, standardize = TRUE)
# kmeans_adaptive_clusters <- data.table(date = as.Date(as.numeric(rownames(new_mat@M))),
#                                        kmeans_adaptive_clusters = res_kmeans_adaptive$IDX)

### LOOP ENDS HERE


# combine clusteres results and dates
clusters <- Reduce(function(x, y) merge(x, y, by = c("date"), all.x = TRUE, all.y = FALSE),
                   list(kmean_clusters, hclust_clusters, fuzzy_cmeans_clusters, kmeans_adaptive_clusters))
clusters <- cbind(symbol = s, clusters)

# NED LOOP



# summary stats
table(clusters_data$cluster)


BLOOD
class(BLOOD)
model.parameters <- WH.regression.two.components(data = BLOOD, Yvar = 1, Xvars = c(2:3))
Predicted.BLOOD <- WH.regression.two.components.predict(data = BLOOD[, 2:3],
                                                        parameters = model.parameters)

