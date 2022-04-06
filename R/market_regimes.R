library(HistDAWass)
library(data.table)
library(AzureStor)
library(ggplot2)
library(QuantTools)
library(future.apply)
library(fmpcloudr)
library(PerformanceAnalytics)
library(runner)
library(leanr)



# get data from azure
ENDPOINT = storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), key=Sys.getenv("BLOB-KEY"))
CONT = storage_container(ENDPOINT, "equity-usa-hour-fmpcloud-adjusted")
CONTMIN = storage_container(ENDPOINT, "equity-usa-minute-fmpcloud")
fmpcloudr::fmpc_set_token(Sys.getenv("APIKEY-FMPCLOUD"))



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



# SPY DATA ----------------------------------------------------------------

# minute SPY data
spy <- storage_read_csv(CONTMIN, "AAPL.csv")
spy <- as.data.table(spy)

# change timezone
spy[, datetime := as.POSIXct(as.numeric(formated), origin = as.POSIXct("1970-01-01", tz = "EST"), tz = "EST")]
spy[, formated := NULL]
spy[, t := NULL]

# keep only trading hours
spy <- spy[format.POSIXct(datetime, format = "%H:%M:%S") %between% c("09:30:00", "16:30:00")]

# calculate returns
spy[, returns := c / shift(c) - 1]

# remove missing vlaues
spy <- na.omit(spy)

# prepare data for HistDAWass package
spy[, day := format(datetime, format = "%Y-%m-%d")]
list_of_t_spy <- spy[ , list(list(data2hist(returns))), by = c("day")]

# create MatH object
new_mat<-MatH(x=list_of_t_spy$V1,
              nrows=nrow(list_of_t_spy),
              ncols = 1,
              rownames = unique(list_of_t_spy$day),
              varnames = "returns")
# plot(new_mat, type="DENS") # to see the data

# cluster analysis
res<-WH_kmeans(new_mat,k=3)
clusters <- res$solution$IDX
# 2. Hierarchical clustering
res <- WH_hclust(new_mat, simplify = TRUE, method = "complete")
plot(res) # it plots the dendrogram
clusters <- cutree(res, k = 3) # it returns the labels for 5 clusters
# 3. Fuzzy c-means of a dataset of histogram-valued data
res <- WH_fcmeans(new_mat, k = 3, m = 1.5, rep = 10, simplify = TRUE, qua = 10, standardize = TRUE)
clusters <- data.table(1:length(res$solution$IDX), clusters = res$solution$IDX)
clusters$date <- rownames(res$solution$membership)
clusters <- clusters[, .(date, clusters)]
# 4. K-means of a dataset of histogram-valued data using adaptive Wasserstein distances
res <- WH_adaptive.kmeans(new_mat, k = 2, rep = 10, simplify = TRUE, qua = 10, standardize = TRUE)
clusters <- data.table(unique(spy$day), cluster = res$IDX)

# combine clusteres results and dates
clusters_data <- as.data.table(clusters, keep.rownames = TRUE)
setnames(clusters_data, c("date", "cluster"))
clusters_data[, date := as.Date(date)]
spy_daily <- as.data.table(fmpc_price_history("AAPL", min(clusters_data$date), max(clusters_data$date)))
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

# backtest
backtest_data <- clusters_data[, .(date, cluster, adjClose)]
backtest_data[, returns := (adjClose / shift(adjClose)) - 1]
backtest_data <- na.omit(backtest_data)
backtest_data[, indicator := ifelse(cluster == 1, 1, 0)]
results <- backtest(returns = backtest_data$returns, indicator = backtest_data$cluster, c(3), return_cumulative = FALSE)
results_xts <- cbind(as.xts.data.table(backtest_data[, .(date, returns)]), results)
Return.cumulative(results)
charts.PerformanceSummary(results_xts)
charts.PerformanceSummary(results_xts["2007-01/2010-01"])
charts.PerformanceSummary(results_xts["2020-01/2021-01"])
SharpeRatio(cbind(as.xts.data.table(backtest_data[, .(date, returns)]), results))



# CROSS SECTION CLUSTERING ------------------------------------------------


# import all data from Azure storage
azure_blobs <- list_blobs(CONT)
# symbols <- toupper(gsub("\\.csv", "", azure_blobs$name))
# files_from_symbols <- paste0(tolower(symbols), ".csv")
market_data_list <- lapply(azure_blobs$name, function(x) {
  print(x)
  y <- tryCatch(storage_read_csv2(CONT, x), error = function(e) NA)
  if (is.null(y) | all(is.na(y))) return(NULL)
  y <- cbind(symbol = x, y)
  return(y)
})
market_data <- rbindlist(market_data_list)
market_data[, symbol := toupper(gsub("\\.csv", "", symbol))]
market_data[, returns := close / shift(close) - 1]
market_data <- na.omit(market_data)
market_data$datetime <- as.POSIXct(as.numeric(market_data$datetime),
                                   origin=as.POSIXct("1970-01-01", tz="EST"),
                                   tz="EST")

# prepare data for HistDAWass package
sample <- market_data[datetime %between% c("2019-10-01", "2020-04-01")]
sample <- sample[, day := as.Date(format(datetime))]
list_of_t_cross_sectoin <- lapply(unique(sample$day), function(x) {
  sample_ <- sample[day == x]
  data2hist(sample_$returns)
})
# list_of_t <- sample[, list(list(data2hist(returns))), by = day]

# create MatH object
new_mat<-MatH(x=list_of_t,
              nrows=length(list_of_t),
              ncols = 1,
              rownames = unique(sample$day),
              varnames = "returns")
plot(new_mat[1:15], type="DENS") # to see the data

# cluster analysis
# 1. K MEANS
res<-WH_kmeans(new_mat,k=3)
clusters <- res$solution$IDX
# 2. Hierarchical clustering
res <- WH_hclust(new_mat, simplify = TRUE, method = "complete")
plot(res) # it plots the dendrogram
clusters <- cutree(res, k = 3) # it returns the labels for 5 clusters
# 3. Fuzzy c-means of a dataset of histogram-valued data
res <- WH_fcmeans(new_mat, k = 2, m = 1.5, rep = 10, simplify = TRUE, qua = 10, standardize = TRUE)
clusters <- data.table(1:length(res$solution$IDX), clusters = res$solution$IDX)
# 4. K-means of a dataset of histogram-valued data using adaptive Wasserstein distances
res <- WH_adaptive.kmeans(new_mat, k = 2, rep = 10, simplify = TRUE, qua = 10, standardize = TRUE)

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
new_mat<-MatH(x=l,
              nrows=length(l),
              ncols = 1,
              rownames = l_dates,
              varnames = "returns")
plot(new_mat[1:15], type="DENS") # to see the data

# cluster analysis
# 1. K MEANS
res<-WH_kmeans(new_mat,k=2)
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
ggplot(clusters_data[datetime %between% c("2020-01-01", "2020-01-01")], aes(x = datetime, y = close, colour = factor(cluster), group = 1)) +
  geom_line()
ggplot(clusters_data[date %between% c("2021-01-01", "2021-11-01")], aes(x = date, y = close, colour = factor(cluster), group = 1)) +
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


# HELP FILE> LATER TO equityData package
adjust <- function(data = market_data, symbol_col = "symbol", ohlc_cols = c("o", "h", "c", "l")) {

  # globals for the function
  symbols <- unique(data[, get(symbol_col)])
  blob_factor_files <- paste0(tolower(symbols), ".csv")

  # import factor files
  cont <- storage_container(ENDPOINT, "factor-files")
  factor_files <- lapply(blob_factor_files, function(x) {
    print(x)
    y <- storage_read_csv(cont, x, col_names = FALSE)
    if (nrow(y) > 0) {
      y$symbol <- toupper(gsub("\\.csv", "", x))
    }
    y
  })
  factor_files <- rbindlist(factor_files)
  setnames(factor_files, colnames(factor_files), c("date", "price_factor", "split_factor", "previous_price", "symbol"))
  factor_files[, symbol := toupper(symbol)]
  factor_files[, date := as.Date(as.character(date), "%Y%m%d")]

  # get only data from IPO
  ipo_dates <- vapply(symbols, function(x) {
    y <- get_ipo_date(x, api_key = Sys.getenv("APIKEY-FMPCLOUD"))
    if (is.null(y)) {
      return("2004-01-02")
    } else {
      return(y)
    }
  }, character(1))
  ipo_dates_dt <- as.data.table(ipo_dates, keep.rownames = TRUE)
  setnames(ipo_dates_dt, "rn", "symbol")
  ipo_dates_dt[, ipo_dates := as.Date(ipo_dates)]
  ipo_dates_dt[, symbol := toupper(gsub("\\.csv", "", symbol))]

  # keep only data after IPO
  market_data_new <- ipo_dates_dt[data, on = "symbol"]
  market_data_new <- market_data_new[datetime >= ipo_dates]
  market_data_new_daily <- market_data_new[, .SD[.N], by = .(symbol, date = as.Date(datetime))]
  market_data_new_daily$ipo_dates <- NULL

  # adjust
  df <- market_data_new[symbol %in% unique(factor_files$symbol)]
  df[, date:= as.Date(datetime)]
  df <- merge(df, factor_files, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE)
  df[, `:=`(split_factor = na.locf(split_factor, na.rm = FALSE, rev = TRUE),
            price_factor = na.locf(price_factor, na.rm = FALSE, rev = TRUE)), by = symbol]
  df[, `:=`(split_factor = ifelse(is.na(split_factor), 1, split_factor),
            price_factor = ifelse(is.na(price_factor), 1, price_factor))]
  df[, (ohlc_cols) := lapply(.SD, function(x) {x * price_factor * split_factor}), .SDcols = ohlc_cols]
  keep_cols <- c(symbol_col, "datetime", ohlc_cols, "v")
  df <- df[, ..keep_cols]
  setorder(df, datetime)
  df <- unique(df)
  setorder(df, symbol, datetime)

  return(df)
}




results <- WH.1d.PCA(data = BLOOD, var = 1, listaxes = c(1:2))
results$WASSVARIANCE
results$INERTIA
results$PCAout

results <- WH.1d.PCA(data = new_mat, var = 1, listaxes = c(1:2))
results$INERTIA
results$PCAout$eig
results$PCAout$ind
results$PCAout$svd
results$PCAout$quanti.sup

