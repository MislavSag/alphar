library(data.table)
library(timechange)
library(roll)
library(tiledb)
library(lubridate)
library(rtsplot)
library(TTR)
library(patchwork)
library(ggplot2)
library(AzureStor)
library(PerformanceAnalytics)



# UTILS -------------------------------------------------------------------
# date segments
GFC <- c("2007-01-01", "2010-01-01")
COVID <- c("2020-01-01", "2021-06-01")
AFTER_COVID <- c("2021-06-01", "2022-01-01")
NEW <- c("2022-01-01", as.character(Sys.Date()))


# IMPORT DATA -------------------------------------------------------------
# import market data
arr <- tiledb_array("D:/equity-usa-hour-fmpcloud-adjusted", as.data.frame = TRUE)
system.time(hour_data <- arr[])
tiledb_array_close(arr)
hour_data_dt <- as.data.table(hour_data)
hour_data_dt[, time := as.POSIXct(time, tz = "UTC")]

# keep only trading hours
hour_data_dt <- hour_data_dt[as.integer(time_clock_at_tz(time,
                                                         tz = "America/New_York",
                                                         units = "hours")) %in% 10:16]

# clean data
hour_data_dt[, returns := close / shift(close) - 1, by = "symbol"]
hour_data_dt <- unique(hour_data_dt, by = c("symbol", "time"))
hour_data_dt <- na.omit(hour_data_dt)

# spy data
spy <- hour_data_dt[symbol == "SPY", .(time, close, returns)]
spy <- unique(spy, by =  "time")

# visualize
rtsplot(as.xts.data.table(spy[, .(time, close)]))
rtsplot(as.xts.data.table(hour_data_dt[symbol == "AAPL", .(time, close)]))




# MINMAX INDICATORS -------------------------------------------------------
# calculate rolling quantiles
market_data <- copy(hour_data_dt)
market_data[, p_999_4year := roll::roll_quantile(volume, 255*8*4, p = 0.999), by = .(symbol)]
market_data[, p_001_4year := roll::roll_quantile(volume, 255*8*4, p = 0.001), by = .(symbol)]
market_data[, p_999_2year := roll::roll_quantile(volume, 255*8*2, p = 0.999), by = .(symbol)]
market_data[, p_001_2year := roll::roll_quantile(volume, 255*8*2, p = 0.001), by = .(symbol)]
market_data[, p_999_year := roll::roll_quantile(volume, 255*8, p = 0.999), by = .(symbol)]
market_data[, p_001_year := roll::roll_quantile(volume, 255*8, p = 0.001), by = .(symbol)]
market_data[, p_999_halfyear := roll::roll_quantile(volume, 255*4, p = 0.999), by = .(symbol)]
market_data[, p_001_halfyear := roll::roll_quantile(volume, 255*4, p = 0.001), by = .(symbol)]

market_data[, p_99_4year := roll::roll_quantile(volume, 255*8*4, p = 0.99), by = .(symbol)]
market_data[, p_01_4year := roll::roll_quantile(volume, 255*8*4, p = 0.01), by = .(symbol)]
market_data[, p_99_2year := roll::roll_quantile(volume, 255*8*2, p = 0.99), by = .(symbol)]
market_data[, p_01_2year := roll::roll_quantile(volume, 255*8*2, p = 0.01), by = .(symbol)]
market_data[, p_99_year := roll::roll_quantile(volume, 255*8, p = 0.99), by = .(symbol)]
market_data[, p_01_year := roll::roll_quantile(volume, 255*8, p = 0.01), by = .(symbol)]
market_data[, p_99_halfyear := roll::roll_quantile(volume, 255*4, p = 0.99), by = .(symbol)]
market_data[, p_01_halfyear := roll::roll_quantile(volume, 255*4, p = 0.01), by = .(symbol)]

market_data[, p_97_4year := roll::roll_quantile(volume, 255*8*4, p = 0.97), by = .(symbol)]
market_data[, p_03_4year := roll::roll_quantile(volume, 255*8*4, p = 0.03), by = .(symbol)]
market_data[, p_97_2year := roll::roll_quantile(volume, 255*8*2, p = 0.97), by = .(symbol)]
market_data[, p_03_2year := roll::roll_quantile(volume, 255*8*2, p = 0.03), by = .(symbol)]
market_data[, p_97_year := roll::roll_quantile(volume, 255*8, p = 0.97), by = .(symbol)]
market_data[, p_03_year := roll::roll_quantile(volume, 255*8, p = 0.03), by = .(symbol)]
market_data[, p_97_halfyear := roll::roll_quantile(volume, 255*4, p = 0.97), by = .(symbol)]
market_data[, p_03_halfyear := roll::roll_quantile(volume, 255*4, p = 0.03), by = .(symbol)]

market_data[, p_95_4year := roll::roll_quantile(volume, 255*8*4, p = 0.95), by = .(symbol)]
market_data[, p_05_4year := roll::roll_quantile(volume, 255*8*4, p = 0.05), by = .(symbol)]
market_data[, p_95_2year := roll::roll_quantile(volume, 255*8*2, p = 0.95), by = .(symbol)]
market_data[, p_05_2year := roll::roll_quantile(volume, 255*8*2, p = 0.05), by = .(symbol)]
market_data[, p_95_year := roll::roll_quantile(volume, 255*8, p = 0.95), by = .(symbol)]
market_data[, p_05_year := roll::roll_quantile(volume, 255*8, p = 0.05), by = .(symbol)]
market_data[, p_95_halfyear := roll::roll_quantile(volume, 255*4, p = 0.95), by = .(symbol)]
market_data[, p_05_halfyear := roll::roll_quantile(volume, 255*4, p = 0.05), by = .(symbol)]

# save market data
# file_name <- paste0("D:/risks/minmax/minmaxvol_data_",
#                     format(Sys.Date(), format = "%Y%m%d"),".csv")
# fwrite(market_data, file_name)

# # read old data
# list.files("D:/risks/minmax")
# market_data <- fread("D:/risks/minmax/minmax_data_20221024.csv")

# exrtreme returns
cols <- colnames(market_data)[grep("^p_9", colnames(market_data))]
cols_new <- paste0("above_", cols)
market_data[, (cols_new) := lapply(.SD, function(x) ifelse(volume > shift(x), volume - shift(x), 0)),
            by = .(symbol), .SDcols = cols]
cols <- colnames(market_data)[grep("^p_0", colnames(market_data))]
cols_new <- paste0("below_", cols)
market_data[, (cols_new) := lapply(.SD, function(x) ifelse(volume < shift(x), abs(volume - shift(x)), 0)),
            by = .(symbol), .SDcols = cols]

# crate dummy variables
cols <- colnames(market_data)[grep("^p_9", colnames(market_data))]
cols_new <- paste0("above_dummy_", cols)
market_data[, (cols_new) := lapply(.SD, function(x) ifelse(volume > shift(x), 1, 0)), by = .(symbol), .SDcols = cols]
cols <- colnames(market_data)[grep("^p_0", colnames(market_data))]
cols_new <- paste0("below_dummy_", cols)
market_data[, (cols_new) := lapply(.SD, function(x) ifelse(volume < shift(x), 1, 0)), by = .(symbol), .SDcols = cols]

# get tail risk mesures with sum
cols <- colnames(market_data)[grep("below_p|above_p|dummy", colnames(market_data))]
indicators <- market_data[, lapply(.SD, function(x) sum(x, na.rm = TRUE)), by = .(time), .SDcols = cols]
colnames(indicators) <- c("time", paste0("sum_", cols))
setorder(indicators, time)
above_sum_cols <- colnames(indicators)[grep("above", colnames(indicators))]
below_sum_cols <- colnames(indicators)[grep("below", colnames(indicators))]
excess_sum_cols <- gsub("above", "excess", above_sum_cols)
indicators[, (excess_sum_cols) := indicators[, ..above_sum_cols] - indicators[, ..below_sum_cols]]

# get tail risk mesures with sd
cols <- colnames(market_data)[grep("below_p|above_p|dummy", colnames(market_data))]
indicators_sd <- market_data[, lapply(.SD, function(x) sd(x, na.rm = TRUE)), by = .(time), .SDcols = cols]
colnames(indicators_sd) <- c("time", paste0("sd_", cols))
setorder(indicators_sd, time)
above_sum_cols <- colnames(indicators_sd)[grep("above", colnames(indicators_sd))]
below_sum_cols <- colnames(indicators_sd)[grep("below", colnames(indicators_sd))]
excess_sum_cols <- gsub("above", "excess", above_sum_cols)
indicators_sd[, (excess_sum_cols) := indicators_sd[, ..above_sum_cols] - indicators_sd[, ..below_sum_cols]]

# merge indicators and spy
indicators <- merge(indicators, indicators_sd, by = c("time"), all.x = TRUE, all.y = FALSE)
indicators <- merge(indicators, spy, by = "time")



# VISUALIZATION -----------------------------------------------------------
#
ggplot(indicators, aes(x = time)) +
  geom_line(aes(y = sum_above_p_999_2year))
ggplot(indicators, aes(x = time)) +
  geom_line(aes(y = sum_below_p_001_2year))

# visualize for sample predictors
sample_ = copy(indicators)
indicators[, .(time, returns, sum_above_p_999_year, sum_below_p_001_year)]
sample_[, buy := ifelse(shift(sum_above_p_999_year, 1, type = "lag") > 10000000, 1, 0)]
sample_[, sell := ifelse(shift(sum_below_p_001_year, 1, type = "lag") > 1000000, 1, 0)]
table(sample_$buy)
v_buy <- sample_[buy == 1, time, ]
v_sell <- sample_[sell == 1, time, ]
ggplot(sample_, aes(x = time)) +
  geom_line(aes(y = close)) +
  # geom_vline(xintercept = v_buy, color = "green") +
  geom_vline(xintercept = v_sell, color = "red")
ggplot(sample_[date %between% GFC], aes(x = date)) +
  geom_line(aes(y = close)) +
  geom_vline(xintercept = v_buy, color = "green") +
  geom_vline(xintercept = v_sell, color = "red")
ggplot(sample_[date %between% COVID], aes(x = date)) +
  geom_line(aes(y = close)) +
  geom_vline(xintercept = v_buy, color = "green") +
  geom_vline(xintercept = v_sell, color = "red")
ggplot(sample_[date %between% AFTER_COVID], aes(x = date)) +
  geom_line(aes(y = close)) +
  geom_vline(xintercept = v_buy, color = "green") +
  geom_vline(xintercept = v_sell, color = "red")
ggplot(sample_[date %between% NEW], aes(x = date)) +
  geom_line(aes(y = close)) +
  geom_vline(xintercept = v_buy, color = "green") +
  geom_vline(xintercept = v_sell, color = "red")

# visualize dummy
# dummmy variables
ggplot(indicators, aes(x = time)) +
  geom_line(aes(y = sum_excess_dummy_p_99_year))
ggplot(indicators[as.Date(time) %between% COVID], aes(x = time)) +
  geom_line(aes(y = sum_excess_dummy_p_99_year))
ggplot(indicators[as.Date(time) %between% c("2020-02-01", "2020-03-01")], aes(x = time)) +
  geom_line(aes(y = sum_excess_dummy_p_99_year))
ggplot(indicators[as.Date(time) %between% c("2020-02-01", "2020-03-01")], aes(x = time)) +
  geom_line(aes(y = SMA(sum_excess_dummy_p_99_year, 10)))
ggplot(indicators, aes(x = time)) +
  geom_line(aes(y = sum_below_p_001_2year))


sample_ = copy(indicators)
indicators[, .(time, returns, sum_above_dummy_p_99_2year, sum_below_dummy_p)]
sample_[, buy := ifelse(shift(sum_above_p_999_year, 1, type = "lag") > 0.4, 1, 0)]
sample_[, sell := ifelse(shift(sum_below_p_001_year, 1, type = "lag") > 0.4, 1, 0)]
table(sample_$buy)
v_buy <- sample_[buy == 1, time, ]
v_sell <- sample_[sell == 1, time, ]
ggplot(sample_, aes(x = time)) +
  geom_line(aes(y = close)) +
  geom_vline(xintercept = v_buy, color = "green") +
  geom_vline(xintercept = v_sell, color = "red")
ggplot(sample_[date %between% GFC], aes(x = date)) +
  geom_line(aes(y = close)) +
  geom_vline(xintercept = v_buy, color = "green") +
  geom_vline(xintercept = v_sell, color = "red")





#
#
# # HIGH VOLUME AND HIGH PRICE INCREASE -------------------------------------
# # calculate rolling quantiles
# hour_data_dt[, p_99_halfyear := roll::roll_quantile(returns, 7 * 22 * 3, p = 0.99), by = .(symbol)]
# hour_data_dt[, p_99_halfyear_vol := roll::roll_quantile(volume, 7 * 22 * 3, p = 0.99), by = .(symbol)]
#
# # exrtreme returns and lovumes dummy
# hour_data_dt[returns > shift(p_99_halfyear), p_99_halfyear_dummy := 1]
# hour_data_dt[volume > shift(p_99_halfyear_vol), p_99_halfyear_vol_dummy := 1]
# hour_data_dt[p_99_halfyear_dummy == 1]
# hour_data_dt[p_99_halfyear_vol_dummy == 1]
# hour_data_dt[p_99_halfyear_dummy == 1 & p_99_halfyear_vol_dummy == 1]
#
# # visualize points
# symbol_ <- "MSFT"
# sample_ <- hour_data_dt[symbol == symbol_ & p_99_halfyear_dummy == 1 & p_99_halfyear_vol_dummy == 1]
# ggplot(hour_data_dt[symbol == symbol_], aes(x = time)) +
#   geom_line(aes(y = close)) +
#   geom_point(data = sample_, aes(y = close), color = "red")
# ggplot(hour_data_dt[symbol == symbol_ & time %between% COVID], aes(x = time)) +
#   geom_line(aes(y = close)) +
#   geom_point(data = sample_[time %between% COVID], aes(y = close), color = "red")
# ggplot(hour_data_dt[symbol == symbol_ & time %between% NEW], aes(x = time)) +
#   geom_line(aes(y = close)) +
#   geom_point(data = sample_[time %between% NEW], aes(y = close), color = "red")
#
# # visualize exact points
# sample_ <- hour_data_dt[p_99_halfyear_dummy == 1 & p_99_halfyear_vol_dummy == 1]
# data_sample <- sample_[sample(1:nrow(sample_), 4)]
# gs <- list()
# for (i in 1:4) {
#   time_ <- data_sample[i, time]
#   exact_point <- hour_data_dt[symbol == data_sample[i, symbol] & time %between% c(time_, time_ + 30 * 60 * 60)]
#   gs[[i]] <- ggplot(exact_point, aes(x = time, y = close)) + geom_line()
# }
# (gs[[1]] / gs[[2]]) | (gs[[3]] / gs[[4]])
#
# # test if next n hours are up / down
# hour_data_dt[, ret_1 := shift(close, -1L, type = "shift") / close - 1]
# hour_data_dt[ret_1 >= 0, ret_1_dummy := 1]
# hour_data_dt[ret_1 < 0, ret_1_dummy := 0]
# hour_data_dt[p_99_halfyear_dummy == 1 & p_99_halfyear_vol_dummy == 1]
