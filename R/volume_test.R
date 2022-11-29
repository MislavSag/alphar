library(data.table)
library(tiledb)
library(roll)
library(ggplot2)


# UTILS -------------------------------------------------------------------
# date segments
GFC <- c("2007-01-01", "2010-01-01")
COVID <- c("2020-01-01", "2021-06-01")
AFTER_COVID <- c("2021-06-01", "2022-01-01")
NEW <- c("2022-01-01", as.character(Sys.Date()))



# import minute data
arr <- tiledb_array("D:/equity-usa-minute-fmpcloud-adjusted",
                    as.data.frame = TRUE)
dt <- arr["AAPL"]
dt <- as.data.table(dt)
attr(dt$time, "tz") <- "UTC"
rtsplot(as.xts.data.table(dt[seq(1, nrow(dt), by = 10), .(time, close)]))

# keep trading hours
dt[, time_ := with_tz(time, "America/New_York")]
dt[, time_ := as.ITime(time_)]
dt <- dt[time_ %between% c(as.ITime("09:30:00"), as.ITime("16:00:00"))]
dt[, time_ := NULL]

# change timezone to NY
dt[, time := with_tz(time, "America/New_York")]

# keep unique values (should be redundand)
dt <- unique(dt, by = c("symbol", "time"))

# calculate returns
setorder(dt, symbol, time)
dt[, returns := close / shift(close, 1L) - 1, by = "symbol"]

# remove missing values
dt <- na.omit(dt)

# create weekly volume dt
weekly_volume <- dt[, .(weekly_volume = sum(volume, na.rm = TRUE)),
                    by = .(symbol,
                           year = data.table::year(time),
                           week = data.table::week(time))]
weekly_volume[, weekly_volume := shift(weekly_volume)]

# filter bars with high volume
dt[, `:=`(week = data.table::week(time), year = data.table::year(time))]
dt <- merge(dt, weekly_volume, by = c("symbol", "week", "year"),
            all.x = TRUE, all.y = FALSE)
dt <- na.omit(dt)
dt[volume / weekly_volume  > 0.01]
dt[volume / weekly_volume  > 0.02 & as.ITime(time) %between% c(as.ITime("10:30:00"), as.ITime("15:00:00"))]
dt[volume / weekly_volume  > 0.01 & returns < -0.01]
dt[volume / weekly_volume  > 0.01 & abs(returns) < 0.001]
# dt[, volume_high := (volume / weekly_volume  > 0.02)]
# dt[, volume_high := (volume / weekly_volume  > 0.01) & as.ITime(time) %between% c(as.ITime("10:30:00"), as.ITime("15:00:00"))]
# dt[, volume_high := (volume / weekly_volume  > 0.01) & returns < -0.01]
dt[, volume_high := (volume / weekly_volume  > 0.01) & abs(returns) < 0.001]

# visualize volumes
ggplot(dt, aes(time)) +
  geom_line(aes(y = close)) +
  geom_point(data = dt[volume_high == TRUE], aes(y = close), color = "red")
ggplot(dt[time %between% COVID], aes(time)) +
  geom_line(aes(y = close)) +
  geom_point(data = dt[volume_high == TRUE & time %between% COVID], aes(y = close), color = "red")
ggplot(dt[time %between% AFTER_COVID], aes(time)) +
  geom_line(aes(y = close)) +
  geom_point(data = dt[volume_high == TRUE & time %between% AFTER_COVID], aes(y = close), color = "red")
ggplot(dt[time %between% NEW], aes(time)) +
  geom_line(aes(y = close)) +
  geom_point(data = dt[volume_high == TRUE & time %between% NEW], aes(y = close), color = "red")
date_ = as.Date("2017-08-03")
date_span = c(date_, date_ + 8)
ggplot(dt[time %between% date_span], aes(time)) +
  geom_line(aes(y = close)) +
  geom_point(data = dt[volume_high == TRUE & time %between% date_span], aes(y = close), color = "red")

# train model
backtest_data <- copy(dt)
backtest_data[, return_nex_minutes := shift(close, 5, type = "lead") / close - 1]
backtest_data[, return_nex_minutes_bin := ifelse(return_nex_minutes > 0, 1, 0)]
backtest_data[volume_high == TRUE]
backtest_data[volume_high == TRUE, PerformanceAnalytics::Return.cumulative(return_nex_minutes)]
table(backtest_data[volume_high == TRUE, return_nex_minutes_bin])
