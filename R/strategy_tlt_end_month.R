library(data.table)
library(tiledb)
library(findata)
library(ggplot2)



# import TLT
arr <- tiledb_array("D:/equity-usa-daily-fmp",
                    as.data.frame = TRUE,
                    query_layout = "UNORDERED",
                    selected_ranges = list(symbol = cbind("TLT", "TLT")))
DT <- arr[]
tiledb_array_close(arr)
dt <- as.data.table(DT)

# plot TLT
ggplot(dt, aes(x = date, y = adjClose)) +
  geom_line()

# calculate returns
dt[, returns := adjClose / shift(adjClose) - 1]
dt <- na.omit(dt)

# plot returns
ggplot(dt, aes(x = date, y = returns)) +
  geom_line()

# get mean of returns from every date of the month
dt[, day_of_month := lubridate::day(date)]

# summarise
tlt_ret <- dt[, .(ret_mean = mean(returns), ret_median = median(returns)), by = day_of_month]
head(tlt_ret)
ggplot(tlt_ret, aes(x = day_of_month, y = ret_mean)) +
  geom_bar(stat = "identity")
ggplot(tlt_ret, aes(x = day_of_month, y = ret_median)) +
  geom_bar(stat = "identity")
