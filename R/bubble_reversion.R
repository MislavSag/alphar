library(tiledb)
library(data.table)
library(nanotime)
library(rtsplot)
library(ggplot2)
library(patchwork)
library(PerformanceAnalytics)
library(lubridate)
library(TTR)
library(timechange)
library(AzureStor)
library(runner)
library(onlineBcp)
library(rvest)
library(Rcpp)
library(findata)
library(parallel)



# configure s3
config <- tiledb_config()
config["vfs.s3.aws_access_key_id"] <- Sys.getenv("AWS-ACCESS-KEY")
config["vfs.s3.aws_secret_access_key"] <- Sys.getenv("AWS-SECRET-KEY")
config["vfs.s3.region"] <- Sys.getenv("AWS-REGION")
context_with_config <- tiledb_ctx(config)

# params
exuber_window = 600

# import hour ohlcv
arr <- tiledb_array("D:/equity-usa-hour-fmpcloud-adjusted",
                    as.data.frame = TRUE,
                    query_layout = "UNORDERED")
hour_data <- arr[]
tiledb_array_close(arr)
hour_data_dt <- as.data.table(hour_data)
attr(hour_data_dt$time, "tz") <- Sys.getenv("TZ")

# import exuber data
arr <- tiledb_array("s3://equity-usa-hour-features-exuber",
                    as.data.frame = TRUE,
                    query_layout = "UNORDERED")
system.time(exuber <- arr[])
tiledb_array_close(arr)
exuber_dt <- as.data.table(exuber)
attr(exuber_dt$date, "tz") <- Sys.getenv("TZ")
exuber_dt = unique(exuber_dt, by = c("symbol", "date"))
setnames(exuber_dt, "date", "time")

# merge market data and exuber data
exuber_dt <- merge(hour_data_dt, exuber_dt, by = c("symbol", "time"),
                   all.x = TRUE, all.y = FALSE)

# remove new observations
exuber_dt = exuber_dt[time < as.Date("2023-01-01")]

# choose parameter set
cols_by_parameter <- colnames(exuber_dt)[grep(exuber_window, colnames(exuber_dt))]
cols <- c("symbol", "time", "close", cols_by_parameter)
exuber_dt <- exuber_dt[, ..cols]
colnames(exuber_dt) <- gsub(paste0("exuber_", exuber_window, "_\\d+_"), "", colnames(exuber_dt))

# create new variable radf_sum and select variables we need for our analysis
exuber_dt[, radf_sum := adf_log + sadf_log + gsadf_log + badf_log + bsadf_log]

# visulize collapses
df <- na.omit(exuber_dt)
df[, bubble := radf_sum > 5 & radf_sum < 20]
df[, .N, by = bubble]
symbol_ = "TSLA"
df_ <- df[symbol == symbol_]
ggplot(df_, aes(x = time)) +
  geom_line(aes(y = close)) +
  geom_point(data = df_[bubble == TRUE], aes(y = close), color = "red")
df_ <- df[symbol == symbol_][1:10000]
ggplot(df_, aes(x = time)) +
  geom_line(aes(y = close)) +
  geom_point(data = df_[bubble == TRUE], aes(y = close), color = "red")
df_ <- df[symbol == symbol_][10000:15000]
ggplot(df_, aes(x = time)) +
  geom_line(aes(y = close)) +
  geom_point(data = df_[bubble == TRUE], aes(y = close), color = "red")
df_ <- df[symbol == symbol_][15000:20000]
ggplot(df_, aes(x = time)) +
  geom_line(aes(y = close)) +
  geom_point(data = df_[bubble == TRUE], aes(y = close), color = "red")
df_ <- df[symbol == symbol_][20000:25000]
ggplot(df_, aes(x = time)) +
  geom_line(aes(y = close)) +
  geom_point(data = df_[bubble == TRUE], aes(y = close), color = "red")
df_ <- df[symbol == symbol_][25000:nrow(df_)]
ggplot(df_, aes(x = time)) +
  geom_line(aes(y = close)) +
  geom_point(data = df_[bubble == TRUE], aes(y = close), color = "red")

# generate signals for buy
df <- na.omit(exuber_dt)
df[, returns := close / shift(close, 1L) - 1, by = symbol]
df[, exuberance := radf_sum > 5 & radf_sum < 50]

# inspect what we got
symbol_ = "DXCM"
df_ <- df[symbol == symbol_]
close_ = df_[exuberance == TRUE, .(time, close)]
ggplot(df_, aes(x = time)) +
  geom_line(aes(y = close)) +
  geom_point(data = close_, aes(y = close), color = "red")

# identify collapse
df[, momentum := close / shift(close, 7 * 22 * 2) - 1, by = symbol]
df[exuberance == TRUE & momentum < 0, collapse := TRUE]

# inspect collapses
symbol_ = "DXCM"
df_ <- df[symbol == symbol_]
close_ = df_[collapse == TRUE, .(time, close)]
ggplot(df_, aes(x = time)) +
  geom_line(aes(y = close)) +
  geom_point(data = close_, aes(y = close), color = "red")

# if thereis collapse on rolling window
df[, collapse_roll := frollapply(collapse, 7 * 30, function(x) any(x == TRUE)), by = symbol]

# inspect collapse roll
symbol_ = "AAL"
df_ <- df[symbol == symbol_]
close_ = df_[collapse_roll == TRUE, .(time, close)]
ggplot(df_, aes(x = time)) +
  geom_line(aes(y = close)) +
  geom_point(data = close_, aes(y = close), color = "red")

# timestamo when collapse end it is slowing (exuber < 4)
df[, collapse_slow := NULL]
df[(collapse_roll == TRUE) & (radf_sum <= 3), collapse_slow := TRUE]

# inspect collapse slow
symbol_ = "AAL"
df_ <- df[symbol == symbol_]
close_ = df_[collapse_roll == TRUE, .(time, close)]
close_slow = df_[collapse_slow == TRUE, .(time, close)]
ggplot(df_, aes(x = time)) +
  geom_line(aes(y = close)) +
  geom_point(data = close_, aes(y = close), color = "red") +
  geom_point(data = close_slow, aes(y = close), color = "blue")

# get first blue point, that is first time the radf sum become lower than x (e.g. 3)
df[collapse_slow == TRUE]
df[, collapse_slow_roll := frollapply(collapse_slow, 7 * 5, function(x) all(is.na(x))), by = symbol]
df[collapse_slow_roll == TRUE]
df[collapse_slow_roll == TRUE & shift(collapse_slow, -1) == TRUE, collapse_slow_first := TRUE]

# inspect
symbol_ = "ANR"
df_ <- df[symbol == symbol_]
close_ = df_[collapse_roll == TRUE, .(time, close)]
close_slow_first = df_[collapse_slow_first == TRUE, .(time, close)]
ggplot(df_, aes(x = time)) +
  geom_line(aes(y = close)) +
  geom_point(data = close_, aes(y = close), color = "red", size = 0.8) +
  geom_point(data = close_slow_first, aes(y = close), color = "blue", size = 2)
dates_ = c("2014-05-01", "2015-03-01")
ggplot(df_[time %between% dates_], aes(x = time)) +
  geom_line(aes(y = close)) +
  geom_point(data = close_[time %between% dates_], aes(y = close), color = "red", size = 0.8) +
  geom_point(data = close_slow_first[time %between% dates_], aes(y = close), color = "blue", size = 2)



# OLD WAY
# df[, collapse_slow_first := frollapply(collapse_slow, 7 * 5, function(x) {
#   x_ <- x[!is.na(x)]
#   if (length(x_) > 0) {
#     return(tail(x[!is.na(x)], 1))
#   } else {
#     return(NA)
#   }
# }), by = symbol]


# prepare data for QC
qc_data <- df[collapse_slow_first == TRUE, .(time, symbol, collapse_slow_first)]
qc_data[, time := with_tz(time, tzone = "America/New_York")]
# qc_data = qc_data[as.IDate(time) %between% c(as.IDate("2021-01-01"), as.IDate("2022-12-01"))]
qc_data = unique(qc_data, by = c("time"))
qc_data = melt(qc_data, id.vars = "time", measure.vars = "symbol")
setorder(qc_data, "time")
qc_data[, time := as.character(time)]
qc_data = qc_data[, c(1, 3)]
qc_data[, dummy := 1]

# save for QC
bl_endp_key <- storage_endpoint(Sys.getenv("BLOB-ENDPOINT-SNP"), Sys.getenv("BLOB-KEY-SNP"))
cont <- storage_container(bl_endp_key, "qc-backtest")
time_ <- format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
file_name <- paste0("bubble_rev_", time_, ".csv")
storage_write_csv(qc_data, cont, file_name, col_names = FALSE)
storage_write_csv(data.frame(symbol = unique(qc_data$value)),
                  cont, "symbols.csv", col_names = FALSE)
print(file_name)



# BIG DATA TO QC ----------------------------------------------------------
# prepare data
qc_data <- na.omit(exuber_dt)
symbols_keep = unique(qc_data$symbol)[1:10]
qc_data = qc_data[symbol %in% symbols_keep]
qc_data = qc_data[, .(symbol, time, radf_sum)]
qc_data = dcast(qc_data, time ~ symbol, value.var = "radf_sum")
qc_data[, time := with_tz(time, tzone = "America/New_York")]
setorder(qc_data, "time")
qc_data[, time := as.character(time)]

# save for QC
bl_endp_key <- storage_endpoint(Sys.getenv("BLOB-ENDPOINT-SNP"), Sys.getenv("BLOB-KEY-SNP"))
cont <- storage_container(bl_endp_key, "qc-backtest")
time_ <- format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
file_name <- paste0("bubble_rev_", time_, ".csv")
storage_write_csv(qc_data, cont, file_name, col_names = FALSE)
storage_write_csv(data.frame(symbol = unique(qc_data$value)),
                  cont, "symbols.csv", col_names = FALSE)
print(file_name)
