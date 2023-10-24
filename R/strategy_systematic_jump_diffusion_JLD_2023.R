library(data.table)
library(arrow)
library(lubridate)
library(xts)
library(nanotime)



# SETUP -------------------------------------------------------------------
# uris
URIPRICES = "C:/Users/Mislav/SynologyDrive/equity/usa/minute"

# files
files = list.files(URIPRICES, full.names = TRUE)


# IMPORT DATA -------------------------------------------------------------
# loop that import minute data, upsample to 5 min and return returns
lapply(files, function(f) {
  # debug
  # f = files[1]

  # import minute data
  dt = read_parquet(f)

  # change timezone
  # dt[, date := with_tz(date, tzone = "America/New_York")]

  # keep relevant columns
  # dt = dt[, .(date, close)]

  dt[, date_nano := as.nanotime(date)]
  dt5 = dt[, .(
    open = head(open, 1),
    high = max(high, na.rm = TRUE),
    low = min(low, na.rm = TRUE),
    close = tail(close, 1),
    volume = sum(volume, na.rm = TRUE)
  ),
  by = .(time = nano_ceiling(date_nano, as.nanoduration("00:05:00")))]
  dt5[, time := as.POSIXct(time, tz = "UTC")]

  dt
  dt5

  # upsample
  # dt[, minute5 := cut(date, breaks = "5 min")]
  xts::to.minutes5(as.xts.data.table(dt))



})
