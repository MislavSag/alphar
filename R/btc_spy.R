library(data.table)
library(httr)
library(AzureStor)
library(lubridate)


# SET UP ------------------------------------------------------------------

# get data from azure
ENDPOINT = storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), key=Sys.getenv("BLOB-KEY"))
CONT = storage_container(ENDPOINT, "equity-usa-hour-fmpcloud-adjusted")
CONTMIN = storage_container(ENDPOINT, "equity-usa-minute-fmpcloud")
fmpcloudr::fmpc_set_token(Sys.getenv("APIKEY-FMPCLOUD"))

# get crypt data
get_crypto <- function(tick = "btcusd",
                       interval = "1d",
                       from = as.numeric(Sys.time() - as.difftime(5, unit="days")) * 1000,
                       to = as.numeric(Sys.time() - as.difftime(1, unit="days")) * 1000) {
  url <- "https://financialmodelingprep.com/api/v4/historical-price-crypto"
  url_full <- paste(url, tick, interval, round(from, 0), round(to, 0), sep = "/")
  p <- RETRY("GET", url_full, query = list(apikey = Sys.getenv("APIKEY-FMPCLOUD"), limit = 1000))
  d <- content(p)
  dt <- rbindlist(d)
  dt[, ot := as.POSIXct(ot / 1000, origin = "1970-01-01")]
  dt[, ct := as.POSIXct(ct / 1000, origin = "1970-01-01") + 1]
  return(dt)
}

start_dates <- as.POSIXct(paste0(seq.Date(as.Date("2019-01-01"), Sys.Date(), by = 50), " 00:00:00"), tz = "EST")
end_dates <- start_dates + as.difftime(5, unit="days")
start_dates <- as.numeric(start_dates) * 1000
end_dates <- as.numeric(end_dates) * 1000

btc <- lapply(seq_along(start_dates), function(x) {
  get_crypto(from = start_dates[x], to = end_dates[x])
})
btc_dt <- rbindlist(btc)
btc_ts <- btc_dt[, .(ct, c)]
btc_ts <- unique(btc_ts)
setnames(btc_ts, c("datetime", "close_btc"))
attr(btc_ts$datetime, "tzone") <- "UTC"

# get spy data
# spy <- storage_read_csv2(CONT, "spy.csv")
# spy <- as.data.table(spy[, c("datetime", "close")])
spy <- fmpcloudr::fmpc_price_history("SPY", startDate = as.Date("2019-01-01"), endDate = Sys.Date())
spy <- as.data.table(spy)[, .(date, adjClose)]

# check timezones
attributes(btc_ts$datetime)
attributes(spy$datetime)

# merge
# X <- spy[btc_ts, on = "datetime"]
# spy$datetime <- floor_date(spy$datetime)
# btc_ts$datetime <- floor_date(btc_ts$datetime)
# X <- merge(spy, btc_ts, by = "datetime", all.x = TRUE, all.y = FALSE)
# X <- X[, 2:3]
# X <- apply(X, 2, diff)
# X <- scale(X)
# nrow(X)
# head(X)
btc_ts[, date := as.Date(datetime)]
X <- merge(spy, btc_ts, by = "date", all.x = TRUE, all.y = FALSE)
X <- na.omit(X)
X <- X[, c(2, 4)]
X <- apply(X, 2, diff)
X <- scale(X)

# estimte VAR
library(vars)
VARselect(X, lag.max = 20)
mod <- VAR(X, p = 1)

VARselect(X, lag.max = 14)
mod <- VAR(X, p = 14)
mod

