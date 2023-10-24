library(data.table)
library(arrow)
library(highfrequency)
library(lubridate)
library(ggplot2)





# UTILS -------------------------------------------------------------------
# get critical values
get_critical_values = function(n, alpha = 0.95) {
  Cn <- sqrt(2 * log(n)) - (log(pi) + log(log(n)))/(2 * sqrt(2 * log(n)))
  Sn <- 1/sqrt(2 * log(n))
  betastar <- -log(-log(1 - alpha))
  criticalValue <- Cn + Sn * betastar
  return(criticalValue)
}

# critical values
cv_1000 = get_critical_values(1000)
cv_10000 = get_critical_values(10000)
cv_100000 = get_critical_values(100000)


# DATA IMPORT -------------------------------------------------------------
# set up
PATHPRICES = "F:/equity/usa/minute-adjusted"
URIJUMPS = "F:/equity/usa/jumps"

# files
files_jumps  = list.files(URIJUMPS, full.names = TRUE)
files_prices = list.files(PATHPRICES, full.names = TRUE)

# # import minute data
# sample_file = files[1]
# symbol_ = gsub(".*/|\\.parquet", "", sample_file)
# ohlcv = read_parquet(sample_file)
# data = ohlcv[, .(DT = date, PRICE = close)]
#
# # SPY
# spy_file =files[grep("SPY", files)]
# spy = read_parquet(spy_file)
# spy = spy[, .(DT = date, PRICE = close)]
# spy[, DT := with_tz(DT, "America/New_York")]
#
# # change timezone
# data[, DT := with_tz(DT, "America/New_York")]
# spy[, DT := with_tz(DT, "America/New_York")]



# JUMPS -------------------------------------------------------------------
# extract jumps for every symbol
jumps_l = lapply(files_jumps[1:20], function(f) {
  # debug
  # f = files_jumps[1]

  # symbol
  print(f)
  jumps_ = read_parquet(f)
  symbol_ = gsub(".*/|\\.parquet", "", f)

  # sample
  jumps_sample = jumps_[abs(ztest) > cv_1000]
  jumps_sample = cbind(symbol = symbol_, jumps_sample)

  return(jumps_sample)
})
jumps = rbindlist(jumps_l)

# add dummy jumps
jumps[ztest < -cv_100000, jump_cv_100000_neg := 1]
jumps[ztest < -cv_10000,  jump_cv_10000_neg  := 1]
jumps[ztest < -cv_1000,   jump_cv_1000_neg   := 1]

# aggregate jumps
jumps_aggregate = jumps[, .(jump_cv_100000_neg = sum(jump_cv_100000_neg, na.rm = TRUE)), by = date]
jumps_aggregate[, jump_cv_100000_neg_roll := frollsum(jump_cv_100000_neg, 390 * 5, na.rm = TRUE)]
setorder(jumps_aggregate, date)
jumps_aggregate = na.omit(jumps_aggregate)

# visualize aggregate
ggplot(jumps_aggregate, aes(x = date, y = jump_cv_100000_neg_roll)) +
  geom_line()
  # geom_point(data = jumps_dt[jump == TRUE], aes(x = DT, y = PRICE), color = "red", size = 2)
ggplot(jumps_dt[1800000:1886346], aes(x = DT, y = PRICE)) +
  geom_line() +
  geom_point(data = jumps_dt[1800000:1886346][jump == TRUE], aes(x = DT, y = PRICE), color = "red", size = 2)
ggplot(jumps_dt[1700000:1800000], aes(x = DT, y = PRICE)) +
  geom_line() +
  geom_point(data = jumps_dt[1700000:1800000][jump == TRUE], aes(x = DT, y = PRICE), color = "red", size = 2)
ggplot(jumps_dt[1600000:1700000], aes(x = DT, y = PRICE)) +
  geom_line() +
  geom_point(data = jumps_dt[1600000:1700000][jump == TRUE], aes(x = DT, y = PRICE), color = "red", size = 2)
ggplot(jumps_dt[1500000:1600000], aes(x = DT, y = PRICE)) +
  geom_line() +
  geom_point(data = jumps_dt[1500000:1600000][jump == TRUE], aes(x = DT, y = PRICE), color = "red", size = 2)

# visualize individual
sample_file = files_jumps[1]
symbol_ = gsub(".*/|\\.parquet", "", sample_file)
ohlcv = read_parquet(paste0(PATHPRICES, "/", symbol_, ".parquet"))
ohlcv[, date := with_tz(date, "America/New_York")]
ohlcv = jumps[symbol == symbol_][ohlcv[, .(date, close_strategy = close)], on = c("date")]
ggplot(ohlcv, aes(x = date, y = close_strategy)) +
  geom_line() +
  geom_point(data = ohlcv[jump_cv_100000_neg == 1], aes(x = date, y = close), color = "red", size = 2)
ggplot(ohlcv[1500000:1600000], aes(x = date, y = close_strategy)) +
  geom_line() +
  geom_point(data = ohlcv[1500000:1600000][jump_cv_100000_neg == 1], aes(x = date, y = close), color = "red", size = 2)
ggplot(ohlcv[1600000:1700000], aes(x = date, y = close_strategy)) +
  geom_line() +
  geom_point(data = ohlcv[1600000:1700000][jump_cv_100000_neg == 1], aes(x = date, y = close), color = "red", size = 2)
ggplot(ohlcv[1700000:1800000], aes(x = date, y = close_strategy)) +
  geom_line() +
  geom_point(data = ohlcv[1700000:1800000][jump_cv_100000_neg == 1], aes(x = date, y = close), color = "red", size = 2)


# sample_jump_date = jumps_dt[jump == TRUE][sample(1:.N, 1), DT]
# sample_data = jumps_dt[DT %between% c(sample_jump_date - 1000, sample_jump_date + 1000)]
# ggplot(sample_data, aes(x = DT, y = PRICE)) +
#   geom_line() +
#   geom_point(data = sample_data[jump == TRUE], aes(x = DT, y = PRICE), color = "red", size = 2)
