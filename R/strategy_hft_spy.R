library(data.table)
library(highfrequency)
library(arrow)
library(lubridate)


# symbol
s = "spy"

# import tick trades
tick_files = list.files("F:/equity/usa/tick/trades/finam/spy", full.names = TRUE)
ticks = lapply(tick_files, read_parquet)
ticks = rbindlist(ticks)
sampleTDataRaw
ticks[, `:=`(
  EX     = "P",
  SYMBOL = "SPY",
  COND   = "FTI",
  CORR   = 0
)]
setnames(ticks, c("DT", "PRICE", "SIZE", "EX", "SYMBOL", "COND", "CORR"))
cols = colnames(sampleTDataRaw)
ticks = ticks[, ..cols]
ticks[, DT := with_tz(DT, tzone = "America/New_York")]
ticks[, DT := force_tz(DT, tzone = "UTC")] # highfrequency package works better if timezone is (wrongly) set to UTC

# clean tick trades data
ticks = noZeroPrices(ticks)
ticks[as.ITime(DT) > as.ITime("16:00:00")]
ticks[as.ITime(DT) < as.ITime("09:30:00")]
ticks = exchangeHoursOnly(ticks)
ticks = mergeTradesSameTimestamp(ticks, selection = "median")
# 130.968.422
# 47.409.784

# import quotes data

