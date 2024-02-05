library(data.table)
library(PMwR)
library(duckdb)
library(lubridate)
library(NMOF)
library(duckdb)
library(xts)



# UTILS -------------------------------------------------------------------
# trade details
trade_details = function(bt.results, prices)
  data.frame(price = prices,
             suggest = bt.results$suggested.position,
             position = unname(bt.results$position),
             wealth = bt.results$wealth,
             cash = bt.results$cash)




# IMPORT DATA -------------------------------------------------------------
# import hour data
dt = fread("F:/lean_root/data/all_stocks_hour.csv")
col = c("time", "open", "high", "low", "close", "volume", "close_adj", "symbol")
setnames(dt, col)

# set time zone
attributes(dt$time)
if (!("tzone" %in% names(attributes(dt$time)))) {
  setattr(dt$time, 'tzone', "America/New_York")
} else if (attributes(dt$time)[["tzone"]] != "America/New_York") {
  dt[, time := force_tz(time, "America/New_York")]
}
attributes(dt$time)

# unique
dt <- unique(dt, by = c("symbol", "time"))

# adjust all columns
unadjustd_cols = c("open", "high", "low")
dt[, (unadjustd_cols) := lapply(.SD, function(x) (close_adj / close) * x), .SDcols = unadjustd_cols]

# remove NA values
dt = na.omit(dt)

# keep only adjusted columns and raw close column
cols = c("open", "high", "low", "close")
setnames(dt, c("close", "close_adj"), c("close_raw", "close"))

# order
setkey(dt, "symbol")

# need at least half year observations
dt_n = dt[, .N, by = symbol]
symbols_keep = dt_n[N > 255*7, symbol]
dt = dt[symbols_keep]

# remove prices that are lower than 1$ through all period
dt_penny_all = dt[, all(close_raw < 1), by = symbol]
dt = dt[dt_penny_all[V1 == FALSE, symbol]]

# free memory
gc()

# checl if column names are in symbols
symbols_ = dt[, unique(symbol)]
"time" %in% symbols_
"open" %in% symbols_
"low" %in% symbols_


# BACKTEST WITH BTEST -----------------------------------------------------
# preapre data
btest_prepare_series = function(series = "close") {
  cols_ = c("symbol", "time", series)
  P = dt[, ..cols_]
  P = dcast(P, time ~ symbol, value.var = series)
  cols_ = colnames(P)[2:ncol(P)]
  P[, (cols_) := lapply(.SD, function(x) {
    # x = P[, 2]
    if (!is.na(tail(x, 1))) {
      x = nafill(x, type = "locf")
      return(x)
    }
    na_grp = rleid(is.na(x))
    last_na = max(na_grp, na.rm = TRUE)
    x_ = nafill(x[na_grp != last_na], type = "locf")
    x_ = c(x_, rep(NA, length(x) - length(x_)))
    return(x_)
  }), .SDcols = cols_]
  P[, time := with_tz(time, "UTC")]
  P = as.xts.data.table(P)
  P = P["2000-01-01/"]
  P = P[, !colSums(is.na(P)) == nrow(P)]
  return(P)
}
P = btest_prepare_series()
all(sapply(P, is.double))
timestamp <- index(P)
instrument <- colnames(P)
volume = btest_prepare_series("volume")
all(sapply(volume, is.double))
close_raw = btest_prepare_series("close_raw")
all(sapply(close_raw, is.double))

# define when stocks are active
first_non_na_rows <- apply(P, 2, function(col) {
  first_non_na_index <- which(!is.na(col))
  if (length(first_non_na_index) > 0) {
    return(first_non_na_index[1])
  } else {
    return(NA)
  }
})
last_block_start_indices <- apply(is.na(P), 2, function(x) {
  rev(which(!x))[1]
})
last_block_start_indices[!is.na(tail(P, 1))] = nrow(P)
active <- data.frame(instrument = colnames(P),
                     start = timestamp[first_non_na_rows],
                     end = timestamp[last_block_start_indices])
active[instrument == "hrmn", ]
active$end = floor_time(active$end, unit = "month") %m-% months(1) - days(1)

# crate all envs again
P = P[paste0("/", max(active$end), " 00:00:00")]
timestamp <- index(P)
instrument <- colnames(P)
volume = volume[paste0("/", max(active$end), " 00:00:00")]
close_raw = close_raw[paste0("/", max(active$end), " 00:00:00")]

# create signal function
mom = function(P, k) {
  o <- order(P[nrow(P), ]/ P[1, ], decreasing = TRUE)
  w <- numeric(ncol(P))
  w[o[1:k]] <- 1/k
  w
}
signal <- function(active, volume, close_raw, fun, k, nvol) {
  timestamp <- Timestamp()
  print(timestamp)

  # Keep only active stocks
  active_stocks <- active[timestamp >= active$start & timestamp <= active$end, "instrument"]

  # Filter stocks with close_raw >= 1
  valid_stocks <- which(close_raw[timestamp, active_stocks, drop = FALSE] >= 1, arr.ind = TRUE)
  active_stocks <- active_stocks[valid_stocks]

  # Calculate average volume for active stocks
  avg_volume <- colMeans(volume[timestamp, active_stocks, drop = FALSE], na.rm = TRUE)

  # Select top N most liquid stocks
  top_liquid_stocks <- names(sort(avg_volume, decreasing = TRUE))[1:nvol]

  # Get price data for top liquid stocks
  Pj <- Close(n = 250*7)[, top_liquid_stocks, drop = FALSE]

  # Apply momentum strategy
  wj <- fun(Pj, k)

  # Create a complete weight vector
  w <- numeric(length = ncol(volume))
  names(w) <- colnames(volume)
  w[top_liquid_stocks] <- wj

  return(w)
}

bt.mom <- btest(prices = list(coredata(P)),
                signal = signal,
                do.signal = "lastofmonth",
                convert.weights = TRUE,
                initial.cash = 1000,
                active = active,
                nvol = 1000,
                k = 30,
                fun = mom,
                b = 250*7,
                timestamp = timestamp,
                instrument = instrument,
                volume = volume,
                close_raw = close_raw)
gc()
bt.mom
summary(as.NAVseries(bt.mom), na.rm = TRUE)
jrn = as.data.frame(journal(bt.mom))
tail(jrn, 10)

# Open open prices
# High high prices
# Low low prices
# Close close prices
# Wealth the total wealth (value of positions plus cash) at a given point in time
# Cash cash (in accounting currency)
# Time current time (an integer)
# Timestamp the timestamp when that is specified (i.e. when the argument timestamp is supplied);
# if not, it defaults to Time
# Portfolio the current portfolio
# SuggestedPortfolio the currently suggested portfolio
# Globals an environment
# All these objects, with the

# Close() == Close(lag = 1) -> current close
# Close(Time():1) -> to get all prices
# Close(n = Time()) -> to get last n data points
# Close(n = 10) -> get last 10 prices



# ARCHIVE -----------------------------------------------------------------
# # import distinct symbols from hour data
# conn <- dbConnect(duckdb::duckdb(), ":memory:")
# query <- paste("SELECT DISTINCT Symbol FROM read_csv_auto('F:/lean_root/data/all_stocks_hour.csv')")
# distinct_symbols <- dbGetQuery(conn, query)
# dbDisconnect(conn)
#
# # take sample of symbols for developing
# symbols = sample(distinct_symbols[[1]], 15000)
#
# # import daily data
# conn <- dbConnect(duckdb::duckdb(), ":memory:")
# symbols_string = paste(symbols, collapse = "', '")
# symbols_string = paste0("'", symbols_string, "'")
# query <- sprintf("
# SELECT *
# FROM read_csv_auto('F:/lean_root/data/all_stocks_hour.csv')
# WHERE Symbol IN (%s)
# ", symbols_string)
# dt = dbGetQuery(conn, query)
# dbDisconnect(conn)
