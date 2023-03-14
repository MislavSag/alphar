# Title:  Title
# Author: Name
# Description: Description

# packages
library(tiledb)
library(data.table)
library(checkmate)
library(ggplot2)
library(fredr)
library(findata)
library(TSstudio)
library(gausscov)
library(vars)
library(runner)



# SET UP ------------------------------------------------------------------
# check if we have all necessary env variables
assert_choice("AWS-ACCESS-KEY", names(Sys.getenv()))
assert_choice("AWS-SECRET-KEY", names(Sys.getenv()))
assert_choice("AWS-REGION", names(Sys.getenv()))
assert_choice("BLOB-ENDPOINT", names(Sys.getenv()))
assert_choice("BLOB-KEY", names(Sys.getenv()))
assert_choice("APIKEY-FMPCLOUD", names(Sys.getenv()))
assert_choice("FRED-KEY", names(Sys.getenv()))

# set credentials
config <- tiledb_config()
config["vfs.s3.aws_access_key_id"] <- Sys.getenv("AWS-ACCESS-KEY")
config["vfs.s3.aws_secret_access_key"] <- Sys.getenv("AWS-SECRET-KEY")
config["vfs.s3.region"] <- Sys.getenv("AWS-REGION")
context_with_config <- tiledb_ctx(config)
fredr_set_key(Sys.getenv("FRED-KEY"))

# init fmp
fmp = FMP$new()



# FRED --------------------------------------------------------------------
# fred help function
get_fred <- function(id = "VIXCLS", name = "vix", calculate_returns = FALSE) {
  x <- fredr_series_observations(
    series_id = id,
    observation_start = as.Date("1950-01-01"),
    observation_end = Sys.Date()
  )
  x <- as.data.table(x)
  x <- x[, .(date, value)]
  x <- unique(x)
  setnames(x, c("date", name))

  # calcualte returns
  if (calculate_returns) {
    x <- na.omit(x)
    x[, paste0(name, "_ret_month") := get(name) / shift(get(name), 22) - 1]
    x[, paste0(name, "_ret_year") := get(name) / shift(get(name), 252) - 1]
    x[, paste0(name, "_ret_week") := get(name) / shift(get(name), 5) - 1]
  }
  x
}

# fred help function for vintage days
get_fred_vintage <- function(id = "TB3MS", name = "tbl") {
  start_dates <- seq.Date(as.Date("1950-01-01"), Sys.Date(), by = 365)
  end_dates <- c(start_dates[-1], Sys.Date())
  map_fun <- function(start_date, end_date) {
    x <- tryCatch({
      fredr_series_observations(
        series_id = id,
        observation_start = start_date,
        observation_end = end_date,
        realtime_start = start_date,
        realtime_end = end_date
      )
    }, error = function(e) NULL)
    x
  }
  x_l <- mapply(map_fun, start_dates, end_dates, SIMPLIFY = FALSE)
  x <- rbindlist(x_l)
  x <- unique(x)
  x <- x[, .(realtime_start, value)]
  setnames(x, c("date", name))
  x
}

# bond yield
yield = get_fred_vintage(id = "DGS10", name = "yield")
yield = get_fred(id = "DGS10", name = "yield")
sp500 = get_fred(id = "SP500", name = "sp500")
cpi = get_fred_vintage(id = "CPIAUCSL", name = "cpi")
cpi[, inflation := cpi / shift(cpi, 12) - 1]



# MARKET DATA -------------------------------------------------------------
# import market data (choose frequency)
symbols = c("TLT", "TIP", "SPY")
arr <- tiledb_array("D:/equity-usa-daily-fmp",
                    as.data.frame = TRUE,
                    query_layout = "UNORDERED",
                    selected_ranges = list(symbol = cbind(symbols, symbols))
)
system.time(prices <- arr[])
tiledb_array_close(arr)
prices <- as.data.table(prices)

# filter dates and symbols
prices_dt <- unique(prices, by = c("symbol", "date"))
setorder(prices_dt, symbol, date)

# keep only dates after TLT min
prices_dt[symbol == "TLT"]
prices_dt[symbol == "TIP"]
prices_dt = prices_dt[date > prices_dt[symbol == "TIP", min(date)]]

# clean daily prices
prices_dt <- prices_dt[open > 0 & high > 0 & low > 0 & close > 0 & adjClose > 0] # remove rows with zero and negative prices
setorder(prices_dt, "symbol", "date")
prices_dt[, returns := adjClose   / data.table::shift(adjClose) - 1, by = symbol] # calculate returns
prices_dt <- prices_dt[returns < 1] # TODO:: better outlier detection mechanism. For now, remove daily returns above 100%
adjust_cols <- c("open", "high", "low")
prices_dt[, (adjust_cols) := lapply(.SD, function(x) x * (adjClose / close)), .SDcols = adjust_cols] # adjust open, high and low prices
prices_dt[, close := adjClose]
prices_dt <- na.omit(prices_dt[, .(symbol, date, open, high, low, close, volume, returns)])
prices_n <- prices_dt[, .N, by = symbol]
prices_n <- prices_n[N > 700]  # remove prices with only 700 or less observations
prices_dt <- prices_dt[symbol %in% prices_n$symbol]

# weekly prices
prices_dt_week = copy(prices_dt)
prices_dt_week[, ymw := paste0(data.table::year(date),
                               data.table::month(date),
                               data.table::week(date))]
prices_dt_week <- prices_dt_week[, .(date = tail(date, 1), close = tail(close, 1)), by = .(symbol, ymw)]
prices_dt_week[, returns := close   / data.table::shift(close) - 1, by = symbol]


# PREPARE -----------------------------------------------------------------
# merge
plot_dt_merged = dcast(prices_dt[, .(symbol, date, close = close / 10)], date ~ symbol, value.var = "close")
plot_dt_merged = plot_dt_merged[yield, on = "date"]
plot_dt_merged = cpi[, .(date, inflation)][plot_dt_merged, on = "date"]
plot_dt_merged[, inflation := nafill(inflation, "locf")]
plot_dt_merged = na.omit(plot_dt_merged)

# merge week
plot_dt_merged_week = dcast(prices_dt_week[, .(symbol, date, returns = returns)], date ~ symbol, value.var = "returns")
plot_dt_merged_week = plot_dt_merged_week[yield, on = "date"]
plot_dt_merged_week = na.omit(plot_dt_merged_week)
cols_ = c("SPY", "TIP", "TLT")
plot_dt_merged_week[, (cols_) := lapply(.SD, function(x) nafill(x, "locf")), .SDcols = cols_]
plot_dt_merged_week = cpi[, .(date, inflation)][plot_dt_merged_week, on = "date"]
plot_dt_merged_week[, inflation := nafill(inflation, "locf")]
plot_dt_merged_week = na.omit(plot_dt_merged_week)



# PREDICTOR IMPORTANCE ----------------------------------------------------
# # prepare data for gausscov
# dt = copy(plot_dt_merged_week)
# # cols = c("SPY", "TIP", "TLT")
# # dt[, (cols) := lapply(.SD, log), .SDcols = cols]
#
# # create lags
# n_lags = 24
# cols = colnames(dt)[2:length(dt)]
# dt[, (paste0("lag_",rep(cols, each = n_lags), "_", rep(1:n_lags, times = length(cols)))) :=
#      unlist(lapply(.SD, function(x) shift(x, 1:n_lags, type = "lag")), recursive = FALSE), .SDcols = cols]
# dt = na.omit(dt)
#
# # y and X
# y = as.matrix(dt[, "TLT"])
# cols_ = colnames(dt)[grep("lag", colnames(dt))]
# X = as.matrix(dt[, .SD, .SDcols = cols_])
#
# # f1st
# f1st_fi_ <- f1st(y, X, kmn = 10)
# predictors_f1st <- colnames(X)[f1st_fi_[[1]][, 1]]
#
# # f3st_1
# f3st_1_ <- f3st(y, X, kmn = 10, m = 1)
# predictors_f3st_1 <- unique(as.integer(f3st_1_[[1]][1, ]))[-1]
# predictors_f3st_1 <- predictors_f3st_1[predictors_f3st_1 != 0]
# predictors_f3st_1 <- colnames(X)[predictors_f3st_1]



# LM WITH IMPORTANT VARS --------------------------------------------------
# prepare dataset
dt = copy(plot_dt_merged_week)
n_lags = 24
cols = colnames(dt)[2:length(dt)]
dt[, (paste0("lag_",rep(cols, each = n_lags), "_", rep(1:n_lags, times = length(cols)))) :=
     unlist(lapply(.SD, function(x) shift(x, 1:n_lags, type = "lag")), recursive = FALSE), .SDcols = cols]
dt = na.omit(dt)

# y and X
df = as.data.frame(dt[, -c(2:4, 6)])
y_index = which(colnames(df) == "TLT")

# parameters
window_lengths <- c(200)

# VAR
# cl <- makeCluster(4L)
# clusterExport(cl, "X", envir = environment())
# clusterEvalQ(cl,{library(vars); library(BigVAR); library(tsDyn)})
roll_preds <- lapply(window_lengths, function(win) {
  runner(
    x = df,
    f = function(x) {

      # important predictors
      # x =df[1:200, ]
      print(head(x[, 1], 1))
      print(tail(x[, 1], 1))
      # x = as.data.frame(setDT(df)[date %between% c(as.Date("2009-01-27"), as.Date("2009-04-07")), ])
      f3st_1_ <- f3st(as.matrix(x[, y_index]), as.matrix(x[, -c(1, y_index)]), kmn = 1, m = 1)
      predictors_f3st_1 <- unique(as.integer(f3st_1_[[1]][1, ]))[-1]
      predictors_f3st_1 <- predictors_f3st_1[predictors_f3st_1 != 0]
      predictors_f3st_1 <- colnames(x[, -y_index])[predictors_f3st_1]
      print(predictors_f3st_1)

      # lm
      f = as.formula(paste0("TLT ~ ", paste0(predictors_f3st_1, collapse = " + ")))
      res = lm(f, data = as.data.frame(x))

      return(res)
    },
    k = win,
    lag = 0L,
    na_pad = TRUE
    # cl = cl
  )
})
# parallel::stopCluster(cl)

# extract predictions for all windows
roll_preds[[1]][[1000]]
summary(roll_preds[[1]][[400]])$coefficients[, 4]



# ANALYSIS ----------------------------------------------------------------
# visualize prices
plot_dt = melt(prices_dt[, .(symbol, date, close)], id.vars = "date", variable.name = "symbol", measure.var = "close")
ggplot(prices_dt, aes(x = date, y = close, color = symbol)) +
  geom_line() +
  scale_x_date(date_breaks = "1 year", date_labels = "%Y") +
  geom_vline(xintercept = as.numeric(as.Date("2022-01-01")))

# correlation
summary_dt = dcast(prices_dt[, .(symbol, date, close)], date ~ symbol, value.var = "close")
cor(summary_dt[, c(2:3)])
rolling_corr = roll::roll_cor(summary_dt[, TIP], summary_dt[, TLT], 22)
plot(xts::xts(rolling_corr, order.by = summary_dt[, date]), type = "l")

# vis prices and yields
plot_dt_merged_long = melt(plot_dt_merged, id.vars = "date")
ggplot(plot_dt_merged_long, aes(x = date, y = value, color = variable)) +
  geom_line() +
  scale_x_date(date_breaks = "1 year", date_labels = "%Y") +
  geom_vline(xintercept = as.numeric(as.Date("2022-01-01")))

# spread
reg = lm(log(TLT) ~ lag(log(TIP)) + lag(log(SPY)) + lag(inflation), data = plot_dt_merged)
spread = reg$residuals
spread_mean = mean(spread, na.rm = TRUE)
spread_sd = sd(spread, na.rm = TRUE)
spread_z = (spread - spread_mean) / spread_sd
spread_z = as.data.table(cbind.data.frame(as.Date(plot_dt_merged$date), spread_z))

plot(as.xts.data.table(spread_z))
xts::addEventLines(xts::xts(3, as.Date("2022-01-01")), col = 'blue')

# CPI
plot(as.xts.data.table(cpi[, .(date, inflation)]))
