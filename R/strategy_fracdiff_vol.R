library(fastverse)
library(finutils)
library(fracdiff)
library(roll)
library(ggplot2)
library(PerformanceAnalytics)
library(AzureStor)


# Prices data
prices = qc_daily(
  file_path = "F:/lean/data/stocks_daily.csv",
  min_obs = 252,
  duplicates = "fast",
  add_dv_rank = FALSE,
  add_day_of_month = FALSE,
  profiles_fmp = TRUE,
  fmp_api_key = "6c390b6279b8a67cc7f63cbecbd69430"
)

# Parameters
d = 0.8

# Calculate fracdiff returns and volatility at the end of the month
prices[, closed := diffseries(close, d = d), by = symbol]
prices[, i := 1:.N, by = symbol]
prices[i == 1, closed := NA]
prices[, vol_252 := roll_sd(closed, 252), by = symbol]
prices[, vol_125 := roll_sd(closed, 252), by = symbol]
prices[, vol_66 := roll_sd(closed, 252), by = symbol]
prices[, vol_22 := roll_sd(closed, 252), by = symbol]
prices[, let(i = NULL)]

# Downsample to monthly data
prices[, month := data.table::yearmon(date)]
pricesm = prices[, .(
  date = max(date),
  close = data.table::last(close),
  close_raw = data.table::last(close_raw),
  vol_252 = data.table::last(vol_252),
  vol_125 = data.table::last(vol_125),
  vol_66 = data.table::last(vol_66),
  vol_22 = data.table::last(vol_22)
), by = .(symbol, month)]

# Crate target variable
pricesm[, target := shift(close, 1, type = "lead") / close - 1, by = symbol]

# Remove missing
pricesm = na.omit(pricesm)

# Plot
pricesm[, decile := dplyr::ntile(vol_22, 10), by = month]
pricesm[, mean(target), by = decile][order(decile)] |>
  ggplot(aes(x = decile, y = V1)) +
  geom_bar(stat = "identity") +
  theme_minimal() +
  labs(title = "Mean target by decile of vol",
       x = "Decile",
       y = "Mean target")
pricesm[, median(target), by = decile][order(decile)] |>
  ggplot(aes(x = decile, y = V1)) +
  geom_bar(stat = "identity") +
  theme_minimal() +
  labs(title = "Mean target by decile of vol",
       x = "Decile",
       y = "Mean target")
pricesm[, mean(target), by = .(decile, year = data.table::year(date))][order(year, decile)] |>
  ggplot(aes(x = decile, y = V1)) +
  geom_bar(stat = "identity") +
  facet_wrap(~ year) +
  theme_minimal() +
  labs(title = "Mean target by decile of vol",
       x = "Decile",
       y = "Mean target")
pricesm[, median(target), by = .(decile, year = data.table::year(date))][order(year, decile)] |>
  ggplot(aes(x = decile, y = V1)) +
  geom_bar(stat = "identity") +
  facet_wrap(~ year) +
  theme_minimal() +
  labs(title = "Median target by decile of vol",
       x = "Decile",
       y = "Mean target")


# Backtest
back = pricesm[decile == 1 | decile == 10]
back = back[close_raw > 5]
back[decile == 1, signal := 1]
back[decile == 10, signal := -1]
back[, weight := 1 / length(signal), by = month]
# back = back[decile == 1]
portfolio = back[, .(ret = sum(target * weight)), by = month]
portfolio_xts = portfolio[, .(month = zoo::as.yearmon(month), ret)]
portfolio_xts = as.xts.data.table(portfolio_xts)
charts.PerformanceSummary(portfolio_xts)



# INSAMPLE OPT -----------------------------------------------------------
# Params
minp  = c(0.1, 1, 5, 10)
d     = c(0.2, 0.4, 0.6, 0.8)
nb    = c(10, 30, 50)
rollv = c(22, 66, 125, 252)
volt  = c(1000, 100000, 500000)
remove_funds = c(TRUE, FALSE)
p = expand.grid(minp, d, nb, remove_funds, rollv, volt,
                stringsAsFactors = FALSE)
setDT(p)
colnames(p) = c("minp", "d", "nb", "remove_funds", "rollv", "volt")

# Add closed columns
setorder(prices, symbol, date)
prices[, closed08 := diffseries(close, d = 0.8), by = symbol]
prices[, closed06 := diffseries(close, d = 0.6), by = symbol]
prices[, closed04 := diffseries(close, d = 0.4), by = symbol]
prices[, closed02 := diffseries(close, d = 0.2), by = symbol]
prices[, i := 1:.N, by = symbol]
prices[i == 1, let(
  closed08 = NA,
  closed06 = NA,
  closed04 = NA,
  closed02 = NA
)]
prices[, i := NULL]
prices[, month := data.table::yearmon(date)]
prices = na.omit(prices, cols = c("closed08", "closed06", "closed04", "closed02"))

# Insample opt using grid
insample_results = list()
for (i in 1:nrow(p)) {
  # i = 137
  print(i)

  # params
  p_ = p[i, ]
  minp_ = p_$minp
  d_ = p_$d
  nb_ = p_$nb
  remove_funds_ = p_$remove_funds
  rollv_ = p_$rollv
  volt_ = p_$volt

  # Calculate fracdiff returns and volatility at the end of the month
  setorder(prices, symbol, date)
  prices[, vol := roll_sd(x, rollv_), by = symbol,
         env = list(x = paste0("closed0", d_ * 10))]

  # Downsample to monthly data
  pricesm = prices[, .(
    date = max(date),
    open = data.table::first(open),
    close = data.table::last(close),
    close_raw = data.table::last(close_raw),
    volume = sum(volume),
    vol = data.table::last(vol),
    isEtf = data.table::last(isEtf),
    idFund = data.table::last(isFund)
  ), by = .(symbol, month)]

  # Filter by volume
  pricesm = pricesm[volume > volt_]

  # Crate target variable
  pricesm[, target := close / open - 1]
  pricesm[, target := shift(target, 1, type = "lead"), by = symbol]

  # Back
  back = pricesm[close_raw > minp_]
  if (remove_funds_ == TRUE) {
    back = back[isEtf == FALSE & idFund == FALSE]
  }
  # back[symbol == "agnc"]
  # back[symbol == "cnp"]
  # back[symbol == "cag"]
  back[, rank := frankv(vol, ties.method = "first"), by = month]

  # back[month == 2025]
  # dim(back)
  # dim(back[month == 2025])
  # back[month == 2025][symbol == "aeg"]
  # back[month == 2025][rank <= nb_][, .(symbol, month, close, vol, target, rank)]
  # # back[symbol == "cnp"]
  # back[symbol == "cag"]

  back = back[rank <= nb_]
  back[, weight := 1 / length(rank), by = month]
  # back[rank == 1][order(month)]
  # back[symbol == "agnc"]
  # back[symbol == "cnp"]
  # back[symbol == "cag"]
  portfolio = back[, .(ret = sum(target * weight, na.rm = TRUE)), by = month]
  setorder(portfolio, month)
  portfolio_xts = portfolio[, .(month = zoo::as.yearmon(month), ret)]
  portfolio_xts = as.xts.data.table(portfolio_xts)
  portfolio_xts = portfolio_xts[portfolio_xts$ret < 1, ]
  insample_results[[i]] = cbind(
    ret_cum = Return.cumulative(portfolio_xts)[1, 1],
    ret_anu = Return.annualized(portfolio_xts)[1, 1],
    dd      = min(Drawdowns(portfolio_xts, )),
    sharpe  = SharpeRatio.annualized(portfolio_xts, scale = 12)[1, 1]
  )
  # charts.PerformanceSummary(portfolio_xts)
}

# Merge params to res
dt = lapply(seq_along(insample_results), function(i) {
  as.data.table(cbind(
    p[i, ],
    insample_results[[i]]
  ))
})
dt = rbindlist(dt)
dt[, which.max(sharpe)]
dt[order(sharpe)]
tail(dt[order(sharpe)], 50)
dt[minp == 5][order(sharpe)]

# Best
best_p = dt[minp == 5][which.max(sharpe)]
# best_p = dt[which.max(sharpe)]
prices[, vol := roll_sd(x, best_p[, rollv]), by = symbol,
       env = list(x = paste0("closed0", d_ * 10))]
pricesm = prices[, .(
  date = max(date),
  close = data.table::last(close),
  close_raw = data.table::last(close_raw),
  volume = sum(volume),
  vol = data.table::last(vol),
  isEtf = data.table::last(isEtf),
  idFund = data.table::last(isFund)
), by = .(symbol, month)]
pricesm = pricesm[volume > best_p[, volt]]
pricesm[, target := shift(close, 1, type = "lead") / close - 1, by = symbol]
back = pricesm[close_raw > best_p[, minp]]
if (best_p[, remove_funds] == TRUE) {
  back = back[isEtf == FALSE & idFund == FALSE]
}
back[, rank := frankv(vol, ties.method = "first"), by = month]
back = back[rank <= best_p[, nb]]
setorder(back, month, symbol)
back[, weight := 1 / length(rank), by = month]
portfolio = back[, .(ret = sum(target * weight, na.rm = TRUE)), by = month]
setorder(portfolio, month)
portfolio_xts = portfolio[, .(month = zoo::as.yearmon(month), ret)]
portfolio_xts = as.xts.data.table(portfolio_xts)
portfolio_xts = portfolio_xts[portfolio_xts$ret < 1, ]
charts.PerformanceSummary(portfolio_xts)
charts.PerformanceSummary(portfolio_xts["2000/2005"])
charts.PerformanceSummary(portfolio_xts["2005/2010"])
charts.PerformanceSummary(portfolio_xts["2010/2015"])
charts.PerformanceSummary(portfolio_xts["2015/2020"])
charts.PerformanceSummary(portfolio_xts["2020/2025"])
charts.PerformanceSummary(portfolio_xts["2024"])
charts.PerformanceSummary(portfolio_xts["2021"])

# Extract symbols of best strategy and saved it for QC backtest
qc_data = back[, .(symbol, date)]
qc_data[, date := as.character(date)]
qc_data[, symbol := gsub("\\.\\d+", "", symbol)]
bl_endp_key = storage_endpoint(Sys.getenv("BLOB-ENDPOINT-SNP"),
                               Sys.getenv("BLOB-KEY-SNP"))
cont = storage_container(bl_endp_key, "qc-backtest")
storage_write_csv(as.data.frame(qc_data), cont, "lvl.csv")


# ETFS --------------------------------------------------------------------
# library(httr)
#
# tmp_file = tempfile(fileext = ".csv")
# profile = lapply(0:10, function(p) {
#   GET(
#     "https://financialmodelingprep.com/stable/profile-bulk",
#     query = list(part = p, apikey = "6c390b6279b8a67cc7f63cbecbd69430"),
#     write_disk(tmp_file, overwrite = TRUE)
#   )
#   Sys.sleep(1L)
#   dt = fread(tmp_file)
#   if (nrow(dt) == 0) return(NULL)
#   dt
# })
# profile = rbindlist(profile, fill = TRUE)
# profile = profile[!is.na(symbol)]
# profile = profile[, .(symbol, currency, country, isin, exchange, industry,
#                       sector, ipoDate, isEtf, isFund)]
# setnames(profile, "symbol", "fmp_symbol")
#
# # Save ETF and fund symbols
# profile[, sort(unique(exchange))]
# us_etf = profile[isEtf == 1 | isFund == 1] |>
#   _[exchange %in% c("AMEX", "NASDAQ", "NYSE")] |>
#   _[, .(symbol = unique(fmp_symbol))]
# us_etf
# fwrite(us_etf, "D:/strategies/lv/us_etf.csv")

