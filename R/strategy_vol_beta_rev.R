library(finutils)
library(fastverse)
library(PerformanceAnalytics)
library(roll)
library(RollingWindow)
library(ggplot2)
library(httr)


# Import daily data
prices = qc_daily(
   file_path = "F:/lean/data/stocks_daily.csv",
   min_obs = 253,
   duplicates = "fast",
   market_symbol = "spy"
)

# List of etfs
etfs = list.files("F:/lean/data/equity/usa/universes/etf")

# Get profiles for industry
parts = 0:10
for (part in parts) {
  GET("https://financialmodelingprep.com/stable/profile-bulk",
      query = list(apikey = "6c390b6279b8a67cc7f63cbecbd69430", part = part),
      write_disk(paste0("profile_", part, ".csv")))
  Sys.sleep(1L)
}
profile_files = paste0("profile_", parts, ".csv")
profile = lapply(profile_files, fread)
profile = rbindlist(profile, fill = TRUE)
profile = unique(profile, by = "symbol")
lapply(profile_files, file.remove)

# Calculate volatility
prices[, roll_vol := roll_sd(returns, 252), by = symbol]

# Calculate beta
prices[, beta := RollingWindow::RollingBeta(returns, market_ret, 252), by = symbol]

# Merge spy volatility with current vol
spy = prices[symbol == "spy"]
prices = merge(prices, spy[, .(date, spy_vol = roll_vol)], by = "date", all.x = TRUE, all.y = FALSE)
prices = na.omit(prices, cols = "spy_vol")

# Donwsample to monhly data
prices[, month := data.table::yearmon(date)]
pricesm = prices[, .(
  date = data.table::last(date),
  open = data.table::first(open),
  close = data.table::last(close),
  close_raw = data.table::last(close_raw),
  volume = sum(volume),
  roll_vol = data.table::last(roll_vol),
  spy_vol = data.table::last(spy_vol),
  beta = data.table::last(beta)
), by = .(symbol, month)]

# Define target variable
setorder(pricesm, symbol, date)
pricesm[, target_ret := close / open - 1, by = symbol]
pricesm[, target_ret := shift(target_ret, type = "lead"), by = symbol]
pricesm = na.omit(pricesm, cols = "target_ret")
spym = pricesm[symbol == "spy", .(month, date, target_ret)]

# Calculate momentum
pricesm[, mom12 := shift(close, 1) / shift(close, 12) - 1, by = symbol]
pricesm = na.omit(pricesm, cols = "mom12")

# Backtest of leasty volatile
back = pricesm[roll_vol < spy_vol]
# back = back[mom12 > 0]
back[, weight := 1 / nrow(.SD), by = month]
portfolio = back[, .(ret = sum(weight * target_ret)), by = month]
portfolio_xts = spym[portfolio, on = "month"]
portfolio_xts = portfolio_xts[, .(date = zoo::as.yearmon(month), strategy = ret, benchmark = target_ret)]
portfolio_xts = as.xts.data.table(portfolio_xts)
charts.PerformanceSummary(portfolio_xts)
SharpeRatio.annualized(portfolio_xts)

# Backtest of least volatile and beta approach to zero
# back = pricesm[roll_vol < spy_vol]
back = na.omit(back, cols = "beta")
back[, abs_beta := abs(beta)]
back[, beta_rank := frank(abs_beta, ties.method = "first"), by = month]
n = 50
back = back[beta_rank <= n]
back[, weight := 1 / nrow(.SD), by = month]
back[month == max(month), symbol]
portfolio = back[, .(ret = sum(weight * target_ret)), by = month]
portfolio = spym[portfolio, on = "month"]
portfolio = portfolio[, .(date = zoo::as.yearmon(month),
                          strategy = ret * 2,
                          benchmark = target_ret)]
portfolio = portfolio[strategy < 0.5]
portfolio_xts = as.xts.data.table(portfolio)
charts.PerformanceSummary(portfolio_xts)
SharpeRatio.annualized(portfolio_xts)
lapply(Drawdowns(portfolio_xts), min)
Return.annualized(portfolio_xts)
portfolio[date == max(date)]

# Backtest of least volatile and beta approach to zero witout ETF's
back = pricesm[roll_vol < spy_vol]
# back[, .N, month] |>
#   ggplot(aes(x = month, y = N)) +
#   geom_col()

# back = copy(pricesm)
back[, fmp_ticker := gsub("\\.\\d+", "", toupper(symbol))]
profile_ = profile[symbol %in% back[, unique(fmp_ticker)]]
back = merge(back,
             profile[, .(symbol, marketCap, lastDividend, companyName, sector, ipoDate, isEtf, isFund)],
             by.x = "fmp_ticker", by.y = "symbol", all.x = TRUE, all.y = FALSE)
back = na.omit(back, cols = "sector")
back = back[symbol %notin% etfs]
back = na.omit(back, cols = "beta")

back = back[isEtf == FALSE & isFund == FALSE & close_raw > 5 & volume > 200000]

# back[, .N, month] |>
#   ggplot(aes(x = month, y = N)) +
#   geom_col()

back[, abs_beta := abs(beta)]
back[, beta_rank := frankv(abs_beta, order = 1L, ties.method = "first"), by = month]

n = 100
back = back[beta_rank <= n]
back[, mom_rank := frankv(mom12, order = 1L, ties.method = "first"), by = month]
nmom = 20
back = back[mom_rank <= nmom]

back[, weight := 1 / nrow(.SD), by = month]
back[month == max(month), symbol]
portfolio = back[, .(ret = sum(weight * target_ret)), by = month]
portfolio = spym[portfolio, on = "month"]
portfolio = portfolio[, .(date = zoo::as.yearmon(month),
                          strategy = ret,
                          benchmark = target_ret)]
portfolio = portfolio[strategy < 0.5]
portfolio_xts = as.xts.data.table(portfolio)
charts.PerformanceSummary(portfolio_xts)
charts.PerformanceSummary(portfolio_xts["2004/"])
# charts.PerformanceSummary(portfolio_xts["2024/"])
# charts.PerformanceSummary(portfolio_xts["2022/"])
SharpeRatio.annualized(portfolio_xts["2004/"])
lapply(Drawdowns(portfolio_xts), min)
Return.annualized(portfolio_xts)

1. Vol dionice < vol SPY-a
2. kompanije, ne trgujemo ETF-ove (QC HasFundmantalata u fundamnetal)
3. Beta sto blize nuli (min(abs(beta))), ostavljemo m
4. Kupujemo n dioncia koje su najvise pale u godinu dana (t_1 / t_12 - 1)
