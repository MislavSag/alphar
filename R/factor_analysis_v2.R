# Title:
# Author: Name
# Description: Description

# packages
library(tiledb)
library(data.table)
library(checkmate)
library(httr)
library(qlcal)
library(fredr)
library(findata)




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
fredr::fredr_set_key(Sys.getenv("FRED-KEY"))

# set business days
qlcal::setCalendar("UnitedStates/NYSE")

# help functions and variables
future_return <- function(x, n) {
  (data.table::shift(x, n, type = 'lead') - x) / x
}
trading_year <- 256
trading_halfyear <- trading_year / 2
trading_quarter <- trading_year / 4

# help function for weigthed mean
# source: https://stackoverflow.com/questions/40269022/weighted-average-using-na-weights
weighted_mean = function(x, w, ..., na.rm=FALSE){
  if(na.rm){
    keep = !is.na(x)&!is.na(w)
    w = w[keep]
    x = x[keep]
  }
  weighted.mean(x, w, ..., na.rm=FALSE)
}



# UNIVERSE ----------------------------------------------------------------
# universe consists of US stocks
url <- modify_url("https://financialmodelingprep.com/",
                  path = "api/v3/stock/list",
                  query = list(apikey = Sys.getenv("APIKEY-FMPCLOUD")))
p <- GET(url)
res <- content(p)
securities <- rbindlist(res, fill = TRUE)
stocks_us <- securities[type == "stock" &
                          exchangeShortName %in% c("AMEX", "NASDAQ", "NYSE", "OTC")]
symbols_list <- stocks_us[, unique(symbol)]

# profiles
p <- GET("https://financialmodelingprep.com//api/v4/profile/all",
         query = list(apikey = Sys.getenv("APIKEY-FMPCLOUD")),
         write_disk("tmp/profiles.csv", overwrite = TRUE))
profiles <- fread("tmp/profiles.csv")
profiles <- profiles[country == "US"]
setnames(profiles, tolower(colnames(profiles)))

# sp500 historical universe raw
url <- "https://financialmodelingprep.com/api/v3/historical/sp500_constituent?apikey=15cd5d0adf4bc6805a724b4417bbaafc"
p <- GET(url)
res <- content(p)
sp500 <- rbindlist(res)
sp500_symbols <- c(sp500[, symbol], sp500[, removedTicker])
sp500_symbols <- sp500_symbols[sp500_symbols != ""]

# sp500 historical universe cleaned
fmp = FMP$new()
sp500_symbols_fmp = fmp$get_sp500_symbols()



# # FF DATA -----------------------------------------------------------------
# # FF data download
# factors_ff_daily_raw <- download_french_data("Fama/French 3 Factors [Daily]")
# factors_ff_daily <- factors_ff_daily_raw$subsets$data[[1]]
# factors_ff_daily[, `:=`(
#   date = as-Date(date, origin ),
#   rf = as.numeric(RF) / 100,
#   mkt_excess = as.numeric(`Mkt-RF`) / 100,
#   smb = as.numeric(SMB) / 100,
#   hml = as.numeric(HML) / 100
# )]
# factors_ff_daily <- factors_ff_daily_raw$subsets$data[[1]] |>
#   transmute(
#     date = ymd(date),
#     rf = as.numeric(RF) / 100,
#     mkt_excess = as.numeric(`Mkt-RF`) / 100,
#     smb = as.numeric(SMB) / 100,
#     hml = as.numeric(HML) / 100
#   ) |>
#   filter(date >= start_date & date <= end_date)




# MARKET CAP --------------------------------------------------------------
# market cap data
arr <- tiledb_array("s3://equity-usa-market-cap",
                    as.data.frame = TRUE,
                    query_layout = "UNORDERED")
system.time(market_cap <- arr[])
tiledb_array_close(arr)
market_cap <- as.data.table(market_cap)
market_cap <- market_cap[date %in% qlcal::getBusinessDays(min(date, na.rm = TRUE),
                                                          max(date, na.rm = TRUE))]



# EARNING ANNOUNCEMENTS ---------------------------------------------------
# get earning announcmenet evetns data from FMP
arr <- tiledb_array("s3://equity-usa-earningsevents", as.data.frame = TRUE)
events <- arr[]
events <- as.data.table(events)
setorder(events)
events <- events[date < Sys.Date()]                 # remove announcements for today
events <- unique(events, by = c("symbol", "date"))  # remove duplicated symbol / date pair
events <- events[symbol %in% symbols_list]          # keep only relevant symbols

# create predictors from earnings announcements
events[, `:=`(
  nincr = frollsum(eps > epsEstimated, 4, na.rm = TRUE),
  nincr_half = frollsum(eps > epsEstimated, 2, na.rm = TRUE),
  nincr_2y = frollsum(eps > epsEstimated, 8, na.rm = TRUE),
  nincr_3y = frollsum(eps > epsEstimated, 12, na.rm = TRUE),
  eps_diff = (eps - epsEstimated + 0.00001) / (epsEstimated + 0.00001)
), by = symbol]



# FUNDAMENTAL DATA --------------------------------------------------------
# income statement data
arr <- tiledb_array("s3://equity-usa-income-statement-bulk",
                    as.data.frame = TRUE,
                    query_layout = "UNORDERED")
system.time(pl <- arr[])
tiledb_array_close(arr)
pl <- as.data.table(pl)
pl[, `:=`(acceptedDateTime = as.POSIXct(acceptedDate, format = "%Y-%m-%d %H:%M:%S", tz = "America/New_York"),
          acceptedDate = as.Date(acceptedDate, format = "%Y-%m-%d %H:%M:%S", tz = "America/New_York"))]

# balance sheet data
arr <- tiledb_array("s3://equity-usa-balance-sheet-statement-bulk",
                    as.data.frame = TRUE,
                    query_layout = "UNORDERED")
system.time(bs <- arr[])
tiledb_array_close(arr)
bs <- as.data.table(bs)
bs[, `:=`(acceptedDateTime = as.POSIXct(acceptedDate, format = "%Y-%m-%d %H:%M:%S", tz = "America/New_York"),
          acceptedDate = as.Date(acceptedDate, format = "%Y-%m-%d %H:%M:%S", tz = "America/New_York"))]

# financial growth
arr <- tiledb_array("s3://equity-usa-financial-growth-bulk",
                    as.data.frame = TRUE, query_layout = "UNORDERED")
system.time(fin_growth <- arr[])
tiledb_array_close(arr)
fin_growth <- as.data.table(fin_growth)

# financial ratios
arr <- tiledb_array("s3://equity-usa-key-metrics-bulk",
                    as.data.frame = TRUE, query_layout = "UNORDERED")
system.time(fin_ratios <- arr[])
tiledb_array_close(arr)
fin_ratios <- as.data.table(fin_ratios)

# merge all fundamental data
columns_diff_pl <- c("symbol", "date", setdiff(colnames(pl), colnames(bs)))
columns_diff_fg <- c("symbol", "date", setdiff(colnames(fin_growth), colnames(pl)))
columns_diff_fr <- c("symbol", "date", setdiff(colnames(fin_ratios), colnames(pl)))
fundamentals <- Reduce(function(x, y) merge(x, y, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE),
                       list(bs,
                            pl[, ..columns_diff_pl],
                            fin_growth[, ..columns_diff_fg],
                            fin_ratios[, ..columns_diff_fr]))



# MARKET DATA -------------------------------------------------------------
# market daily data
arr <- tiledb_array("D:/equity-usa-daily-fmp",
                    as.data.frame = TRUE,
                    query_layout = "UNORDERED"
)
system.time(prices <- arr[])
tiledb_array_close(arr)
prices <- as.data.table(prices)

# keep only US securites
prices <- prices[symbol %in% c("SPY", symbols_list)]

# free resources
gc()

# clean daily prices
prices_dt <- unique(prices, by = c("symbol", "date")) # remove duplicates if they exists
prices_dt <- prices_dt[date %in% qlcal::getBusinessDays(min(date, na.rm = TRUE),
                                                        max(date, na.rm = TRUE))]
setorder(prices_dt, "symbol", "date")
prices_dt <- prices_dt[open > 0 & high > 0 & low > 0 & close > 0 & adjClose > 0] # remove rows with zero and negative prices
prices_dt[, returns := adjClose   / data.table::shift(adjClose) - 1, by = symbol] # calculate returns
adjust_cols <- c("open", "high", "low")
prices_dt[, (adjust_cols) := lapply(.SD, function(x) x * (adjClose / close)), .SDcols = adjust_cols] # adjust open, high and low prices
prices_dt[, close := adjClose]
prices_dt <- na.omit(prices_dt[, .(symbol, date, open, high, low, close, volume, returns)])

# remove observations with extreme prices
prices_dt <- prices_dt[returns < 0.8 & returns > -0.8]
prices_dt[, high_return := high / shift(high) - 1, by = symbol]
prices_dt <- prices_dt[high_return < 0.8 & high_return > -0.8]
prices_dt[, low_return := low / shift(low) - 1, by = symbol]
prices_dt <- prices_dt[low_return < 0.8 & low_return > -0.8]
prices_dt[, `:=`(low_return = NULL, high_return = NULL)]

# remove observation with less than 3 years of data
prices_n <- prices_dt[, .N, by = symbol]
prices_n <- prices_n[N > (trading_year * 3)]  # remove prices with only 700 or less observations
prices_dt <- prices_dt[symbol %in% prices_n[, symbol]]

# save SPY for later and keep only events symbols
spy <- prices_dt[symbol == "SPY"]

# add profiles data
prices_dt = profiles[, .(symbol, industry, sector)][prices_dt, on = "symbol"]

# add market cap data
prices_dt <- market_cap[prices_dt, on = c("symbol", "date")]

# add number of shares
# fundamentals[, 1:10]
# fundamentals[, .(symbol, acceptedDate, weightedAverageShsOut, weightedAverageShsOutDil)]
# fundamentals[symbol == "AAPL", .(symbol, acceptedDate, weightedAverageShsOut, weightedAverageShsOutDil)]
# colnames(fundamentals)[grep("weight", colnames(fundamentals), ignore.case = TRUE)]
fund_merge <- fundamentals[, .(symbol, date = acceptedDate, weightedAverageShsOut,
                               weightedAverageShsOutDil)]
prices_dt <- fund_merge[prices_dt, on = c("symbol", "date"), roll = Inf]



# OHLCV PREDICTORS --------------------------------------------------------
# momentum
setorder(prices_dt, "symbol", "date") # order, to be sure
prices_dt[, mom1w := close / shift(close, 5) - 1, by = symbol]
prices_dt[, mom1w_lag := shift(close, 10) / shift(close, 5) - 1, by = symbol]
prices_dt[, mom1m := close / shift(close, 22) - 1, by = symbol]
prices_dt[, mom1m_lag := shift(close, 22) / shift(close, 44) - 1, by = symbol]
prices_dt[, mom6m := close / shift(close, 22 * 6) - 1, by = symbol]
prices_dt[, mom6m_lag := shift(close, 22) / shift(close, 22 * 7) - 1, by = symbol]
prices_dt[, mom12m := close / shift(close, 22 * 12) - 1, by = symbol]
prices_dt[, mom12m_lag := shift(close, 22) / shift(close, 22 * 13) - 1, by = symbol]
prices_dt[, mom36m := close / shift(close, 22 * 36) - 1, by = symbol]
prices_dt[, mom36m_lag := shift(close, 22) / shift(close, 22 * 37) - 1, by = symbol]
prices_dt[, chmom := mom6m / shift(mom6m, 22) - 1, by = symbol]
prices_dt[, chmom_lag := shift(mom6m, 22) / shift(mom6m, 44) - 1, by = symbol]

# maximum and minimum return
prices_dt[, maxret_3y := roll::roll_max(returns, trading_year * 3), by = symbol]
prices_dt[, maxret_3y := roll::roll_max(returns, trading_year * 3), by = symbol]
prices_dt[, maxret_2y := roll::roll_max(returns, trading_year * 2), by = symbol]
prices_dt[, maxret := roll::roll_max(returns, trading_year), by = symbol]
prices_dt[, maxret_half := roll::roll_max(returns, trading_halfyear), by = symbol]
prices_dt[, maxret_quarter := roll::roll_max(returns, trading_quarter), by = symbol]
prices_dt[, minret_3y := roll::roll_min(returns, trading_year * 3), by = symbol]
prices_dt[, minret_2y := roll::roll_min(returns, trading_year * 2), by = symbol]
prices_dt[, minret := roll::roll_min(returns, trading_year), by = symbol]
prices_dt[, minret_half := roll::roll_min(returns, trading_halfyear), by = symbol]
prices_dt[, minret_quarter := roll::roll_min(returns, trading_quarter), by = symbol]

# sector momentum
sector_returns <- prices_dt[, .(sec_returns = weighted_mean(returns, marketCap)), by = .(sector, date)]
sector_returns[, secmom := frollapply(sec_returns, 22, function(x) prod(1 + x) - 1), by = sector]
sector_returns[, secmom_lag := shift(secmom, 22), by = sector]
sector_returns[, secmom6m := frollapply(sec_returns, 22, function(x) prod(1 + x) - 1), by = sector]
sector_returns[, secmom6m_lag := shift(secmom6m, 22), by = sector]
sector_returns[, secmomyear := frollapply(sec_returns, 22 * 12, function(x) prod(1 + x) - 1), by = sector]
sector_returns[, secmomyear_lag := shift(secmomyear, 22), by = sector]
sector_returns[, secmom2year := frollapply(sec_returns, 22 * 24, function(x) prod(1 + x) - 1), by = sector]
sector_returns[, secmom2year_lag := shift(secmom2year, 22), by = sector]
sector_returns[, secmom3year := frollapply(sec_returns, 22 * 36, function(x) prod(1 + x) - 1), by = sector]
sector_returns[, secmom3year_lag := shift(secmom3year, 22), by = sector]

# industry momentum
ind_returns <- prices_dt[, .(indret = weighted_mean(returns, marketCap)), by = .(industry, date)]
ind_returns[, indmom := frollapply(indret, 22, function(x) prod(1 + x) - 1), by = industry]
ind_returns[, indmom_lag := shift(indmom, 22), by = industry]
ind_returns[, indmom6m := frollapply(indret, 22, function(x) prod(1 + x) - 1), by = industry]
ind_returns[, indmom6m_lag := shift(indmom6m, 22), by = industry]
ind_returns[, indmomyear := frollapply(indret, 22 * 12, function(x) prod(1 + x) - 1), by = industry]
ind_returns[, indmomyear_lag := shift(indmomyear, 22), by = industry]
ind_returns[, indmom2year := frollapply(indret, 22 * 24, function(x) prod(1 + x) - 1), by = industry]
ind_returns[, indmom2year_lag := shift(indmom2year, 22), by = industry]
ind_returns[, indmom3year := frollapply(indret, 22 * 36, function(x) prod(1 + x) - 1), by = industry]
ind_returns[, indmom3year_lag := shift(indmom3year, 22), by = industry]

# merge sector and industry predictors to prices_dt
prices_dt <- sector_returns[prices_dt, on = c("sector", "date")]
prices_dt <- ind_returns[prices_dt, on = c("industry", "date")]

# volatility
prices_dt[, dvolume := volume * close]
prices_dt[, `:=`(
  dolvol = frollsum(dvolume, 22),
  dolvol_halfyear = frollsum(dvolume, trading_halfyear),
  dolvol_year = frollsum(dvolume, trading_year),
  dolvol_2year = frollsum(dvolume, trading_year * 2),
  dolvol_3year = frollsum(dvolume, trading_year * 3)
), by = symbol]
prices_dt[, `:=`(
  dolvol_lag = shift(dolvol, 22),
  dolvol_halfyear_lag = frollsum(dolvol_halfyear, 22),
  dolvol_year_lag = frollsum(dolvol_year, 22)
), by = symbol]

# illiquidity
prices_dt[, `:=`(
  illiquidity = abs(returns) / volume, #  Illiquidity (Amihud's illiquidity)
  illiquidity_month = frollsum(abs(returns), 22) / frollsum(volume, 22),
  illiquidity_half_year = frollsum(abs(returns), trading_halfyear) /
    frollsum(volume, trading_halfyear),
  illiquidity_year = frollsum(abs(returns), trading_year) / frollsum(volume, trading_year),
  illiquidity_2year = frollsum(abs(returns), trading_year * 2) / frollsum(volume, trading_year * 2),
  illiquidity_3year = frollsum(abs(returns), trading_year * 3) / frollsum(volume, trading_year * 3)
), by = symbol]

# Share turnover
prices_dt[, `:=`(
  share_turnover = volume / weightedAverageShsOut,
  turn = frollsum(volume, 22) / weightedAverageShsOut,
  turn_half_year = frollsum(volume, trading_halfyear) / weightedAverageShsOut,
  turn_year = frollsum(volume, trading_year) / weightedAverageShsOut,
  turn_2year = frollsum(volume, trading_year * 2) / weightedAverageShsOut,
  turn_3year = frollsum(volume, trading_year * 2) / weightedAverageShsOut
), by = symbol]

# Std of share turnover
prices_dt[, `:=`(
  std_turn = roll::roll_sd(share_turnover, 22),
  std_turn_6m = roll::roll_sd(share_turnover, trading_halfyear),
  std_turn_1y = roll::roll_sd(share_turnover, trading_year)
), by = symbol]

# return volatility
prices_dt[, `:=`(
  retvol = roll::roll_sd(returns, width = 22),
  retvol3m = roll::roll_sd(returns, width = 22 * 3),
  retvol6m = roll::roll_sd(returns, width = 22 * 6),
  retvol1y = roll::roll_sd(returns, width = 22 * 12),
  retvol2y = roll::roll_sd(returns, width = 22 * 12 * 2),
  retvol3y = roll::roll_sd(returns, width = 22 * 12 * 3)
), by= symbol]
prices_dt[, `:=`(
  retvol_lag = shift(retvol, 22),
  retvol3m_lag = shift(retvol3m, 22),
  retvol6m_lag = shift(retvol6m, 22),
  retvol1y_lag = shift(retvol1y, 22)
), by = symbol]

# idiosyncratic volatility
weekly_market_returns <- prices_dt[, .(market_returns = mean(returns, na.rm = TRUE)),
                                   by = date]
weekly_market_returns[, market_returns := frollapply(market_returns, 22, function(x) prod(1+x)-1)]
prices_dt <- weekly_market_returns[prices_dt, on = "date"]
id_vol <- na.omit(prices_dt, cols = c("market_returns", "mom1w"))
id_vol <- id_vol[, .(date, e = QuantTools::roll_lm(market_returns, mom1w, trading_year * 3)[, 3][[1]]),
                 by = symbol]
id_vol[, `:=`(
  idvol = roll::roll_sd(e, 22),
  idvol3m = roll::roll_sd(e, 22 * 3),
  idvol6m = roll::roll_sd(e, 22 * 6),
  idvol1y = roll::roll_sd(e, trading_year),
  idvol2y = roll::roll_sd(e, trading_year * 2),
  idvol3y = roll::roll_sd(e, trading_year * 3)
)]
prices_dt <- id_vol[prices_dt, on = c("symbol", "date")]



# FUNDAMENTAL DATA --------------------------------------------------------
# create Gu et al predictors
fundamentals[, `:=`(
  # Gu et al predictors
  agr = assetGrowth,          # asset growth
  bm = 1 / pbRatio,
  cash = cashAndCashEquivalents / totalAssets, # cash holdings
  chcsho = weightedAverageSharesGrowth,        # change in shares outstanding
  currat = currentRatio,      # current ratio
  dy = dividendYield,         # dividend to price
  ep = 1 / peRatio,           # earnings to price,
  lgr = longTermDebt / shift(longTermDebt) - 1, # growth in long term debt
  mvel1 = marketCap,          # size
  pchcurrat = currentRatio / shift(currentRatio) - 1,  # % change in current ratio
  pchdepr = depreciationAndAmortization / shift(depreciationAndAmortization) - 1, # % change in depreciation
  rd = ResearchAndDevelopmentExpenses / shift(ResearchAndDevelopmentExpenses) - 1, # R&D increase
  rd_mve = ResearchAndDevelopmentExpenses / marketCap, # R&D to market capitalization
  rd_sale = ResearchAndDevelopmentExpenses / revenue,  # R&D to sales
  salecash = revenue / cashAndCashEquivalents,  # sales to cash
  saleinv = revenue / inventory,  # sales to inventory
  salerec = revenue / averageReceivables,  # sales to receivables
  sgr = revenueGrowth,        # sales growth
  sp = 1 / priceToSalesRatio  # sales to price,
  # help variables

), by = symbol]
# fundamentals[, `:=`(
#   lgr = ifelse(is.nan(lgr), 0, lgr),
#   pchcurrat = ifelse(is.nan(pchcurrat), 0, pchcurrat),
#   pchdepr = ifelse(is.nan(pchdepr), 0, pchdepr),
#   rd = ifelse(is.nan(rd), 0, rd)
# )]
# tail(fundamentals)
#
colnames(fundamentals)[grep("book", colnames(fundamentals), ignore.case = TRUE)]
#
# # merge features and fundamental data
# features <- merge(features, fundamentals, by.x = c("symbol", "date"),
#                   by.y = c("symbol", "acceptedDate"), all.x = TRUE, all.y = FALSE)
# features[, `:=`(period.x = NULL, period.y = NULL, period = NULL, link = NULL,
#                 finalLink = NULL, reportedCurrency = NULL)]
# features[symbol == "AAPL", .(symbol, fundamental_date, date, acceptedDateFundamentals)]
#
# # convert char features to numeric features
# char_cols <- features[, colnames(.SD), .SDcols = is.character]
# char_cols <- setdiff(char_cols, c("symbol", "time", "right_time"))
# features[, (char_cols) := lapply(.SD, as.numeric), .SDcols = char_cols]



# MACRO PREDICTORS --------------------------------------------------------
# TODO: ADD SP500 stocks after Matej finish his task!
# net expansion
welch_goyal <- prices_dt[, .(symbol, date, marketCap, returns)]
welch_goyal <- welch_goyal[, .(mcap = sum(marketCap, na.rm = TRUE),
                               vwretx = weighted_mean(returns, marketCap, na.rm = TRUE)), by = date]
setorder(welch_goyal, date)
welch_goyal[, ntis := mcap - (shift(mcap, 22) *
                                (1 + frollapply(vwretx, 22, function(x) prod(1+x)-1)))]
welch_goyal[, ntis3m := mcap - (shift(mcap, 22 * 3) *
                                  (1 + frollapply(vwretx, 22 * 3, function(x) prod(1+x)-1)))]
welch_goyal[, ntis_halfyear := mcap - (shift(mcap, trading_halfyear) *
                                         (1 + frollapply(vwretx, trading_halfyear, function(x) prod(1+x)-1)))]
welch_goyal[, ntis_year := mcap - (shift(mcap, trading_year) *
                                     (1 + frollapply(vwretx, trading_year, function(x) prod(1+x)-1)))]
welch_goyal[, ntis_2year := mcap - (shift(mcap, trading_year * 2) *
                                      (1 + frollapply(vwretx, trading_year*2, function(x) prod(1+x)-1)))]
welch_goyal[, ntis_3year := mcap - (shift(mcap, trading_year * 3) *
                                      (1 + frollapply(vwretx, trading_year*3, function(x) prod(1+x)-1)))]

# dividend to price for all market
url <- "https://financialmodelingprep.com/api/v3/historical-price-full/stock_dividend/"
dividends_l <- lapply(sp500_symbols_fmp, function(s){
  p <- GET(paste0(url, s), query = list(apikey = Sys.getenv("APIKEY-FMPCLOUD")))
  res <- content(p)
  dividends_ <- rbindlist(res$historical, fill = TRUE)
  cbind(symbol = res$symbol, dividends_)
})
dividends <- rbindlist(dividends_l)
dividends[, date := as.Date(date)]
dividends <- na.omit(dividends, cols = c("adjDividend", "date"))
# dividends <- unique(dividends, by = c("symbol", "date"))
dividends[order(adjDividend)]


# dividend sp500
dividends_sp500 <- dividends[, .(div = sum(adjDividend , na.rm = TRUE)), by = date]
setorder(dividends_sp500, date)
dividends_sp500[, div_year := frollsum(div, trading_year, na.rm = TRUE)]
dividends_sp500 <- dividends_sp500[spy, on = "date"]
dividends_sp500[, div_year := nafill(div_year, "locf")]
dividends_sp500[, dp := div_year / close]
dividends_sp500[, dy := div_year / shift(close)]
dividends_sp500 <- na.omit(dividends_sp500[, .(date, dp, dy)])

# add to welch_goyal
welch_goyal <- dividends_sp500[welch_goyal, on = "date"]
plot(as.xts.data.table(welch_goyal[, .(date, dp)]))

# tresury bills
tb3ms <- fredr_series_observations(
  series_id = "TB3MS",
  observation_start = as.Date("2000-01-01") - 1,
  observation_end = Sys.Date(),
  realtime_start = as.Date("2000-01-01"),
  realtime_end = Sys.Date()
)
tb3ms <- as.data.table(tb3ms)
tb3ms <- tb3ms[, .(realtime_start, value)]
setnames(tb3ms, c("date", "tbl"))

# add to welch_goyal
welch_goyal <- tb3ms[welch_goyal, on = c("date"), roll = Inf]
plot(welch_goyal$tb3ms)



library(rvest)
p <- read_html("https://www.gurufocus.com/economic_indicators/150/sp-500-dividend-yield")
p |>
  html_elements("#non-sticky-table")


fundamentals
colnames(fundamentals)[grep("div", colnames(fundamentals), ignore.case = FALSE)]
sp500_symbols
# /api/v3/historical-price-full/stock_dividend/AAPL

# volume of growth / decline stocks
vol_52 <- prices_dt[, .(symbol, date, high, low, close, volume)]
vol_52[, return_52w := close / shift(close, n = 52 * 5) - 1, by = "symbol"]
vol_52[return_52w > 0, return_52w_dummy := "up", by = "symbol"]
vol_52[return_52w <= 0, return_52w_dummy := "down", by = "symbol"]
vol_52 <- vol_52[!is.na(return_52w_dummy)]
vol_52_indicators <- vol_52[, .(volume = sum(volume, na.rm = TRUE)), by = c("return_52w_dummy", "date")]
setorderv(vol_52_indicators, c("return_52w_dummy", "date"))
vol_52_indicators <- dcast(vol_52_indicators, date ~ return_52w_dummy, value.var = "volume")
vol_52_indicators[, volume_down_up_ratio := down / up]
vol_52_indicators <- vol_52_indicators[, .(date, volume_down_up_ratio)]




# MERGE PREDICTORS --------------------------------------------------------
# merge prices, earnings accouncements, fundamentals and  macro vars
predictors <- fundamentals[prices_dt, on = c("symbol", "acceptedDate" = "date"), roll = -Inf]


