library(evir)
library(fGarch)
library(rugarch)
library(PerformanceAnalytics)
library(data.table)
library(tiledb)
library(rvest)



# SET UP ------------------------------------------------------------------
# globals
DATAPATH      = "F:/lean_root/data/all_stocks_hour.csv"
URIEXUBER     = "F:/equity-usa-hour-exuber"
NASPATH       = "C:/Users/Mislav/SynologyDrive/trading_data"



# UNIVERSE ----------------------------------------------------------------
# SPY constitues
spy_const = fread(file.path(NASPATH, "spy.csv"))
symbols_spy = unique(spy_const)

# SP 500
sp500_changes = read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies") %>%
  html_elements("table") %>%
  .[[2]] %>%
  html_table()
sp500_changes = sp500_changes[-1, c(1, 2, 4)]
sp500_changes = as.data.table(sp500_changes)
sp500_changes[, Date := as.Date(Date, format = "%b %d, %Y")]
sp500_changes = sp500_changes[Date < as.Date("2009-06-28")]
sp500_changes_symbols = unlist(sp500_changes[, 2:3], use.names = FALSE)
sp500_changes_symbols = unique(sp500_changes_symbols)
sp500_changes_symbols = sp500_changes_symbols[sp500_changes_symbols != ""]

# SPY constitues + SP 500
symbols_sp500 = unique(c(symbols_spy[, ticker], sp500_changes_symbols))
symbols_sp500 = tolower(symbols_sp500)
symbols_sp500 = c("spy", symbols_sp500)



# IMPORT DATA -------------------------------------------------------------
# import QC data
col = c("date", "open", "high", "low", "close", "volume", "close_adj", "symbol")
dt = fread(DATAPATH, col.names = col)

# filter symbols
dt = dt[symbol %chin% symbols_sp500]

# set time zone
attributes(dt)
if (!("tzone" %in% names(attributes(dt)))) {
  setattr(dt, 'tzone', "America/New_York")
} else if (attributes(dt)[["tzone"]] != "America/New_York") {
  dt[, date := force_tz(date, "America/New_York")]
}
attributes(dt)

# unique
dt <- unique(dt, by = c("symbol", "date"))

# adjust all columns
unadjustd_cols = c("open", "high", "low")
adjusted_cols = paste0(unadjustd_cols, "_adj")
dt[, (adjusted_cols) := lapply(.SD, function(x) (close_adj / close) * x), .SDcols = unadjustd_cols]

# remove NA values
dt = na.omit(dt)

# order
setorder(dt, symbol, date)

# free resources
gc()




# STRATEGY ----------------------------------------------------------------
# compute returns
dt[, returns := close_adj / shift(close_adj) - 1, by = symbol]

# compute the correlation between the stocks
dt_corr = na.omit(dt[, .(symbol, date, returns)])
dt_corr = dcast(dt_corr, date ~ symbol, value.var = "returns")
dt_corr = na.omit(dt_corr, cols = "aapl")

dt_corr[, 1:10]
cor(dt_corr[, 2:10])
dt_corr = corr()

# Compute returns
returns <- na.omit(diff(log(prices)))

# Compute the correlation between the stocks
correlation <- cor(returns)

# Fit a GPD to the tails of the return distribution for each stock
gpd_models <- lapply(returns, function(x) {
  threshold <- quantile(x, 0.1)  # Set threshold for the 10% worst returns
  gpd(x, threshold)
})

# Estimate VaR and ES for each stock
VaRs <- lapply(gpd_models, function(gpd) -2(gpd, 0.01))  # 1% VaR
ESs <- lapply(gpd_models, function(gpd) -gpd.sfall(gpd, 0.01))  # 1% ES

# Aggregate VaR and ES across the portfolio, accounting for correlations
portfolio_VaR <- sum(unlist(VaRs)) * sqrt(t(correlation) %*% correlation)
portfolio_ES <- sum(unlist(ESs)) * sqrt(t(correlation) %*% correlation)


