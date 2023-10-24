# Paper: https://www.tandfonline.com/doi/full/10.1080/0015198X.2021.1908775

library(data.table)
library(httr)
library(rvest)
library(roll)
library(AzureStor)



# SET UP ------------------------------------------------------------------
# globals
PATHOHLCV = "F:/lean_root/data/equity/usa/daily"

# help vars
cols_ohlc = c("open", "high", "low", "close")
cols_ohlcv = c("open", "high", "low", "close", "volume")

# creds for Azure Blob Storage
ENDPOINT = storage_endpoint(Sys.getenv("BLOB-ENDPOINT-SNP"),
                            Sys.getenv("BLOB-KEY-SNP"))
cont = storage_container(ENDPOINT, "indexes")



# DATA --------------------------------------------------------------------
# help function to import symbols
get_symbol = function(symbol) {
  dt = fread(cmd = paste0("unzip -p ", PATHOHLCV, "/", symbol, ".zip"),
              col.names = c("date", cols_ohlcv))
  dt[, date := as.Date(substr(date, 1, 8), "%Y%m%d")]
  dt[, (cols_ohlc) := lapply(.SD, function(x) x / 10000), .SDcols = cols_ohlc]
  dt = dt[, .(date, close)]
  setnames(dt, "close", paste0("close_", symbol))
  return(dt)
}

# import ohlcv data for bond, equity and commodities
bond = get_symbol("tlt")
equity = get_symbol("spy")
commodity = get_symbol("dbc") # # DBC (GSG, USCI)
tips = get_symbol("tip") # # DBC (GSG, USCI)

# monthl index data
get_index = function(blob_name, close_name = "close_bond") {
  x = as.data.table(storage_read_csv(cont, blob_name))
  x = x[, .(date = Date, close = Value)]
  setnames(x, "close", close_name)
}
bondm = get_index("bond_index.csv")
equitym = get_index("equity_index.csv", "close_equity")
commoditym = get_index("commodity_index.csv", "close_commodity")

# yields
url = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/TextView"
urls = paste0(url, "?type=daily_treasury_yield_curve&field_tdr_date_value=all&page=", 1:27)
yields_raw = list()
for (i in seq_along(urls)) {
  print(i)
  req = GET(urls[i], user_agent("Mozilla/5.0"))
  yields_raw[[i]] = content(req) %>%
    html_element("table") %>%
    html_table()
  Sys.sleep(1L)
}
yields = rbindlist(yields_raw)
cols_keep = c("Date", colnames(yields)[11:ncol(yields)])
yields = yields[, ..cols_keep]
cols_num = colnames(yields)[2:ncol(yields)]
yields[, (cols_num) := lapply(.SD, as.numeric), .SDcols = cols_num]
yields[, Date := as.Date(Date, format = "%m/%d/%Y")]
yields = yields[, .(date = Date, y10 = `10 Yr`, y1 = `1 Yr`)]
yields = na.omit(yields)
yields[, ys := y10 / y1]

# plots
plot(as.xts.data.table(bondm))
plot(as.xts.data.table(equitym))
plot(as.xts.data.table(commoditym))
plot(as.xts.data.table(na.omit(yields[, .(date, ys)])))

# merge  all data for daily frequency
dt = Reduce(function(x, y) merge(x, y, by = "date", all.x = TRUE, all.y = FALSE),
            list(yields, equity, commodity, bond))
dt = na.omit(dt)

# merge  all data for monthly frequency
dt = Reduce(function(x, y) merge(x, y, by = "date", all.x = TRUE, all.y = FALSE),
            list(yields, equitym, commoditym, bondm))
dt = na.omit(dt)



# PREDICTORS --------------------------------------------------------------
# zscore function
zscore = function(x, signv = -1) {
  y = signv*roll_scale(x, 12*10, min_obs = 12*7)
  y = ifelse(y < -1, -1, y)
  y = ifelse(y > 1, 1, y)
  y
}

# momentum function
mom = function(x, n = 12) {
  x / shift(x, n) - 1
}

# calculates momentums for bond, equity and commodity
dt[, mom12bond := mom(close_bond)]
dt[, mom12equity := mom(close_equity)]
dt[, mom12commodity := mom(close_commodity)]

# bond trend variable
dt[!is.na(mom12bond), mom12bond_sign := 0]
dt[mom12bond >= 0, mom12bond_sign := 1]

# zscores for equity, commodity and yields
dt[, equity_zscore := zscore(mom12equity)]
dt[, commodity_zscore := zscore(mom12commodity)]

# zscore for yield spread
dt[, ys_zscore := zscore(ys, 1)]

# combined z score
dt[, combined_zscore := ys_zscore+equity_zscore+commodity_zscore+mom12bond_sign]

# keep only combined score
cz = na.omit(dt[, .(date, combined_zscore)])

# plot combined z score
plot(as.xts.data.table(cz[, .(date, combined_zscore)]))
# plot(as.xts.data.table(cz[1:1000, .(date, combined_zscore)]))
# plot(as.xts.data.table(cz[1000:2000, .(date, combined_zscore)]))
# plot(as.xts.data.table(cz[2000:3000, .(date, combined_zscore)]))
# plot(as.xts.data.table(cz[2000:4000, .(date, combined_zscore)]))
# plot(as.xts.data.table(dt[4000:nrow(dt), .(date, commodity_zscore)]))

# positions
cz[!is.na(combined_zscore), position := 0]
cz[shift(combined_zscore, 1) > 0, position := 1]

# save to Azure for backtesting
czqc = cz[, .(date, position)]
czqc[, date := as.character(date)]
blob_key = "0M4WRlV0/1b6b3ZpFKJvevg4xbC/gaNBcdtVZW+zOZcRi0ZLfOm1v/j2FZ4v+o8lycJLu1wVE6HT+ASt0DdAPQ=="
endpoint = "https://snpmarketdata.blob.core.windows.net/"
BLOBENDPOINT = storage_endpoint(endpoint, key=blob_key)
cont = storage_container(BLOBENDPOINT, "qc-backtest")
storage_write_csv(czqc, cont, paste0("bonds_alpha_month.csv"))

# benchmark
x = cz[bond, on = "date"]
x = na.omit(x)
x[, return := close_tlt / shift(close_tlt) - 1]
x[, strategy := return * shift(position)]
x = x[, .(date, benchmark = return, strategy)]
x = na.omit(x)
PerformanceAnalytics::charts.PerformanceSummary(as.xts.data.table(x))
