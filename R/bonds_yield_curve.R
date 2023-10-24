library(data.table)
library(AzureStor)
library(httr)
library(rvest)
library(roll)
library(ggplot2)



# SET UP ------------------------------------------------------------------
# globals
PATHOHLCV = "F:/lean_root/data/equity/usa/daily"

# # help vars
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
treasury = get_symbol("shv")
bondshort = get_symbol("vgsh") # vgsh, scho, spts
bond = get_symbol("tlt")
equity = get_symbol("spy")
commodity = get_symbol("dbc") # # DBC (GSG, USCI)
tips = get_symbol("tip") # # DBC (GSG, USCI)
gold = get_symbol("gld")

# monthly index data
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
yields = yields[, .(date = Date,
                    y30 = `30 Yr`,
                    y20 = `20 Yr`,
                    y10 = `10 Yr`,
                    y1 = `1 Yr`,
                    m6 = `6 Mo`)]
yields[, ys := y10 / m6]

# plots
plot(as.xts.data.table(treasury))
plot(as.xts.data.table(equitym))
plot(as.xts.data.table(commoditym))
plot(as.xts.data.table(yields[, .(date, y30, y20, y10, y1)]),
     legend.loc = "bottomleft")
plot(as.xts.data.table(na.omit(yields[, .(date, ys)])),
     main = "Yield curve - 10y / 1y")
plot(as.xts.data.table(na.omit(yields[, .(date, ys)])),
     ylim = c(0, 5),
     main = "Yield curve - 10y / 1y")
plot(as.xts.data.table(gold))

# merge  all data for daily frequency
dt = Reduce(function(x, y) merge(x, y, by = "date", all.x = TRUE, all.y = FALSE),
            list(yields, equity, bond, bondshort, treasury, gold))



# BOND PRICES ACROSS YILED CURVE ------------------------------------------
# plot
dt_plot = dt[, .(date, ys, close_tlt, close_shv, close_vgsh, close_gld)]
dt_plot[, slope := ifelse(ys > 1.1, 1, 0)]
dt_plot[, bond_return := close_tlt / shift(close_tlt) - 1]
ggplot(dt_plot, aes(x = date, y = close_tlt, color = factor(slope))) +
  geom_line(aes(group = 1))
ggplot(dt_plot, aes(x = date, y = close_vgsh, color = factor(slope))) +
  geom_line(aes(group = 1))
ggplot(dt_plot, aes(x = date, y = close_shv, color = factor(slope))) +
  geom_line(aes(group = 1))
ggplot(dt_plot, aes(x = date, y = close_gld, color = factor(slope))) +
  geom_line(aes(group = 1))


dt_plot[, mean(bond_return, na.rm = TRUE), by = slope]

