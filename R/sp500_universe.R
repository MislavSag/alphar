library(findata)
library(httr)
library(data.table)


fmp = FMP$new()

sp500 = fmp$get_sp500_symbols()

sp500 = fmp$get_sp500_constituent()


url = paste0("https://financialmodelingprep.com/stable/profile-bulk?part=2&apikey=",
             fmp$api_key)
GET(url, write_disk("profiles2.csv"))

p0 = fread("profiles0.csv")
p1 = fread("profiles1.csv")
p2 = fread("profiles2.csv")

p = rbind(p0, p1, p2)

dt = p[sp500[, .(symbol = unique(symbol))], on = "symbol"]
dt[, sum(is.na(price))]

# save
fwrite(
  dt[, .(symbol, isin, price, marketCap, companyName, cik, cusip, exchange, country)],
  "F:/temp/sp500_meta.csv"
)

