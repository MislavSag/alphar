library(data.table)
library(fmpcloudr)
library(httr)
library(rvest)
library(DBI)
source('C:/Users/Mislav/Documents/GitHub/alphar/R/import_data.R')


# set api token
APIKEY = "15cd5d0adf4bc6805a724b4417bbaafc"
fmpc_set_token(APIKEY)

x <- fmpc_price_intraday('AAPL', freq = '30min', startDate = Sys.Date() - 2)
x
tail(x)

x <- fmpc_price_intraday('A', freq = '30min', startDate = "2014-10-20", endDate = "2014-11-10")
x
tail(x)
plot(x$close)


base_url = "https://financialmodelingprep.com/api/v4"
symbol = "A"
multiply = 1
time = 'hour'
from = "2014-10-20"
to = "2014-11-10"
q = paste(symbol, multiply, time, from, to, sep = "/")
url <- paste0(base_url, "/historical-price-adjusted/", q)
req <- GET(url, query = list(apikey = APIKEY))
data <- content(req)
data <- rbindlist(data$results)
plot(as.POSIXct(data$formated), data$c, type = 'l')

fmpc_price_current('AAPL')


callbackURL = 'http://localhost'
consumerKey = 'J36UOLMHTNDRU5YSMY5EPDZJRY7VXMGS'

rameritrade::td_auth_loginURL(consumerKey, callbackURL)

library(QuantTools)

x <- get_finam_data("AAPL", "2010-01-01", to = "2010-02-01", period = "hour", local = FALSE)
