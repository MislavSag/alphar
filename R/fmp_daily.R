library(data.table)
library(ggplot2)
library(httr)
library(PerformanceAnalytics)


# Import daily FMP cloud data
prices = fread("F:/data/equity/daily_fmp_all.csv")

# Set keys
setkey(prices, "symbol")
setorder(prices, symbol, date)

# Remove duplicates
prices = unique(prices, by = c("symbol", "date"))

# Remove where symbol, date or close is missing
prices = na.omit(prices, cols = c("symbol", "date", "close"))
prices = prices[symbol != ""]

# Conver to numeric
cols = c("open", "high", "low", "close", "adjClose")
prices[, (cols) := lapply(.SD, as.numeric), .SDcols = cols]

# Check
prices[symbol == "AAPL"]
plot(as.xts.data.table(prices[symbol == "AAPL", .(date, close, adjClose)]))

# Adjust prices for splits/dividends
prices[, adj_rate := adjClose / close]
prices[, `:=`(
  open = open * adj_rate,
  high = high * adj_rate,
  low = low * adj_rate
)]
setnames(prices, "close", "close_raw")
setnames(prices, "adjClose", "close")
prices[, adj_rate := NULL]
setcolorder(prices, c("symbol", "date", "open", "high", "low", "close", "volume"))

# Calculate returns
prices[, returns := close / shift(close, 1) - 1, by = symbol]

# Remove missing values
prices = na.omit(prices)

# Remove symbols with insufficient data
min_obs = 66
remove_symbols = prices[, .N, by = symbol][N < min_obs, symbol]
if (length(remove_symbols) > 0) {
  prices = prices[!.(remove_symbols)]
}

# Add profiles data from FMP cloud
tmp_file = tempfile(fileext = ".csv")
profile = lapply(0:10, function(p) {
  GET(
    "https://financialmodelingprep.com/stable/profile-bulk",
    query = list(part = p, apikey = Sys.getenv("FMP-SNP")),
    write_disk(tmp_file, overwrite = TRUE)
  )
  Sys.sleep(1L)
  dt = fread(tmp_file)
  if (nrow(dt) == 0) return(NULL)
  dt
})
profile = rbindlist(profile, fill = TRUE)
profile = profile[!is.na(symbol)]

# Check difference between prices and profile
symbols_prices  = prices[, unique(symbol)]
profile_symbols = profile[, unique(symbol)]
symbols_prices[!(symbols_prices %in% profile_symbols)]
profile_symbols[!(profile_symbols %in% symbols_prices)]
keep_symbols = symbols_prices[symbols_prices %in% profile_symbols]
prices = prices[.(keep_symbols)]
profile = profile[symbol %in% symbols_prices]

# Analyse profile data
na.omit(profile[, .N, by = country][order(-N)]) |>
  ggplot(aes(x = reorder(country, -N), y = N)) +
  geom_bar(stat = "identity") +
  labs(title = "Number of symbols by country in FMP profile data")
profile[country == "SR"]
na.omit(profile[, .N, by = exchange][order(-N)]) |>
  ggplot(aes(x = reorder(exchange, -N), y = N)) +
  geom_bar(stat = "identity") +
  labs(title = "Number of symbols by exchange in FMP profile data")
na.omit(profile[, .N, by = currency][order(-N)]) |>
  ggplot(aes(x = reorder(currency, -N), y = N)) +
  geom_bar(stat = "identity") +
  labs(title = "Number of symbols by currency in FMP profile data")

# Keep symbols with enough number of observations per country
country_keep = na.omit(profile[, .N, by = country][order(-N)])[N > 50]
profile_sample = profile[country %in% country_keep$country]
profile_sample = profile_sample[country != ""]
prices = prices[.(profile_sample$symbol)]

# Downsample to monthly data
pricesm = prices[, .(
  open      = data.table::first(open),
  close     = data.table::last(close),
  close_raw = data.table::last(close_raw),
  volume    = sum(volume)
), by = .(symbol, month = data.table::yearmon(date))]

# Create target variable
pricesm[, target := close / open - 1]
pricesm[, target := shift(target, 1, type = "lead"), by = symbol]
pricesm = na.omit(pricesm)
setkey(pricesm, symbol)
setorder(pricesm, symbol, month)
pricesm[, mom12 := shift(close, 24) / shift(close, 1) - 1, by = symbol]
pricesm = na.omit(pricesm)

# Analyse momentum strategy by including more and more countries
countries_symbols = na.omit(profile_sample[, .N, by = country][order(-N)])
countries_symbols = profile[country %in% head(countries_symbols$country, 1)]
countries_symbols = countries_symbols[isFund == FALSE & isEtf == FALSE]
# exchanges = c("NASDAQ", "NYSE", "AMEX")
# countries_symbols = countries_symbols[exchange %in% exchanges]
countries_symbols = countries_symbols[, symbol]

dt = pricesm[.(countries_symbols)]
dt = dt[month > 2020]
dt = dt[volume > 1000 & close > 0.2]
dt = dt[target < 10 & mom12 < 100]
setorder(dt, month, -mom12)
dt = na.omit(dt)
dt = dt[, head(.SD, 30), by = month]
dt[, weight := 1 / length(close), by = month]
portfolio = dt[, .(ret = sum(weight * target)), by = month]
setorder(portfolio, month)
portfolio_xts = as.xts.data.table(portfolio[, .(date = zoo::as.Date.yearmon(month), ret)])
charts.PerformanceSummary(portfolio_xts)
SharpeRatio.annualized(portfolio_xts, scale = 12)

# Last
dt[month == data.table::last(month)]
