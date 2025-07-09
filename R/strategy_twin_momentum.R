library(fastverse)
library(finutils)
library(arrow)
library(TTR)
library(dplyr)
library(lubridate)
library(ggplot2)
library(PerformanceAnalytics)
library(quantreg)
library(corrplot)
library(DescTools)


# FUNDAMENTALS ------------------------------------------------------------
# Import fundamental factors
fundamentals = read_parquet(
  "F:/data/equity/us/predictors_daily/factors/fundamental_factors.parquet",
)

# Clean fundamentals
fundamentals[, 1:10]
fundamentals[, acceptedDate := as.Date(acceptedDate)]
setnames(fundamentals, "date", "fundamental_date")
fundamentals[, date := acceptedDate]
fundamentals[fillingDate > acceptedDate, date := fillingDate]
fundamentals = unique(fundamentals, by = c("symbol", "date"))

# Choose columns we need
fundamentals[, 1:10]
colnames(fundamentals)[grepl("profit", colnames(fundamentals), ignore.case = TRUE)]
fundamental_predictors = c(
  "symbol", "date",
  "returnOnEquity",
  "returnOnAssets",
  "EPS_ttm",
  "payoutRatio",
  # "grossProfit_ttm",
  "freeCashFlowYield",
  "operatingProfitMargin"
)
fund = fundamentals[, ..fundamental_predictors]

# Remove missing values
fund = na.omit(fund)

# At least 8 observations by symbol
keep_symbols = fund[, .N, by = symbol][N > 8, symbol]
fund = fund[symbol %in% keep_symbols]

# Histogram of predictors
fund[, .(freeCashFlowYield)] |>
  ggplot(aes(x = freeCashFlowYield)) + geom_histogram()
summary(fund$freeCashFlowYield)
fund[, DescTools::Winsorize(freeCashFlowYield, val = quantile(freeCashFlowYield, c(0.05, 0.95))), by = data.table::yearmon(date)] |>
  ggplot(aes(x = V1)) + geom_histogram()

# Winsorize predictions by month
predictors = setdiff(colnames(fund), c("symbol", "date"))
fund[, (predictors) := lapply(.SD, function(x) Winsorize(x, quantile(x, c(0.05, 0.95)))),
     by = data.table::yearmon(date),
     .SDcols = predictors]
melt(fund, id.vars = c("symbol", "date")) |>
  ggplot(aes(x = value)) + geom_histogram() + facet_wrap(~variable, scales = "free")

# Uniformization
ggplot(fund, aes(x = freeCashFlowYield)) + geom_histogram()
ggplot(fund, aes(x = ecdf(freeCashFlowYield)(freeCashFlowYield))) + geom_histogram()
fund[, (predictors) := lapply(.SD, function(x) ecdf(x)(x)), .SDcols = predictors]

# Create lags
# lags = c(1, 2, 4, 8)
# cols = setdiff(colnames(fund), c("symbol", "date"))
# cols_lags = as.vector(sapply(cols, function(x) paste0(x, lags)))
# fund[, (cols_lags) := shift(.SD, lags, give.names = TRUE), by = symbol, .SDcols = cols]

# Create SMA of predictors
widths = c(1, 2, 4, 8)
cols_lags = as.vector(sapply(predictors, function(x) paste0(x, widths)))
fund[, (cols_lags) := unlist(lapply(.SD, function(x) lapply(widths, function(w) SMA(x, w))), recursive = FALSE),
     by = symbol, .SDcols = predictors]

# Checks
fund[, all(round(freeCashFlowYield, 1) == round(freeCashFlowYield1, 1), na.rm = TRUE)]
fund[, round(freeCashFlowYield, 1) == round(freeCashFlowYield1, 1)]
fund[, .(round(freeCashFlowYield, 3), round(freeCashFlowYield1, 3))]
fund[round(freeCashFlowYield, 1) != round(freeCashFlowYield1, 1),
     .(round(freeCashFlowYield, 3), round(freeCashFlowYield1, 3))]

# Remove missing values
fund = na.omit(fund)

# Create monthly fund data
month_seq = fund[, .(date, month = ceiling_date(date, "month"))]
head(month_seq)
month_seq = month_seq[month > as.Date("2000-01-01"), sort(unique(month))]
symbol_seq = fund[, unique(symbol)]
length(month_seq) * length(symbol_seq)
meta_fund = CJ(month_seq, symbol_seq)
setnames(meta_fund, c("date", "symbol"))
anyDuplicated(meta_fund)
fund[, date_fund := date]
meta_fund[, date_meta := date]
fund_month = fund[meta_fund, on = c("symbol", "date"), roll = Inf]
setorder(fund_month, date, symbol)

# Clean fund meta
fund_month = na.omit(fund_month)
fund_month[, delay := difftime(date_meta, date_fund, units = "day")]
fund_month[, all(delay >= 0)]
ggplot(fund_month, aes(x = delay)) + geom_histogram()
fund_month[, mean(delay)]
fund_month[, median(delay)]
fund_month = fund_month[delay > (6*30)]

# Number of observations per month
fund_month[, .N, by = date] |>
  ggplot(aes(x = date, y = N)) + geom_line()


# PRICE DATA --------------------------------------------------------------
PRICE_SOURCE = "qc"
if (PRICE_SOURCE == "fmp") {
  # Import FMP cloud prices
  fread("F:/data/equity/daily_fmp_all.csv", nrows = 10)
  prices = read_csv_arrow("F:/data/equity/daily_fmp_all.csv") |>
    filter(symbol %in% fund[, unique(symbol)]) |>
    collect()
  setDT(prices)
  anyDuplicated(prices[, .(symbol, date)])
} else {
  # Import QC prices
  prices = qc_daily(
    file_path = "F:/lean/data/stocks_daily.csv",
    min_obs = 252,
    add_dv_rank = TRUE,
    add_day_of_month = FALSE
  )
  prices[, symbol := toupper(gsub("\\.\\d+", "", symbol))]
  setkey(prices, "symbol")
  prices = prices[.(fund[, unique(symbol)])]
}

# Downsample prices to monthly data
prices[, month := ceiling_date(date, "month")]
pricesm = prices[, .(
  date = data.table::last(date),
  open = data.table::first(open),
  high = max(high),
  low = min(low),
  close_raw = data.table::last(close_raw),
  close = data.table::last(close),
  volume = sum(volume)
), by = .(symbol, month)]

# Create target variable
setorder(pricesm, symbol, date)
pricesm[, target := close/open - 1, by = symbol]
pricesm[target == 0]
pricesm[, target := shift(target, type = "lead"), by = symbol]

# Remove missing targets
pricesm = na.omit(pricesm)

# Remove target outliers
ggplot(pricesm, aes(x = target)) + geom_histogram()
nrow(pricesm[target > 3]) / nrow(pricesm) * 100
pricesm = pricesm[target < 3]
ggplot(pricesm, aes(x = target)) + geom_histogram(bins = 100)

# Number of observations per month
pricesm[, .N, by = month] |>
  ggplot(aes(x = month, y = N)) + geom_line() +
  scale_x_date(date_breaks = "1 year", date_labels = "%y")

# Check observations with target 0
setorder(pricesm, symbol, date)
pricesm[target == 0]


# MERGE -------------------------------------------------------------------
# Merge pricesm and fund_month
dt = merge(fund_month[, .SD, .SDcols = -"date"],
           pricesm[, .SD, .SDcols = -"date"],
           by.x = c("symbol", "date_meta"), by.y = c("symbol", "month"),
           all.x = TRUE, all.y = FALSE)

# Remove columns we dont need
setnames(dt, "date_meta", "date")
cols_remove = c("date_fund", "delay", "open", "low", "high", "close", "volume",
                "adj_close")
dt[, (cols_remove) := NULL]

# Sort for cross section lm
setorder(dt, date, symbol)

# Remove all NA
dt = na.omit(dt)

# Number of observations per month
dt[, .N, by = date] |>
  ggplot(aes(x = date, y = N)) + geom_line() +
  scale_x_date(date_breaks = "1 year", date_labels = "%y")

# Keep only data after 2005
dt = dt[date > as.Date("2010-01-01")]

# Correltion analysis
predictors = setdiff(colnames(dt), c(fundamental_predictors, "target", "close_raw"))
corr = cor(dt[, ..predictors])
corrplot(corr, method = "number")

# Remove variables with corelation greater than 0.9
cor_remove = as.data.table(as.table(corr))[abs(N) > 0.9][V1 != V2]
if (nrow(cor_remove) > 0) {
  keep_ = c()
  remove_ = c()
  for (i in 1:nrow(cor_remove)) {
    if (cor_remove[i, V1] %in% keep_) {
      remove_ = c(remove_, cor_remove[i, V2])
    } else if (cor_remove[i, V2] %in% keep_) {
      remove_ = c(remove_, cor_remove[i, V1])
    } else {
      keep_ = c(keep_, cor_remove[i, V1])
      remove_ = c(remove_, cor_remove[i, V2])
    }
  }
  remove_ = unique(remove_)
  dt[, (remove_) := NULL]
  predictors = setdiff(colnames(dt), c(fundamental_predictors, "target", "close_raw"))
  corrplot(cor(dt[, ..predictors]), method = "number")
}


# CROSS SECTION LM --------------------------------------------------------
# Cross section lm by month
formula_ = paste("target ~", paste(predictors, collapse = " + "))
results = dt[, .(lmo = list(lm(formula_, data = .SD))), by = date]
# results = dt[, .(lmo = list(rq(formula_, data = .SD, tau = 0.9))), by = date]

# Get predictions
predictions = list()
dates = results[, date]
for(i in 1:nrow(results)) {
  # i = 1
  print(i)
  d = dates[i]
  dt_ = dt[d == (date %m-% months(1))]
  pred = predict(results[date == d, lmo][[1]],
                 newdata = dt_[, .SD, .SDcols = predictors])
  pred = cbind(dt_, pred)
  predictions[[i]] = pred
}
dim(predictions[[1]])
dim(predictions[[100]])
dim(predictions[[180]])
predictions = rbindlist(predictions)


# RESEARCH ----------------------------------------------------------------
# Extract data we need for research
deciles = 10
res = predictions[, .(symbol, date, pred, target)]
res[, decile := dplyr::ntile(pred, deciles), by = date]
res[date == as.Date("2025-03-01")][order(pred)]
# res = res[date > as.Date("2020-01-01")]
res[, .(ret = mean(target)), by = decile][order(decile)] |>
  ggplot(aes(x = decile, y = ret)) + geom_bar(stat = "identity") +
  scale_x_continuous(breaks = seq(1, deciles, 1))
res[, .(ret = median(target)), by = decile][order(decile)] |>
  ggplot(aes(x = decile, y = ret)) + geom_bar(stat = "identity") +
  scale_x_continuous(breaks = seq(1, deciles, 1))

# Mean returns by months
res[, .(ret = mean(target)), by = .(y = data.table::year(date), decile)][order(y, decile)] |>
  ggplot(aes(x = decile, y = ret)) + geom_bar(stat = "identity") +
  facet_wrap(~y, scales = "free") +
  scale_x_continuous(breaks = seq(1, deciles, 1))


# BACKTEST ----------------------------------------------------------------
# Backtest
back = res[decile == 10]
back[, weight := 1 / nrow(.SD), by = date]
portfolio = back[, .(ret = sum(weight * target)), by = date]
setorder(portfolio, date)
charts.PerformanceSummary(as.xts.data.table(portfolio))

# Short
back = res[decile %in% 1]
back[, weight := 1 / nrow(.SD), by = date]
portfolio = back[, .(ret = sum(weight * -target)), by = date]
setorder(portfolio, date)
charts.PerformanceSummary(as.xts.data.table(portfolio))
charts.PerformanceSummary(as.xts.data.table(portfolio)["2021/"])
BetaCoVariance(as.xts.data.table(portfolio), )

# # Merge features and fundamental data
# features[, date_features := date]
# features = fundamentals[features, on = c("symbol", "acceptedDate" = "date_features"), roll = Inf]
# features[, .(symbol, date, date_event, acceptedDate, acceptedDateTime, receivablesGrowth)]
#
# # remove unnecessary columns
# features[, `:=`(period = NULL, link = NULL, finalLink = NULL, acceptedDate = NULL,
#                 reportedCurrency = NULL, cik = NULL, calendarYear = NULL,
#                 fillingDate = NULL, fundamental_date = NULL)]
#
# # convert char features to numeric features
# char_cols = features[, colnames(.SD), .SDcols = is.character]
# char_cols = setdiff(char_cols, c("symbol", "time", "right_time", "industry", "sector"))
# features[, (char_cols) := lapply(.SD, as.numeric), .SDcols = char_cols]
#
#
# prices = qc_daily(
#   file_path = "F:/lean/data/stocks_daily.csv",
#   min_obs = 252,
#   add_dv_rank = TRUE,
#   add_day_of_month = FALSE
# )
#
#
