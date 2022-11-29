library(AzureStor)
library(data.table)
library(pins)
library(writexl)
library(forecast)
library(ggplot2)
library(mlr3verse)
library(mlr3cluster)
library(DescTools)
library(future.apply)
library(httr)
library(roll)
library(readr)
library(PerformanceAnalytics)
library(findata)
library(lubridate)
library(patchwork)
library(fredr)
library(gausscov)
library(runner)
library(rollRegres)


# set up
endpoint <- storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), key=Sys.getenv("BLOB-KEY"))
board_fundamentals <- board_azure(
  container = storage_container(endpoint, "fundamentals"), # HERE CHANGE WHEN MINUTE DATA !!!
  path = "",
  n_processes = 10,
  versioned = FALSE,
  cache = NULL
)
CACHEDIR = "D:/findata" # here define your local folder wher data will be saved
board_prices <- board_azure(
  container = storage_container(endpoint, "fmpcloud-daily"),
  path = "",
  n_processes = 6L,
  versioned = FALSE,
  cache = CACHEDIR
)

# utils
# date segments
DOTCOM <- c("2000-01-01", "2002-01-01")
GFC <- c("2007-01-01", "2010-01-01")
AFTERGFCBULL <- c("2010-01-01", "2015-01-01")
COVID <- c("2020-01-01", "2021-06-01")
AFTER_COVID <- c("2021-06-01", "2022-01-01")
NEW <- c("2022-01-01", "2022-03-15")
fred_api_key <- "fb7e8cbac4b84762980f507906176c3c"
fredr_set_key(fred_api_key)


# IMPORT DATA -------------------------------------------------------------
# market data
daily_prices_files <- pin_list(board_prices)
files_new <- setdiff(daily_prices_files, list.files(CACHEDIR))
lapply(files_new, pin_download, board = board_prices)
files_local <- vapply(file.path(CACHEDIR, daily_prices_files),
                      list.files, recursive = TRUE, pattern = "\\.csv", full.names = TRUE,
                      FUN.VALUE = character(1))
# plan(multicore(workers = 4L))
prices_l <- lapply(files_local, fread)
prices <- prices_l[vapply(prices_l, function(x) nrow(x) > 0, FUN.VALUE = logical(1))]
prices <- rbindlist(prices)

# clean daily prices
prices <- prices[open > 0 & high > 0 & low > 0 & close > 0 & adjClose > 0] # remove rows with zero and negative prices
prices <- unique(prices, by = c("symbol", "date")) #TODO SEND MAIL TO FMP CLOUD ! WHY WE HAVE DUPLICATED ROWS, DIFFERENT ADJUSTED CLOSE PRICES
setorder(prices, "symbol", "date")
prices[, returns := adjClose   / data.table::shift(adjClose) - 1, by = symbol] # calculate returns
adjust_cols <- c("open", "high", "low")
prices[, (adjust_cols) := lapply(.SD, function(x) x * (adjClose / close)), .SDcols = adjust_cols] # adjust open, high and low prices
prices[, close_raw := close]
prices[, close := adjClose]
prices <- na.omit(prices[, .(symbol, date, close_raw, open, high, low, close, volume, returns)])
prices <- unique(prices, by = c("symbol", "date")) # remove duplicates if they exists

# TODO: simfin+daily data

# download only USA stocks
url <- modify_url("https://financialmodelingprep.com/", path = "api/v3/available-traded/list",
                  query = list(apikey = Sys.getenv("APIKEY-FMPCLOUD") ))
stocks <- rbindlist(content(GET(url)))
usa_symbols <- stocks[exchangeShortName %in% c("AMEX", "NASDAQ", "NYSE", "OTC")]

# remove ETF's
usa_symbols <- usa_symbols[!grep("etf|spdr", name, ignore.case = TRUE)]

# SPX symbols
utils_data <- UtilsData$new()
sp500_symobls_by_date <- utils_data$sp500_history()

############# TEST ################
# x <- sp500_symobls_by_date[date %between% c("2001-12-22", "2002-01-01")]
# unique(x$symbol)
# unique(x$symbol)[!(unique(x$symbol) %in% unique(usa_symbols$symbol))]
# unique(x$symbol)[!(unique(x$symbol) %in% unique(stocks$symbol))]
#
# fmp <- FMP$new()
# fmp$get_daily("AABA", "2001-12-01", "2002-01-01")
# y <- fmp$get_intraday_equities(symbol = "AABA", from = "2009-12-01", to = "2020-01-01")

############# TEST ################

# get profiles for all symbols
# url <- "https://financialmodelingprep.com/api/v3/profile/"
# profiles_l <- lapply(unique(usa_symbols$symbol), function(x) {
#   content(GET(paste0(url, x), query = list(apikey = Sys.getenv("APIKEY-FMPCLOUD"))))
# })
# profiles <- rbindlist(profiles_l)
# filter_symbols <- profiles[usa_symbols, on = c(symbol = symbol)]
# filter_symbols # non etf

# keep usa, non-etf
prices_usa <- prices[symbol %in% usa_symbols$symbol]
prices_usa[, `:=`(year = year(date), month = month(date))] # help columns

# SPY data
spy <- prices[symbol == "SPY", .(date, close, returns)]
bonds_hy_symbols <- usa_symbols[grep("bond", name, ignore.case = TRUE)]
bonds_hy <- prices[symbol %in% bonds_hy_symbols$symbol]
bonds_hy[date < as.Date("2002-01-01"), unique(symbol)]

# keep SP500 symbols
# prices_usa <- merge(prices_usa, sp500_symbols[, index := 1], by = c("symbol", "date"), all.x = TRUE, all.y = FALSE)
# prices_usa <- prices_usa[index == 1]

##### ADD THIS TO FINDATA #####
get_market_capitalization <- function(ticker, limit = 6000) {
  url = paste0("https://financialmodelingprep.com/api/v3/historical-market-capitalization/", ticker)
  p <- GET(url, query = list(limit = limit, apikey = "15cd5d0adf4bc6805a724b4417bbaafc"))
  data_ <- rbindlist(content(p))
}
##### ADD THIS TO FINDATA #####
market_cap_data_l <- lapply(unique(prices_usa$symbol), get_market_capitalization, limit = 6000)
market_cap_data <- rbindlist(market_cap_data_l)
market_cap_data[, date := as.Date(date)]
prices_usa <- merge(prices_usa, market_cap_data, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE)

# fundamental data
pin_list(board_fundamentals)
key_metrics <- as.data.table(pin_read(board_fundamentals, "key-metrics"))
key_metrics[, price := pfcfRatio * freeCashFlowPerShare]
key_metrics[, `:=`(period = NULL, date = as.Date(as.character(date)), symbol = as.character(symbol))]
fg <- as.data.table(pin_read(board_fundamentals, "financial-growth-growth"))
fg[, `:=`(period = NULL, date = as.Date(as.character(date)), symbol = as.character(symbol))]
is <- as.data.table(pin_read(board_fundamentals, "income-statements"))
is[, `:=`(date = as.Date(as.character(date)), symbol = as.character(symbol), fillingDate = as.Date(as.character(fillingDate)))]
bs <- as.data.table(pin_read(board_fundamentals, "balance-sheet-statement"))
bs[, `:=`(period = NULL, date = as.Date(as.character(date)), symbol = as.character(symbol),
          fillingDate = as.Date(as.character(fillingDate)))]
cf <- as.data.table(pin_read(board_fundamentals, "cash-flow-statement"))
cf[, `:=`(period = NULL, date = as.Date(as.character(date)), symbol = as.character(symbol),
          fillingDate = as.Date(as.character(fillingDate)))]
ratios <- as.data.table(pin_read(board_fundamentals, "ratios"))
ratios[, `:=`(period = NULL, date = as.Date(as.character(date)), symbol = as.character(symbol))]
# merge fundamntal datasets
fundamentals <- Reduce(function(x, y) merge(x, y, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE),
                       list(is, bs, cf, key_metrics, fg, ratios))
setorderv(fundamentals, c("symbol", "fillingDate"))
# writexl::write_xlsx(fundamentals, "~/fundamentals.xlsx")



# COARSE UNIVERSE SELECTION ------------------------------------------------------
# number of symbols by year-month
symbols_by_year_month <- prices_usa[, .(n_symbols = length(unique(symbol))), by = .(year, month)]
symbols_by_year_month[, date := as.Date(paste(year, month, "01", sep = "-"), format = "%Y-%m-%d")]
ggplot(symbols_by_year_month, aes(date, n_symbols)) + geom_line()

# change real month to lead month. we pretend we do analysis in lead month because
# this is the month when we make universe selection decision

# remove where raw close price < 5$
min_price = 5
prices_usa_penny_removed <- copy(prices_usa)
prices_usa_penny_removed[, price_threshold := any(close_raw > min_price), by = c("symbol", "year", "month")]
prices_usa_penny_removed <- prices_usa_penny_removed[price_threshold == TRUE]
nrow(prices_usa)
nrow(prices_usa_penny_removed)
prices_usa_penny_removed[, n_obs_pet_month := .N, by = c("symbol", "year", "month")]
prices_usa_penny_removed <- prices_usa_penny_removed[n_obs_pet_month > 15]

# keep only stocks with positive month rturn on the begining of the month
# help_dt = unique(prices_usa_penny_removed[, .(symbol, year, month)])
# help_dt[, month_lead := shift(month, 1, type = "lead"), by=.(symbol, year)]
# help_dt[month == 1 & is.na(month_lead), month_lead := 12]
# prices_usa_penny_removed[help_dt, on=.(symbol, year, month), month_lead := i.month_lead, by=.EACHI ]
# head(prices_usa_penny_removed, 50)
# head(prices_usa_penny_removed, 10)

# keep most liquid
n_most_liquid <- 500
prices_usa_penny_removed[, volume_sum := sum(volume, na.rm = TRUE), by = .(symbol, year, month)]
most_liquid <- unique(prices_usa_penny_removed[, .(symbol, year, month, volume_sum)])
most_liquid <- most_liquid[, .SD[volume_sum %in% tail(sort(unique(volume_sum)), n_most_liquid)], by = .(year, month)]
prices_most_liquid <- most_liquid[, `:=`(index = 1, volume_sum = NULL)][prices_usa, on = .(year = year, month = month, symbol = symbol)]
prices_most_liquid <- prices_most_liquid[index == 1]
prices_most_liquid[, index := NULL]
setorderv(prices_most_liquid, c("year", "month", "symbol"))

# keep only stocks with positive month rturn on the begining of the month
# prices_most_liquid[, month_return := Return.cumulative(returns), by = c("symbol", "year", "month")]
# prices_most_liquid <- prices_most_liquid[month_return > 0]

# calculate beta
# setorderv(prices_most_liquid, c("symbol", "date"))
# prices_most_liquid[, rolling_beta := RollingWindow::RollingBeta(returns,
#                                                                 spy_[date_ %in% date, returns],
#                                                                 window = 2,
#                                                                 expanding = TRUE), by = symbol]

# rolling vol
prices_most_liquid[, vol_monthly := roll::roll_sd(returns, length(returns), min_obs = 1L), by = symbol]
prices_most_liquid[, vol_monthly := data.table::last(vol_monthly, 1L), by = .(symbol, year, month)]

# save for Nebojsa
# sample_ <- prices_most_liquid[, tail(.SD, 1), by = c("symbol", "year", "month")]
# sample_[symbol == "FL"]
# fwrite(sample_, "Ndata.csv")
# writexl::write_xlsx(sample_, "Ndata.xlsx")

# keep least volatility
n_least_volatile <- 250
least_volatile <- unique(prices_most_liquid[, .(symbol, year, month, vol_monthly)])
least_volatile <- least_volatile[, .SD[vol_monthly %in% head(sort(unique(vol_monthly)), n_least_volatile)], by = .(year, month)]
prices_most_liquid_least_vol <- least_volatile[, `:=`(index = 1, vol_monthly = NULL)][
  prices_most_liquid, on = .(year = year, month = month, symbol = symbol)
]
prices_most_liquid_least_vol <- prices_most_liquid_least_vol[index == 1]
prices_most_liquid_least_vol[, index := NULL]
setorderv(prices_most_liquid_least_vol, c("year", "month", "symbol"))

# inspect
prices_most_liquid_least_vol[year == 2022 & month == 1, unique(symbol)]



# FINE FUNDAMENTAL FILTERING  -------------------------------------------------------------
# choose fundamnetal features
features_fund <- fundamentals[, .(symbol, date, fillingDate, fiveYRevenueGrowthPerShare,
                                  fiveYDividendperShareGrowthPerShare, payoutRatio.x,
                                  returnOnCapitalEmployed, freeCashFlowYield)]
features_fund[, filling_date := fillingDate]
setnames(features_fund, "date", "date_fund")
features_fund <- features_fund[, payoutratio_5y_mean := frollmean(payoutRatio.x, n = 4 * 5, na.rm = TRUE), by = "symbol"]
features_fund <- features_fund[, returnOnCapitalEmployed_5y_mean := frollmean(returnOnCapitalEmployed, n = 4 * 5, na.rm = TRUE), by = "symbol"]
features_fund <- features_fund[, freeCashFlowYield_5y_mean := frollmean(freeCashFlowYield, n = 4 * 5, na.rm = TRUE), by = "symbol"]
# features <- features[, earningsYield_10Y_mean := frollmean(earningsYield, n = 4 * 10, na.rm = TRUE), by = "symbol"]
features_fund[, `:=`(payoutRatio.x = NULL, returnOnCapitalEmployed = NULL, freeCashFlowYield = NULL)]
features_fund[symbol == "MSFT" & date_fund == as.Date("2021-12-31")]
features_fund[date_fund == as.Date("2021-09-30")]

# create dataset
DT <- features_fund[prices_most_liquid_least_vol, on = .(symbol = symbol, fillingDate = date), roll = +Inf]
DT[fillingDate == as.Date("2021-09-30")]
DT <- na.omit(DT, cols = c("fiveYRevenueGrowthPerShare", "fiveYDividendperShareGrowthPerShare", "freeCashFlowYield_5y_mean"))
DT[fillingDate == as.Date("2021-09-30")]

# 1) remove stocks with fundamnetals below thresholds
DT_sample <- DT[fiveYRevenueGrowthPerShare >= 0.05]
DT_sample <- DT_sample[fiveYDividendperShareGrowthPerShare >= 0.02]
DT_sample <- DT_sample[payoutratio_5y_mean <= 0.7]
DT_sample <- DT_sample[returnOnCapitalEmployed_5y_mean >= 0.025]
DT_sample <- DT_sample[freeCashFlowYield_5y_mean >= 0.01]

# 2) cluster analysis
data_ <- DT[fillingDate == as.Date("2021-12-31"), .(symbol, fillingDate, fiveYRevenueGrowthPerShare,
                                                    fiveYDividendperShareGrowthPerShare, freeCashFlowYield_5y_mean)]
cols <- colnames(data_)[3:ncol(data_)]
summary(data_[, ..cols])
data_[, (cols) := lapply(.SD, function(x) {Winsorize(as.numeric(x), probs = c(0.05, 0.95), na.rm = TRUE)}), .SDcols = cols]
summary(data_[, ..cols])
data_task <- TaskClust$new(id = "fundamentals", backend = data_[, 3:ncol(data_)])
design = benchmark_grid(
  tasks = data_task,
  learners = list(
    lrn("clust.kmeans", centers = 3L),
    lrn("clust.pam", k = 3L),
    lrn("clust.cmeans", centers = 3L),
    lrn("clust.ap"),
    # lrn("clust.SimpleKMeans"),
    lrn("clust.agnes", k = 3L)),
  resamplings = rsmp("insample"))
print(design)

# execute benchmark
bmr = benchmark(design)

# define measure
measures = list(msr("clust.wss"), msr("clust.silhouette"))
bmr$aggregate(measures)

# predictions
task = TaskClust$new(id = "fundamentals_pred", backend = data_[, 3:ncol(data_)])
learner = lrn("clust.pam", k = 3L)
learner$train(task)
preds = learner$predict(task = task)
autoplot(preds, task, type = "pca")
autoplot(preds, task, type = "pca", frame = TRUE, frame.type = "norm")

# extract class assigments
nrow(data_) == length(preds$partition)
data_$symbol[which(preds$partition == 1)]
data_$symbol[which(preds$partition == 2)]
data_$symbol[which(preds$partition == 3)]


# QC BACKTEST -------------------------------------------------------------
# inspect
DT_sample[year == 2022 & month == 2, unique(symbol)]

# shift dates to lead month
qc_data <- DT_sample[, .(fillingDate, symbol)]
qc_data[, month_lead := ceiling_date(fillingDate, unit = "month")]
qc_data[, month_last := ceiling_date(month_lead, unit = "month") - 1]
qc_data <- qc_data[ , .(symbol = symbol, month = seq(month_lead, month_last, by = "day")), by = 1:nrow(qc_data)]
qc_data <- qc_data[, .(month, symbol)]
setnames(qc_data, "month", "date")
qc_data <- unique(qc_data)

# save universe on blob azure
qc_data[, date := format.Date(date, format = "%Y%m%d")]
qc_data_save <- qc_data[, .(symbol = paste0(unlist(symbol), collapse = ",")), by = date]
storage_write_csv(qc_data_save, storage_container(endpoint, "qc-backtest"), "universe.csv", col_names = FALSE)


# MARKET RISK -------------------------------------------------------------
# mcap (micro, mid, large) 52w high / 52w low
mcap_52 <- prices_usa[, .(symbol, date, high, low, close, marketCap)]
mcap_52[, q_33 := quantile(marketCap, probs = c(0.33), na.rm = TRUE), by = "date"]
mcap_52[, q_66 := quantile(marketCap, probs = c(0.66), na.rm = TRUE), by = "date"]
mcap_52 <- mcap_52[!is.na(q_33) & !is.na(q_66)]
mcap_52[marketCap < q_33, size := "micro" ]
mcap_52[marketCap > q_66, size := "large" ]
mcap_52[marketCap >= q_33 & marketCap <= q_66, size := "mid" ]
mcap_52[is.na(size)]
setorderv(mcap_52, c("symbol", "date"))
mcap_52[, high52w := frollapply(high, 52 * 5, max), by = "symbol"]
mcap_52[, low52w := frollapply(low, 52 * 5, min), by = "symbol"]
divergence <- 0.02 # PARAMETER
mcap_52[(high52w / close  - 1) < divergence, high52w_dummy := 1]
mcap_52[(low52w / close  - 1) > -divergence, low52w_dummy := 1]
print(paste0("Many missing values!!! Percent of total observations: ", sum(is.na(mcap_52$marketCap)) / nrow(mcap_52) ))
mcap_52 <- mcap_52[!is.na(marketCap)]
mcap_52_indicator <- mcap_52[, .(high52w_sum = sum(high52w_dummy, na.rm = TRUE),
                                 low52w_sum = sum(low52w_dummy, na.rm = TRUE)), by = c("size", "date")]
setorderv(mcap_52_indicator, c("size", "date"))
mcap_52_indicator[, low_high_52w_ratio := low52w_sum / high52w_sum]
g1 <- ggplot(mcap_52_indicator, aes(x = date, y = low_high_52w_ratio, color = size)) +
  geom_line() +
  ggtitle("Whole period")
g1 <- ggplot(mcap_52_indicator[date %between% DOTCOM], aes(x = date, y = low_high_52w_ratio, color = size)) +
  geom_line() +
  ggtitle("DOTCOM")
g2 <- ggplot(mcap_52_indicator[date %between% GFC], aes(x = date, y = low_high_52w_ratio, color = size)) +
  geom_line() +
  ggtitle("GFC")
g3 <- ggplot(mcap_52_indicator[date %between% AFTERGFCBULL], aes(x = date, y = low_high_52w_ratio, color = size)) +
  geom_line() +
  ggtitle("AFTERGFCBULL")
g4 <- ggplot(mcap_52_indicator[date %between% COVID], aes(x = date, y = low_high_52w_ratio, color = size)) +
  geom_line() +
  ggtitle("COVID")
(g1 / g2) | (g3 / g4)
(g1 / g2 / g3 / g4)

# volume of growth / decline stocks
vol_52 <- prices_usa[, .(symbol, date, high, low, close, volume)]
vol_52[, return_52w := close / shift(close, n = 52 * 5) - 1, by = "symbol"]
vol_52[return_52w > 0, return_52w_dummy := "up", by = "symbol"]
vol_52[return_52w <= 0, return_52w_dummy := "down", by = "symbol"]
vol_52 <- vol_52[!is.na(return_52w_dummy)]
vol_52_indicators <- vol_52[, .(volume = sum(volume, na.rm = TRUE)), by = c("return_52w_dummy", "date")]
setorderv(vol_52_indicators, c("return_52w_dummy", "date"))
vol_52_indicators <- dcast(vol_52_indicators, date ~ return_52w_dummy)
vol_52_indicators[, volume_down_up_ratio := down / up]
ggplot(vol_52_indicators, aes(x = date, y = volume_down_up_ratio)) +
  geom_line() +
  ggtitle("Whole period")
g1 <- ggplot(vol_52_indicators[date %between% DOTCOM], aes(x = date, y = volume_down_up_ratio)) +
  geom_line() +
  ggtitle("DOTCOM")
g2 <- ggplot(vol_52_indicators[date %between% GFC], aes(x = date, y = volume_down_up_ratio)) +
  geom_line() +
  ggtitle("GFC")
g3 <- ggplot(vol_52_indicators[date %between% AFTERGFCBULL], aes(x = date, y = volume_down_up_ratio)) +
  geom_line() +
  ggtitle("AFTERGFCBULL")
g4 <- ggplot(vol_52_indicators[date %between% COVID], aes(x = date, y = volume_down_up_ratio)) +
  geom_line() +
  ggtitle("COVID")
(g1 / g2 / g3 / g4)

# spread high yield / investment grade
bonds <- prices[symbol %in% c("DHF", "DSM"), .(symbol, date, close)]
bonds <- dcast(bonds, date ~ symbol)
bonds$spread <- bonds[, 2] / bonds[, 3]
ggplot(bonds, aes(x = date, y = spread)) +
  geom_line()

# VIX
get_fread_data <- function(id = "VIXCLS") {
  data_ <- fredr(series_id = id, observation_start = as.Date("2000-01-01"), observation_end = Sys.Date())
  data_ <- as.data.table(data_)
  data_ <- data_[, .(date, value)]
  colnames(data_)[2] <- id
  return(data_)
}
vix <- get_fread_data("VIXCLS")
vix[, date := as.Date(date)]



# FEATURE IMPORTANCE ------------------------------------------------------
# merge all data
mcap_52_indicator_ <- melt(mcap_52_indicator, id.vars = c("date", "size"))
mcap_52_indicator_[, var := paste0(size, "_", variable)]
mcap_52_indicator_ <- dcast(mcap_52_indicator_, date ~ var, value.var = "value")
DT <- Reduce(function(x, y) merge(x, y, by = "date", all.x = TRUE, all.y = FALSE),
             list(spy, vix, vol_52_indicators, mcap_52_indicator_))
DT <- na.omit(DT)

# labels for feature importance and ML
DT[, ret_1 := shift(close, -1L, "shift") / close  - 1]
DT[, ret_5 := shift(close, -4L, "shift") / close - 1]
DT[, ret_22 := shift(close, -21L, "shift") / close - 1]
DT <- na.omit(DT)

# define feature cols and labels
LABEL = "ret_1"
features_cols <- colnames(DT)[which(colnames(DT) == "VIXCLS"):(which(colnames(DT) == "ret_1") - 1)]
# labels <- colnames(DT)[which(colnames(DT) == "ret_1"):which(colnames(DT) == "ret_22")]
# cols <- c(features_cols, labels)

# convert integer to numeric
cols_change <- c("up", "down")
DT[, (cols_change) := lapply(.SD, as.numeric), .SDcols = cols_change]

# add interactions
interactions <- model.matrix( ~ .^2 - 1, data = DT[, ..features_cols])
cols_keep <- c("date", "close", "returns", LABEL)
DT <- cbind(DT[, ..cols_keep], interactions)
colnames(DT) <- gsub(":", "_", colnames(DT))
features_cols_inter <- colnames(DT)[which(colnames(DT) == "VIXCLS"):ncol(DT)]

# remove inf values
dim(DT)
DT <- DT[is.finite(rowSums(DT[, .SD, .SDcols = is.numeric], na.rm = TRUE))] # remove inf values
dim(DT)

# remove constant columns
remove_cols <- features_cols_inter[apply(DT[, ..features_cols_inter], 2, var, na.rm=TRUE) == 0]
print(paste0("Removing feature with 0 standard deviation: ", remove_cols))
# feature_cols <- setdiff(feature_cols, remove_cols)

# remove highly correlated features
cor_matrix <- cor(DT[, ..features_cols_inter])
cor_matrix_rm <- cor_matrix
cor_matrix_rm[upper.tri(cor_matrix_rm)] <- 0
diag(cor_matrix_rm) <- 0
remove_cols <- features_cols_inter[apply(cor_matrix_rm, 2, function(x) any(abs(x) > 0.98))]
print(paste0("Removing highly correlated featue (> 0.98): ", remove_cols))
features_cols_inter <- setdiff(features_cols_inter, remove_cols)

# stationarity
number_differences <- lapply(DT[, ..features_cols_inter],
                             function(x) forecast::ndiffs(log(x)))
number_differences_cols <- number_differences[number_differences > 0]
DT[, (names(number_differences_cols)) := lapply(.SD, function(x) x - shift(x)), .SDcols = names(number_differences_cols)]

# importnat features
cols_keep <- c(features_cols_inter, LABEL)
X <- DT[, ..cols_keep]
X <- na.omit(X)
X <- as.matrix(X)

# f1st
f1st_fi <- f1st(X[, ncol(X)], X[, -ncol(X)], kmn = 20, sub = TRUE)
cov_index_f1st <- colnames(X[, -ncol(X)])[f1st_fi[[1]][, 1]]

# f3st_1
f3st_1 <- f3st(X[, ncol(X)], X[, -ncol(X)], m = 1)
cov_index_f3st_1 <- unique(as.integer(f3st_1[[1]][1, ]))[-1]
cov_index_f3st_1 <- cov_index_f3st_1[cov_index_f3st_1 != 0]
cov_index_f3st_1 <- colnames(X[, -ncol(X)])[cov_index_f3st_1]

# interesection of all important vars
most_important_vars <- intersect(cov_index_f1st, cov_index_f3st_1)
important_vars <- c(cov_index_f1st, cov_index_f3st_1)

# train/test and holdout set
start_holdout_date <- as.Date("2021-01-01")
holdout_ids <- which(DT$date > start_holdout_date)
X_model <- DT[-holdout_ids, ]
X_holdout <- DT[holdout_ids, ] # TODO SAVE THIS FOR QUANTCONNECT BACKTESTING

# select only labels and features
X_model <- X_model[, .SD, .SDcols = c("date", features_cols_inter, "returns")]
X_holdout <- X_holdout[, .SD, .SDcols = c("date", features_cols_inter, "returns")]



# SIMPLE MODELS -----------------------------------------------------------
# train data
cols_keep <- c("date", "returns", most_important_vars)
X_train <- X_model[, ..cols_keep]
X_train <- na.omit(X_train)
X_test <- na.omit(X_holdout[, ..cols_keep])

# parameters
window_lengths <- c(252, 252 * 2)

# # linear regression KEEP ONLY BECAUSE IT CAN BE HELPFULL LATER
# linear_regressions <- lapply(window_lengths, function(x) {
#   y <- roll_regres(returns ~ ., data = X_train[, 2:ncol(X_train)], width = x, do_compute = "1_step_forecasts")
#   y <- cbind.data.frame(date = X_train[, date], y$one_step_forecasts)
#   colnames(y)[2] <- paste0("pred_lm_one_step_", x)
#   y
# })
# preds_linear_regressions <- Reduce(function(x, y) merge(x, y, by = "date", all.x = TRUE, all.y = FALSE),
#                                    linear_regressions)
# preds_linear_regressions$sell <- apply(preds_linear_regressions[, 2:ncol(preds_linear_regressions)], 1, function(x) all(x < 0))
# preds_linear_regressions <- as.data.table(preds_linear_regressions)
# preds_linear_regressions[sell == TRUE, pred_lm_one_step_all_negative := -0.1]
# preds_linear_regressions[sell == FALSE, pred_lm_one_step_all_negative := 0.1]
# preds_linear_regressions$sell <- NULL
# tail(preds_linear_regressions)


# VAR
library(vars)
library(BigVAR)
library(tsDyn)
roll_preds <- lapply(window_lengths, function(x) {
   runner(
    x = X_train,
    f = function(x) {
      # x <- X_train[1:300, 1:ncol(X_train)]
      y <- as.data.frame(x[, 2:ncol(x)])
      y <- y[, which(apply(y, 2 , sd) != 0), drop = FALSE]
      if(length(y) == 1) print("STOP!")

      # VAR
      res <- VAR(y, lag.max = 5, type = "both")
      p <- predict(res)
      var_pred_onestep <- p$fcst[[1]][1, 1]

      # BigVar
      Y <- as.matrix(y)
      B = BigVAR.fit(Y, struct='Basic', p=5, lambda=1)[,,1]
      Z = VARXLagCons(Y, p=5, oos=TRUE)$Z
      yhat = B%*%Z[,ncol(Z),drop=FALSE]
      bigvar_pred_onestep <- yhat[1]

      # TVAR (1)
      tv1 <- tryCatch(TVAR(y, lag=2, nthresh=1, thDelay=1, trim=0.1, mTh=1, plot=FALSE), error = function(e) NULL)
      if (is.null(tv1)) {
        tv1_pred_onestep <- NA
      } else {
        tv1 <- predict(tv1)
        tv1_pred_onestep <- tv1[1, 1]
      }

      # TVAR (2)
      tv2 <- tryCatch(TVAR(y, lag=2, nthresh=2, thDelay=1, trim=0.1, mTh=1, plot=FALSE), error = function(e) NULL)
      if (is.null(tv2)) {
        tv2_pred_onestep <- NA
      } else {
        tv2 <- predict(tv2)
        tv2_pred_onestep <- tv2[1, 1]
      }

      # merge all predictions
      data.frame(var_pred_onestep = var_pred_onestep,
                 bigvar_pred_onestep = bigvar_pred_onestep,
                 tv1_pred_onestep = tv1_pred_onestep,
                 tv2_pred_onestep = tv2_pred_onestep)
    },
    k = x,
    lag = 0L,
    na_pad = TRUE
  )
})
roll_preds_merged <- lapply(roll_preds, function(x) lapply(x, as.data.table))
roll_preds_merged <- lapply(roll_preds_merged, rbindlist, fill = TRUE)
lapply(roll_preds_merged, function(x) x[, V1 := NULL])
roll_preds_merged <- lapply(seq_along(roll_preds_merged), function(i) {
  colnames(roll_preds_merged[[i]]) <- paste0(colnames(roll_preds_merged[[i]]), "_", window_lengths[i])
  roll_preds_merged[[i]]
})
roll_preds_merged <- as.data.table(do.call(cbind, roll_preds_merged))
roll_preds_merged <- roll_preds_merged[, lapply(.SD, unlist)]
roll_preds_merged$mostly_negative <- apply(roll_preds_merged, 1, function(x) sum(x < 0) > 5)
roll_preds_merged$mostly_negative_tv2 <- apply(roll_preds_merged[, .(tv2_pred_onestep_252, tv2_pred_onestep_504)],
                                               1, function(x) sum(x < 0) > 2)
roll_preds_merged[, mostly_negative := ifelse(mostly_negative == TRUE, -0.1, 0.1)]
roll_preds_merged[, mostly_negative_tv2 := ifelse(mostly_negative_tv2 == TRUE, -0.1, 0.1)]
roll_preds_merged$mean_pred <- apply(roll_preds_merged, 1, function(x) mean(x))
predictions_var <- cbind(date = X_train[, date], roll_preds_merged)

# TVAR
# library(tsDyn)
# data(zeroyld)
# tv <- TVAR(y, lag=2, nthresh=1, thDelay=1, trim=0.1, mTh=1, plot=FALSE)
# print(tv)
# summary(tv)
# plot(tv)
# predict(tv)
# c(AIC(tv), BIC(tv), logLik(tv))

# backtest apply
backtest <- function(returns, indicator, threshold, return_cumulative = TRUE) {
  sides <- vector("integer", length(indicator))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(indicator[i-1])) {
      sides[i] <- NA
    } else if (indicator[i-1] < threshold) { #  & indicator_2[i-1] > 1
      sides[i] <- 0
    } else {
      sides[i] <- 1
    }
  }
  sides <- ifelse(is.na(sides), 1, sides)
  returns_strategy <- returns * sides
  if (return_cumulative) {
    return(PerformanceAnalytics::Return.cumulative(returns_strategy))
  } else {
    return(returns_strategy)
  }
}
Performance <- function(x) {
  cumRetx = Return.cumulative(x)
  annRetx = Return.annualized(x, scale=252 * 8)
  sharpex = SharpeRatio.annualized(x, scale=252 * 8)
  winpctx = length(x[x > 0])/length(x[x != 0])
  annSDx = sd.annualized(x, scale=252 * 8)

  DDs <- findDrawdowns(x)
  maxDDx = min(DDs$return)
  maxLx = max(DDs$length)

  Perf = c(cumRetx, annRetx, sharpex, winpctx, annSDx, maxDDx, maxLx)
  names(Perf) = c("Cumulative Return", "Annual Return","Annualized Sharpe Ratio",
                  "Win %", "Annualized Volatility", "Maximum Drawdown", "Max Length Drawdown")
  return(Perf)
}
backtest_data <- merge(spy, predictions_var, by = "date", all.x = TRUE, all.y = FALSE)
backtest_data <- na.omit(backtest_data)
Return.cumulative(backtest_data$returns)
backtest_results <- lapply(backtest_data[, 4:ncol(backtest_data)], function(x) backtest(backtest_data$returns, x, -0.002))
backtest_results <- as.data.table(backtest_results)
backtest_results

# individual backtests
x <- backtest(backtest_data$returns, backtest_data$tv2_pred_onestep_504, -0.002, FALSE)
backtest_results_ind <- as.xts(cbind(SPY = backtest_data$returns, Strategy = x), order.by = as.Date(backtest_data$date))
charts.PerformanceSummary(backtest_results_ind)
Performance(backtest_results_ind$SPY)
Performance(backtest_results_ind$Strategy)
# subsample
charts.PerformanceSummary(as.xts(cbind(backtest_data$returns, x), order.by = as.Date(backtest_data$date))[1:1000])
charts.PerformanceSummary(as.xts(cbind(backtest_data$returns, x), order.by = as.Date(backtest_data$date))[1000:2000])
charts.PerformanceSummary(as.xts(cbind(backtest_data$returns, x), order.by = as.Date(backtest_data$date))[2000:3000])
charts.PerformanceSummary(as.xts(cbind(backtest_data$returns, x), order.by = as.Date(backtest_data$date))[3000:4000])
charts.PerformanceSummary(as.xts(cbind(backtest_data$returns, x), order.by = as.Date(backtest_data$date))[4000:nrow(backtest_data)])

# prediction on test set with best model
X_test_ <- rbind(X_train[(nrow(X_train)-504+1):nrow(X_train),], X_test)
roll_preds_test <- lapply(window_lengths, function(x) {
  runner(
    x = X_test_,
    f = function(x) {
      # x <- X_train[1:300, 1:ncol(X_train)]
      y <- as.data.frame(x[, 2:ncol(x)])
      y <- y[, which(apply(y, 2 , sd) != 0), drop = FALSE]
      if(length(y) == 1) print("STOP!")

      # VAR
      res <- VAR(y, lag.max = 5, type = "both")
      p <- predict(res)
      var_pred_onestep <- p$fcst[[1]][1, 1]

      # BigVar
      Y <- as.matrix(y)
      B = BigVAR.fit(Y, struct='Basic', p=5, lambda=1)[,,1]
      Z = VARXLagCons(Y, p=5, oos=TRUE)$Z
      yhat = B%*%Z[,ncol(Z),drop=FALSE]
      bigvar_pred_onestep <- yhat[1]

      # TVAR (1)
      tv1 <- tryCatch(TVAR(y, lag=2, nthresh=1, thDelay=1, trim=0.1, mTh=1, plot=FALSE), error = function(e) NULL)
      if (is.null(tv1)) {
        tv1_pred_onestep <- NA
      } else {
        tv1 <- predict(tv1)
        tv1_pred_onestep <- tv1[1, 1]
      }

      # TVAR (2)
      tv2 <- tryCatch(TVAR(y, lag=2, nthresh=2, thDelay=1, trim=0.1, mTh=1, plot=FALSE), error = function(e) NULL)
      if (is.null(tv2)) {
        tv2_pred_onestep <- NA
      } else {
        tv2 <- predict(tv2)
        tv2_pred_onestep <- tv2[1, 1]
      }

      # merge all predictions
      data.frame(var_pred_onestep = var_pred_onestep,
                 bigvar_pred_onestep = bigvar_pred_onestep,
                 tv1_pred_onestep = tv1_pred_onestep,
                 tv2_pred_onestep = tv2_pred_onestep)
    },
    k = x,
    lag = 0L,
    na_pad = TRUE
  )
})
predictions_test <- as.data.table(roll_preds_test)
predictions_test <- unlist(predictions_test$V2)
predictions_test <- cbind.data.frame(date = X_test_[, date], predictions_test)
setnames(predictions_test, c("date", "tv2_pred_onestep_504"))

backtest_data_test <- merge(spy, predictions_test, by = "date", all.x = TRUE, all.y = FALSE)
backtest_data_test <- na.omit(backtest_data_test)
Return.cumulative(backtest_data_test$returns)
backtest_results_test <- lapply(backtest_data_test[, 4:ncol(backtest_data_test)], function(x) backtest(backtest_data_test$returns, x, -0.002))
backtest_results_test <- as.data.table(backtest_results_test)
backtest_results_test

# individual backtests
x <- backtest(backtest_data_test$returns, backtest_data_test$tv2_pred_onestep_504 , -0.0001, FALSE)
backtest_results_test <- as.xts(cbind(SPY = backtest_data_test$returns, Strategy = x), order.by = as.Date(backtest_data_test$date))
charts.PerformanceSummary(backtest_results_test)
Performance(backtest_results_test$SPY)
Performance(backtest_results_test$Strategy)

# save data to blob
predictions <- rbind(predictions_var[, .(date, tv2_pred_onestep_504)], predictions_test)
predictions <- na.omit(predictions)
predictions <- unique(predictions, by = "date")
setorderv(predictions, "date")
qc_backtest <- copy(predictions)
# cols <- setdiff(cols_keep, "date")
# qc_backtest[, (cols) := lapply(.SD, shift), .SDcols = cols] # VERY IMPORTANT STEP !
qc_backtest <- na.omit(qc_backtest)
file_name <- "D:/risks/pr/systemic_risk_v2.csv"
fwrite(qc_backtest, file_name, col.names = FALSE, dateTimeAs = "write.csv")
bl_endp_key <- storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), key=Sys.getenv("BLOB-KEY"))
cont <- storage_container(bl_endp_key, "qc-backtest")
storage_upload(cont, file_name, basename(file_name))

backtest_data_test <- merge(spy, qc_backtest, by = "date", all.x = TRUE, all.y = FALSE)
backtest_data_test <- na.omit(backtest_data_test)
Return.cumulative(backtest_data_test$returns)
backtest_results_test <- lapply(backtest_data_test[, 4:ncol(backtest_data_test)], function(x) backtest(backtest_data_test$returns, x, -0.002))
backtest_results_test <- as.data.table(backtest_results_test)
backtest_results_test

# individual backtests
x <- backtest(backtest_data_test$returns, backtest_data_test$predictions_test , -0.002, FALSE)
backtest_results_test <- as.xts(cbind(SPY = backtest_data_test$returns, Strategy = x), order.by = as.Date(backtest_data_test$date))
charts.PerformanceSummary(backtest_results_test)



# MODEL SYSTEMIC RISK -----------------------------------------------------
# task for regression
task_reg <- as_task_regr(X_train[, .SD, .SDcols = !c("date")], id = "reg", target = LABEL)
task_reg_holdout <- as_task_regr(X_train[, .SD, .SDcols = !c("date")], id = "reg_holdout", target = LABEL)

# select important features
task_reg$select(most_important_vars)
task_reg_holdout$select(most_important_vars)

# descriptive analysis
library(rpart.plot)
learner = lrn("regr.rpart", maxdepth = 3, minbucket = 50, minsplit = 10, cp = 0.001)
learner$param_set
task_ <- task_reg$clone()
learner$train(task_)
predictins = learner$predict(task_)
predictins$score(msr("regr.mae"))
learner$importance()
rpart_model <- learner$model
rpart.plot(rpart_model)

# xgboost
learner = lrn("regr.xgboost")
graph = po("removeconstants", ratio = 0.05) %>>%
  # scaling
  po("branch", options = c("nop_prep", "yeojohnson", "pca", "ica"), id = "prep_branch") %>>%
  gunion(list(po("nop", id = "nop_prep"), po("yeojohnson"), po("pca", scale. = TRUE), po("ica"))) %>>%
  po("unbranch", id = "prep_unbranch") %>>%
  learner
plot(graph)
graph_learner = as_learner(graph)
as.data.table(graph_learner$param_set)[1:80, .(id, class, lower, upper)]
search_space = ps(
  # preprocesing
  prep_branch.selection = p_fct(levels = c("nop_prep", "yeojohnson", "pca", "ica")),
  pca.rank. = p_int(2, 6, depends = prep_branch.selection == "pca"),
  ica.n.comp = p_int(2, 6, depends = prep_branch.selection == "ica"),
  yeojohnson.standardize = p_lgl(depends = prep_branch.selection == "yeojohnson"),
  # model
  regr.xgboost.nrounds = p_int(100, 5000),
  regr.xgboost.eta = p_dbl(1e-4, 1),
  regr.xgboost.max_depth = p_int(1, 8),
  regr.xgboost.colsample_bytree = p_dbl(0.1, 1),
  regr.xgboost.colsample_bylevel = p_dbl(0.1, 1),
  regr.xgboost.lambda = p_dbl(0.1, 1),
  regr.xgboost.gamma = p_dbl(1e-4, 1000),
  regr.xgboost.alpha = p_dbl(1e-4, 1000),
  regr.xgboost.subsample = p_dbl(0.1, 1)
)
# plan("multisession", workers = 4L)
at_xgboost = auto_tuner(
  method = "random_search",
  learner = graph_learner,
  resampling = rsmp("cv", folds = 3),
  measure = msr("regr.rmse"),
  search_space = search_space,
  term_evals = 25
)
at_xgboost$train(task_reg)

# inspect results
archive <- as.data.table(at_xgboost$archive)
archive
preds = at_xgboost$predict(task_reg)
preds$score(msr("regr.rmse"))

# holdout extreme
preds_holdout <- at_xgboost$predict(task_reg_holdout)
preds_holdout$score(msrs(c("regr.rmse")))
prediciotns_extreme_holdout <- as.data.table(preds_holdout)

prediciotns_extreme_holdout_buy <- prediciotns_extreme_holdout[response > 2]
nrow(prediciotns_extreme_holdout_buy)
prediciotns_extreme_holdout_buy[, `:=`(truth_01 = ifelse(truth > 0, 1, 0),
                                       response_01 = ifelse(response > 0, 1, 0))]
mlr3measures::acc(as.factor(prediciotns_extreme_holdout_buy$truth_01),
                  factor(prediciotns_extreme_holdout_buy$response_01, levels = c("0", "1")))




# BACKTEST ----------------------------------------------------------------
# backtst function
backtest <- function(returns, indicator, threshold, return_cumulative = TRUE) {
  sides <- vector("integer", length(indicator))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(indicator[i-1])) {
      sides[i] <- NA
    } else if (indicator[i-1] > threshold) {
      sides[i] <- 0
    } else {
      sides[i] <- 1
    }
  }
  sides <- ifelse(is.na(sides), 1, sides)
  returns_strategy <- returns * sides
  if (return_cumulative) {
    return(PerformanceAnalytics::Return.cumulative(returns_strategy))
  } else {
    return(returns_strategy)
  }
}

# individual backtest
mcap_52_indicator_merge <- dcast(mcap_52_indicator[, .(size, date, low_high_52w_ratio)], date ~ size)
backtest_data <- merge(spy, mcap_52_indicator_merge, by = "date")
results <- backtest(backtest_data$returns, backtest_data$micro, 5, FALSE)
results <- cbind(spy, strategy = results)
charts.PerformanceSummary(as.xts.data.table(results))





# MLR3 PLAYGROOUND --------------------------------------------------------
library(mlr3temporal)
library(mlr3verse)
library(tsbox)


task = tsk("petrol")
learner = LearnerRegrForecastVAR$new()

rr = rsmp("forecastHoldout", ratio = 0.8)
rr$instantiate(task)
resample = resample(task, learner, rr, store_models = TRUE)
resample$predictions()

task = tsk("petrol")
learner = LearnerRegrForecastVAR$new()

rr = rsmp("RollingWindowCV", folds = 5, fixed_window = F)
rr$instantiate(task)
resample = resample(task, learner, rr, store_models = TRUE)
resample$predictions()

# multivariate
X_train$date <- as.Date(X_train$date)

task = TaskForecast$new(id = 'systemicrisk',
                        backend = X_train,
                        target = 'returns',
                        date_col = 'date')
learner = LearnerRegrForecastVAR$new()

rr = rsmp("RollingWindowCV", folds = 4, fixed_window = FALSE)
rr$instantiate(task)
resample = resample(task, learner, rr, store_models = TRUE)
resample$predictions()
