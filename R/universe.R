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



# IMPORT DATA -------------------------------------------------------------
# market data
daily_prices_files <- pin_list(board_prices)
files_new <- setdiff(daily_prices_files, list.files(CACHEDIR))
lapply(files_new, pin_download, board = board_prices)
files_local <- vapply(file.path(CACHEDIR, daily_prices_files),
                      list.files, recursive = TRUE, pattern = "\\.csv", full.names = TRUE,
                      FUN.VALUE = character(1))
plan(multicore(workers = 4L))
prices_l <- future_lapply(files_local, fread)
prices <- prices_l[vapply(prices_l, function(x) nrow(x) > 0, FUN.VALUE = logical(1))]
prices <- rbindlist(prices)

# clean daily prices
prices <- prices[open > 0 & high > 0 & low > 0 & close > 0 & adjClose > 0] # remove rows with zero and negative prices
setorder(prices, "symbol", "date")
prices[, returns := adjClose   / data.table::shift(adjClose) - 1, by = symbol] # calculate returns
adjust_cols <- c("open", "high", "low")
prices[, (adjust_cols) := lapply(.SD, function(x) x * (adjClose / close)), .SDcols = adjust_cols] # adjust open, high and low prices
prices[, close := adjClose]
prices <- na.omit(prices[, .(symbol, date, open, high, low, close, volume, returns)])
prices <- unique(prices, by = c("symbol", "date")) # remove duplicates if they exists

# download only USA stocks
url <- modify_url("https://financialmodelingprep.com/", path = "api/v3/available-traded/list",
                  query = list(apikey = Sys.getenv("APIKEY-FMPCLOUD") ))
stocks <- rbindlist(content(GET(url)))
filter_symbols <- stocks[exchange %in% c("AMEX",
                                         "New York Stock Exchange Arca", "New York Stock Exchange",
                                         "NYSE American", "NYSEArca",
                                         "NasdaqGS", "NASDAQ", "Nasdaq", "NASDAQ Global Market",
                                         "Nasdaq Capital Market", "Nasdaq Global Market", "Nasdaq Global Select"
                                         )]
usa_symbols <- stocks[exchangeShortName %in% c("AMEX", "NASDAQ", "NYSE", "OTC")]

# remove ETF's
usa_symbols <- usa_symbols[!grep("etf|spdr", name, ignore.case = TRUE)]

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



# UNIVERSE SELECTION ------------------------------------------------------
# number of symbols by year-month
symbols_by_year_month <- prices_usa[, .(n_symbols = length(unique(symbol))), by = .(year, month)]
symbols_by_year_month[, date := as.Date(paste(year, month, "01", sep = "-"), format = "%Y-%m-%d")]
ggplot(symbols_by_year_month, aes(date, n_symbols)) + geom_line()

# keep most liquid
n_most_liquid <- 500
prices_usa[, volume_sum := sum(volume, na.rm = TRUE), by = .(symbol, year, month)]
most_liquid <- unique(prices_usa[, .(symbol, year, month, volume_sum)])
most_liquid <- most_liquid[, .SD[volume_sum %in% tail(sort(unique(volume_sum)), n_most_liquid)], by = .(year, month)]
prices_most_liquid <- most_liquid[, `:=`(index = 1, volume_sum = NULL)][prices_usa, on = .(year = year, month = month, symbol = symbol)]
prices_most_liquid <- prices_most_liquid[index == 1]
prices_most_liquid[, index := NULL]
setorderv(prices_most_liquid, c("year", "month", "symbol"))

# keep least volatile
n_least_volatile <- 250
prices_most_liquid[, vol_monthly := roll::roll_sd(returns, 22), by = symbol]
prices_most_liquid[, vol_monthly := data.table::last(vol_monthly, 1L), by = .(symbol, year, month)]
least_volatile <- unique(prices_most_liquid[, .(symbol, year, month, vol_monthly)])
least_volatile <- least_volatile[, .SD[vol_monthly %in% head(sort(unique(vol_monthly)), n_least_volatile)], by = .(year, month)]
prices_most_liquid_least_vol <- least_volatile[, `:=`(index = 1, vol_monthly = NULL)][
  prices_most_liquid, on = .(year = year, month = month, symbol = symbol)
]
prices_most_liquid_least_vol <- prices_most_liquid_least_vol[index == 1]
prices_most_liquid_least_vol[, index := NULL]
setorderv(prices_most_liquid_least_vol, c("year", "month", "symbol"))

# inspect
prices_most_liquid_least_vol[year == 2022 & month == 2, unique(symbol)]



# FUNDAMENTALS -------------------------------------------------------------
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

# remove stocks with fundamnetals below thresholds
DT_sample <- DT[fiveYRevenueGrowthPerShare >= 0.05]
DT_sample <- DT_sample[fiveYDividendperShareGrowthPerShare >= 0.01]
DT_sample <- DT_sample[payoutratio_5y_mean <= 0.7]
DT_sample <- DT_sample[returnOnCapitalEmployed_5y_mean >= 0.025]
DT_sample <- DT_sample[freeCashFlowYield_5y_mean >= 0.01]

# cluster analysis
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
#
#
# # test
# fundamentals[symbol == "AAPL" & calendarYear == 2021,
#              .(calendarYear, period, revenue, costOfRevenue, grossProfit, researchAndDevelopmentExpenses,
#                benef, operatingIncome)]
# fundamentals$tax





# QC BACKTEST -------------------------------------------------------------
#
qc_data <- DT[, .(fillingDate, symbol)]
setorder(qc_data, fillingDate)
setnames(qc_data, "fillingDate", "date")
qc_data[, date := format.Date(date, format = "%Y%m%d")]
qc_data <- qc_data[, .(symbol = paste0(unlist(symbol), collapse = ",")), by = date]
storage_write_csv(qc_data, storage_container(endpoint, "qc-backtest"), "universe.csv", col_names = FALSE)

