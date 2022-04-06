library(data.table)
library(fmpcloudr)
library(httr)
library(zip)
library(rvest)
library(stringr)
library(leanr)
library(future.apply)
library(zoo)
library(AzureStor)


# set fmpcloudr api token
API_KEY = "15cd5d0adf4bc6805a724b4417bbaafc" # ("APIKEY-FMPCLOUD")
fmpc_set_token(API_KEY)

# get sp 500 stocks
SP500 = fmpc_symbols_index()
SP500_DELISTED <- content(GET(paste0('https://financialmodelingprep.com/api/v3/historical/sp500_constituent?apikey=', API_KEY)))
SP500_DELISTED <- rbindlist(SP500_DELISTED)
SP500_SYMBOLS <- unique(c(SP500$symbol, SP500_DELISTED$symbol))
SP500_SYMBOLS <- c("SPY", SP500_SYMBOLS)
DELISTED_ALL <- rbindlist(content(GET(paste0('https://financialmodelingprep.com/api/v3/delisted-companies?limit=10000&apikey=', API_KEY))))

# get and save market unadjusted data
save_path <- 'D:/market_data/equity/usa/hour/trades'
market_data <- import_lean(save_path)
symbols <- unique(c(SP500_SYMBOLS, unique(market_data$symbol)))

# get data for symbols
save_market_data_update <- function(symbols, save_path = 'D:/market_data/equity/usa/hour/trades') {
  for (symbol in symbols) {
    print(symbol)

    # retrieve old data for symbol
    file_name <- file.path(save_path, paste0(tolower(symbol), ".csv"))
    zip_file_name <- file.path(save_path, paste0(tolower(symbol), ".zip"))
    old_data <- fread(cmd = paste0('unzip -p ', zip_file_name),
                      col.names = c('DateTime', "Open", "High", "Low", "Close", "Volume"))
    old_data <- old_data[!duplicated(DateTime)]
    last_date <- as.Date(max(as.POSIXct(old_data$DateTime, format = "%Y%m%d %H:%M")))

    # check iif stock is delisted
    if ((Sys.Date() - last_date) > 100 & (symbol %in% DELISTED_ALL$symbol | symbol %in% SP500_DELISTED$symbol) |
        (Sys.Date() - last_date) > 500) {
      print("Symbol is delisted. No more market data. Next!")
    } else {
      # get new data
      new_data <- rbindlist(lapply(seq.Date(last_date, Sys.Date() + 5, by = 5), function(d) {
        new_data <- get_market_equities(symbol, from = d, to = d + 5, api_key = API_KEY)
      }))
      if (length(new_data) == 0) {
        print(paste0("No data for symbol ", symbol))
        next()
      } else {
        new_data[, formated := as.POSIXct(formated)]
        new_data <- new_data[order(formated)]
        new_data <- new_data[format(formated, "%H:%M:%S") %between% c("10:00:00", "15:00:00")]
        new_data[, `:=`(DateTime = format.POSIXct(as.POSIXct(formated), "%Y%m%d %H:%M"),
                        Open = o * 10000,
                        High = h * 10000,
                        Low = l * 10000,
                        Close = c * 10000,
                        Volume = v)]
        new_data <- new_data[, .(DateTime, Open, High, Low, Close, Volume)]
        new_data <- new_data[!duplicated(DateTime)]
        if (file.exists(zip_file_name)) {
          new_data <- rbind(new_data, old_data)
          new_data <- new_data[!duplicated(DateTime)]
          setorder(new_data, "DateTime")
        }
        fwrite(new_data, file_name, col.names = FALSE)
        zipr(zip_file_name, file_name)
        file.remove(file_name)
      }
    }
  }
}

# update market data
save_market_data_update(unique(market_data$symbol))

# move all files to QC data folders
file.copy(list.files('D:/market_data/equity/usa/hour/trades', full.names = TRUE),
          'D:/lean_projects/data/equity/usa/hour',
          overwrite = TRUE)

# upload market data trade files to azure storage
print("Save data to Azure storage.")
files_to_blob <- list.files('D:/market_data/equity/usa/hour/trades', full.names = TRUE)
print("Azure auth.")
blob_endpoint <- "https://contentiobatch.blob.core.windows.net/"
blob_key <- "qdTsMJMGbnbQ5rK1mG/9R1fzfRnejKNIuOv3X3PzxoBqc1wwTxMyUuxNVSxNxEasCotuzHxwXECo79BLv71rPw=="
bl_endp_key <- storage_endpoint(blob_endpoint, key=blob_key)
# bl_endp_key <- storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), key=Sys.getenv("BLOB-KEY"))
cont <- storage_container(bl_endp_key, "equity-usa-hour-trades")
print("Azure save.")
lapply(files_to_blob, function(f) storage_upload(cont, f, basename(f)))

# generate factor files
market_data <- import_lean(save_path)
market_data_daily <- market_data[, .SD[.N], by = .(symbol, date = as.Date(datetime))]
symbols <- unique(market_data_daily$symbol)
lapply(unique(market_data_daily$symbol), function(x) {
  print(x)
  create_factor_file(x, market_data_daily, API_KEY, "D:/factor_files")
})

# save factor files to Azure Storage
files_to_blob <- list.files("D:/factor_files", full.names = TRUE)
blob_endpoint <- "https://contentiobatch.blob.core.windows.net/"
blob_key <- "qdTsMJMGbnbQ5rK1mG/9R1fzfRnejKNIuOv3X3PzxoBqc1wwTxMyUuxNVSxNxEasCotuzHxwXECo79BLv71rPw=="
bl_endp_key <- storage_endpoint(blob_endpoint, key=blob_key)
cont <- storage_container(bl_endp_key, "factor-files")
lapply(files_to_blob, function(f) storage_upload(cont, f, basename(f)))

# move all factor file to QC data folders
file.copy(list.files('D:/factor_files', full.names = TRUE),
          'D:/lean_projects/data/equity/usa/factor_files',
          overwrite = TRUE)

# get only data from IPO
plan(multicore(workers = 2L))
ipo_dates <- vapply(symbols, function(x) {
  y <- get_ipo_date(x, api_key = API_KEY)
  if (is.null(y)) {
    return("2004-01-02")
  } else {
    return(y)
  }
}, character(1))
ipo_dates_dt <- as.data.table(ipo_dates, keep.rownames = TRUE)
setnames(ipo_dates_dt, "rn", "symbol")
ipo_dates_dt[, ipo_dates := as.Date(ipo_dates)]
market_data_new <- copy(market_data)
market_data_new <- ipo_dates_dt[market_data_new, on = "symbol"]
market_data_new <- market_data_new[datetime >= ipo_dates]
market_data_new_daily <- market_data_new[, .SD[.N], by = .(symbol, date = as.Date(datetime))]
market_data_new_daily$ipo_dates <- NULL

# adjust for splits and dividends
factor_files_paths <- list.files("D:/factor_files", full.names = TRUE)
factor_files <- lapply(factor_files_paths, fread)
names(factor_files) <- gsub(".*/|.csv", "", factor_files_paths)
factor_files <- rbindlist(factor_files, idcol = TRUE)
setnames(factor_files, colnames(factor_files), c("symbol", "date", "price_factor", "split_factor", "previous_price"))
factor_files[, symbol := toupper(symbol)]
factor_files[, date := as.Date(as.character(date), "%Y%m%d")]

# adjust
df <- market_data_new[symbol %in% unique(factor_files$symbol)]
df[, date:= as.Date(datetime)]
df <- merge(df, factor_files, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE)
df[, `:=`(split_factor = na.locf(split_factor, na.rm = FALSE, rev = TRUE),
          price_factor = na.locf(price_factor, na.rm = FALSE, rev = TRUE)), by = symbol]
df[, `:=`(split_factor = ifelse(is.na(split_factor), 1, split_factor),
          price_factor = ifelse(is.na(price_factor), 1, price_factor))]
cols_change <- c("open", "high", "low", "close")
df[, (cols_change) := lapply(.SD, function(x) {x * price_factor * split_factor}), .SDcols = cols_change]

# # test for breaks
# df[, abs_return := abs(close / shift(close) - 1), by = symbol]
# df[abs_return > 0.4]
# # ON LIST BUT OK: VRTX, AIG
# test <- df[symbol == "L"]
# plot(test$datetime, test$close)
# test[datetime %between% c("2004-09-28", "2004-10-02")]

# change to QC format and save
save_path_adjusted <- 'D:/market_data/equity/usa/hour/trades_adjusted'
for (s in unique(df$symbol)) {
  print(s)
  df_sample <- df[symbol == s, .(datetime, open, high, low, close, volume)]
  df_sample <- df_sample[order(datetime)]
  df_sample[, `:=`(datetime = format.POSIXct(as.POSIXct(datetime), "%Y%m%d %H:%M"),
                   open = open * 10000,
                   high = high * 10000,
                   low = low * 10000,
                   close = close * 10000,
                   volume = volume)]
  df_sample <- unique(df_sample)
  file_name <- file.path(save_path_adjusted, paste0(tolower(s), ".csv"))
  fwrite(df_sample, file_name, col.names = FALSE)
  zip_file_name <- file.path(save_path_adjusted, paste0(tolower(s), ".zip"))
  zipr(zip_file_name, file_name)
  file.remove(file_name)
}

# upload files to azure storage
print("Save data to Azure storage.")
files_to_blob <- list.files(save_path_adjusted, full.names = TRUE)
print("Azure auth.")
blob_endpoint <- "https://contentiobatch.blob.core.windows.net/"
blob_key <- "qdTsMJMGbnbQ5rK1mG/9R1fzfRnejKNIuOv3X3PzxoBqc1wwTxMyUuxNVSxNxEasCotuzHxwXECo79BLv71rPw=="
bl_endp_key <- storage_endpoint(blob_endpoint, key=blob_key)
# bl_endp_key <- storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), key=Sys.getenv("BLOB-KEY"))
cont <- storage_container(bl_endp_key, "equity-usa-hour-trades-adjusted")
print("Azure save.")
lapply(files_to_blob, function(f) storage_upload(cont, f, basename(f)))
