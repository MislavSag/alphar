library(data.table)
library(fmpcloudr)
library(httr)
library(zip)
library(rvest)
library(stringr)
library(equityData)
library(AzureStor)


# set fmpcloudr api token
API_KEY = "15cd5d0adf4bc6805a724b4417bbaafc"
fmpc_set_token(API_KEY)
bl_endp_key <- storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), key=Sys.getenv("BLOB-KEY"))
qc_save_path = "D:/lean_projects/data/equity/usa/hour"

# get sp 500 stocks
SP500 = fmpc_symbols_index()
SP500_DELISTED <- content(GET(paste0('https://financialmodelingprep.com/api/v3/historical/sp500_constituent?apikey=', API_KEY)))
SP500_DELISTED <- rbindlist(SP500_DELISTED)
SP500_SYMBOLS <- unique(c(SP500$symbol, SP500_DELISTED$symbol))
SP500_SYMBOLS <- c("SPY", SP500_SYMBOLS)

# get ticker changes
get_ticker_changes <- function(ticker) {
  p <- POST('https://www.quantumonline.com/search.cfm',
            body = list(
              tickersymbol = ticker,
              sopt = 'symbol',
              '1.0.1' = 'Search'
            ))
  changes <- content(p) %>%
    html_elements(xpath = "//*[contains(text(),'Previous Ticker')]") %>%
    html_text() %>%
    gsub('.*Symbol:', '', .) %>%
    trimws(.)
  date <- as.Date(str_extract(changes, '\\d+/\\d+/\\d+'), '%m/%d/%Y')
  tickers <- str_extract(changes, '\\w+')
  changes <- data.table(ticker = ticker, date = date, ticker_change = tickers)
  return(changes)
}

# get changes
SP500_CHANGES <- lapply(SP500_SYMBOLS, get_ticker_changes)
SP500_CHANGES <- rbindlist(SP500_CHANGES)
SP500_SYMBOLS <- unique(c(SP500_SYMBOLS, SP500_CHANGES$ticker_change))

# get data for symbols
for (symbol in SP500_SYMBOLS) {
  print(symbol)

  # get data
  data_by_symbol <- get_fmpcloud_data(symbol, local = "D:/market_data/equity/usa/hour")

  # if empty continue
  if (is.null(data_by_symbol)) {
    # save to blob
    file_name <- paste0(tolower(symbol), ".csv")
    save_blob_files(data.frame(), file_name, container = "equity-usa-hour")
  } else {
    # save to blob
    file_name <- paste0(tolower(symbol), ".csv")
    save_blob_files(data_by_symbol, file_name, container = "equity-usa-hour")
  }
}

# import all data
equity_hours_files <- equityData::get_all_blob_files("equity-usa-hour")
market_data_hour_list <- list()
cont <- storage_container(bl_endp_key, "equity-usa-hour")
for (i in seq_along(equity_hours_files$name)) {
  x <- storage_read_csv2(cont, equity_hours_files$name[i])
  if (length(x) == 0) {
    next()
  } else {
    symbol <- toupper(gsub("\\..*", "", equity_hours_files$name[i]))
    market_data_hour_list[[i]] <- cbind(symbol = symbol, x)
  }
}
market_data_hour <- rbindlist(market_data_hour_list)
market_data_hour[, datetime := format.POSIXct(as.POSIXct(t / 1000, origin = "1970-01-01", tz = "EST"), format = "%Y%m%d %H:%M")]
setorderv(market_data_hour, c("symbol", "datetime"))

# convert to quantconnect format
market_data_hour_qc <- market_data_hour[format(formated, "%H:%M:%S") %between% c("10:00:00", "15:00:00")]
market_data_hour_qc[, `:=`(DateTime = datetime,
                        Open = o * 10000,
                        High = h * 10000,
                        Low = l * 10000,
                        Close = c * 10000,
                        Volume = v)]
market_data_hour_qc <- market_data_hour_qc[, .(symbol, DateTime, Open, High, Low, Close, Volume)]
market_data_hour_qc <- unique(market_data_hour_qc, by = c("symbol", "DateTime"))

# save QC formated files
for (s in unique(market_data_hour_qc$symbol)) {
  print(s)
  file_name <- file.path(qc_save_path, paste0(tolower(s), ".csv"))
  zip_file_name <- file.path(qc_save_path, paste0(tolower(s), ".zip"))
  sample_ <- market_data_hour[symbol == s]
  fwrite(sample_, file_name, col.names = FALSE)
  zipr(zip_file_name, file_name)
  file.remove(file_name)
}

# generate factor files
# market_data <- import_lean(save_path)
market_data_daily <- market_data_hour[format(formated, "%H:%M:%S") %between% c("10:00:00", "15:00:00")]
market_data_daily <- market_data_daily[, .SD[.N], by = .(symbol, date = as.Date(formated))]
setnames(market_data_daily, c("o", "h", "l", "c", "v"), c("open", "high", "low", "close", "volume"))
market_data_daily$t <- NULL
market_data_daily$datetime <- NULL
symbols <- unique(market_data_daily$symbol)
lapply(symbols, function(x) {
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

