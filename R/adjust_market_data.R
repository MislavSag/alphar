library(zip)
library(data.table)
library(leanr)
library(zoo)

# GLOBALS
API_KEY = '15cd5d0adf4bc6805a724b4417bbaafc'
fmpc_set_token(API_KEY)

# get market data
market_data <- import_lean("D:/market_data/equity/usa/hour/trades")
market_data[, date := as.Date(datetime)]

# get only data from IPO
ipo_dates <- vapply(symbols, get_ipo_date, api_key = API_KEY, character(1))
ipo_dates_dt <- as.data.table(ipo_dates, keep.rownames = TRUE)
setnames(ipo_dates_dt, "rn", "symbol")
ipo_dates_dt[, ipo_dates := as.Date(ipo_dates)]
market_data_new <- copy(market_data)
market_data_new <- ipo_dates_dt[market_data_new, on = "symbol"]
market_data_new <- market_data_new[date >= ipo_dates]

# BKR SYMBOL CHANGE!!! IT WILL BE DOWNLOAD LATER PROBABBLY

# adjust for splits and dividends
factor_files_paths <- list.files("D:/factor_files", full.names = TRUE)
factor_files <- lapply(factor_files_paths, fread)
names(factor_files) <- gsub(".*/|.csv", "", factor_files_paths)
factor_files <- rbindlist(factor_files, idcol = TRUE)
setnames(factor_files, colnames(factor_files), c("symbol", "date", "price_factor", "split_factor", "previous_price"))
factor_files[, symbol := toupper(symbol)]
factor_files[, date := as.Date(date, "%Y%m%d")]

# adjust
df <- market_data_new[symbol %in% unique(factor_files$symbol)]
df <- merge(df, factor_files, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE)
df[, `:=`(split_factor = na.locf(split_factor, na.rm = FALSE, rev = TRUE),
          price_factor = na.locf(price_factor, na.rm = FALSE, rev = TRUE)), by = symbol]
df[, `:=`(split_factor = ifelse(is.na(split_factor), 1, split_factor),
          price_factor = ifelse(is.na(price_factor), 1, price_factor))]
cols_change <- c("open", "high", "low", "close")
df[, (cols_change) := lapply(.SD, function(x) {x * price_factor * split_factor}), .SDcols = cols_change]

# test for breaks
df[, abs_return := abs(close / shift(close) - 1), by = symbol]
df[abs_return > 0.4]
# ON LIST BUT OK: VRTX, AIG

# test individualy
test <- df[symbol == "L"]
plot(test$datetime, test$close)
test[datetime %between% c("2004-09-28", "2004-10-02")]

# change to QC format and save
save_path <- 'D:/market_data/equity/usa/hour/trades_adjusted'
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
  file_name <- file.path(save_path, paste0(tolower(s), ".csv"))
  fwrite(df_sample, file_name, col.names = FALSE)
  zip_file_name <- file.path(save_path, paste0(tolower(s), ".zip"))
  zipr(zip_file_name, file_name)
  file.remove(file_name)
}
