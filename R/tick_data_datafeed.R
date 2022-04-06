library(data.table)
library(httr)
library(RcppQuantuccia)
library(QuantTools)
library(zip)


 # globals
APIKEY = Sys.getenv("APIKEY-FMPCLOUD")
setCalendar("UnitedStates::NYSE")

# find companies with highest market capitalization
sp500 <- GET("https://financialmodelingprep.com/api/v3/sp500_constituent", query = list(apikey = APIKEY))
sp500 <- content(sp500)
sp500 <- rbindlist(sp500)
sp500 <- sp500$symbol
urls <- paste0("https://financialmodelingprep.com/api/v3/historical-market-capitalization/", sp500)
mc <- lapply(urls, GET, query = list(limit = 5000, apikey = APIKEY))
mc_df <- lapply(mc, content)
mc_df <- lapply(mc_df, rbindlist)
mc_df <- rbindlist(mc_df)
largest <- mc_df[, median(marketCap, na.rm = TRUE), symbol]
setorderv(largest, "V1", order = -1L)
top_n <- largest[1:20]
symbols <- top_n$symbol
symbols <- setdiff(symbols, "BRK-B") # nodata for this symbols

# arguments
days = seq.Date(as.Date("2011-01-01"), Sys.Date() - 1, 1)
save_path = "D:/tick_data"

# extracy trading days from days
trading_days <- days[isBusinessDay(days)]

# get data
for (s in symbols) {
  print(s)
  if (!is.na(save_path)) dir_name <- file.path(save_path, tolower(s))
  if (!is.na(save_path) && dir.exists(dir_name)) {
    # exclude days that already exists
    scrap_days <- as.Date(setdiff(trading_days,
                                  as.Date(substr(list.files(dir_name), 1, 8), format = "%Y%m%d")),
                          origin = "1970-01-01")
    scrap_days <- scrap_days[scrap_days > as.Date("2015-06-01")]
  }
  if (!is.na(save_path) && !dir.exists(dir_name)) dir.create(dir_name)

  lapply(scrap_days, function(d) {
    file_name_zip <- paste(format.Date(d, "%Y%m%d"), "trade.zip", sep = "_")
    if (!is.na(save_path) & file.exists(file.path(dir_name, file_name_zip))) return(NULL)
    print(d)
    Sys.sleep(1L)
    tick_data <- tryCatch(get_finam_data(s, d, d, period = "tick"), error = function(e) NA)


    pokusaji <- 1
    while (length(tick_data) > 0 && all(is.na(tick_data)) & pokusaji < 10) {
      tick_data <- tryCatch(get_finam_data(s, d, d, period = "tick"), error = function(e) NULL)
      Sys.sleep(pokusaji)
      pokusaji <- pokusaji + 1
    }


    if (is.null(tick_data)) {
      return(NULL)
    } else {

      # if we dont want to save return data
      if (is.na(save_path)) {
        return(tick_data)
      }

      # save data
      file_name_csv <- paste(format.Date(d, "%Y%m%d"), tolower(s), "Trade", "Tick.csv", sep = "_")
      fwrite(tick_data, file.path(dir_name, file_name_csv))
      zip_file <- file.path(dir_name, file_name_zip)
      zipr(zip_file, file.path(dir_name, file_name_csv))
      file.remove(file.path(dir_name, file_name_csv))
    }
  })
}
