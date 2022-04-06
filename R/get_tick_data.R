library(data.table)
library(equityData)


# globals
symbols = c("SPY", "AAPL")
seq_date = seq.Date(Sys.Date() - 1, as.Date("2004-01-01"), by = -1)
save_path = file.path("D:/tick_data")

# scrap ticker data
lapply(symbols, function(s) {

  # crate directory uf ut doesn't exists
  dir_ <- file.path(save_path, s)
  if (!dir.exists(dir_)) {
    dir.create(dir_)
  }

  lapply(seq_date, function(d) {

    # if file already exists continue
    file_name <- file.path(dir_, paste0(d, ".csv"))
    if (file.exists(file_name)) {
      return(NULL)
    } else {
      tick_data <- get_tick_data(s, d)
      if (is.null(tick_data)) {
        return(NULL)
      } else {
        tick_data[, `:=`(SYMBOL = s,
                         DT = as.POSIXct(formated),
                         EX = "N")]
        setnames(tick_data, c("bid", "bsize", "ask", "asize"), c("BID", "BIDSIZ", "OFR", "OFRSIZ"))
        quotes_raw <- tick_data[, .(DT, EX, BID, BIDSIZ, OFR, OFRSIZ, SYMBOL)]
        fwrite(quotes_raw, file_name)
      }
    }
  })
})

