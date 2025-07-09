library(httr)


tmp_file = tempfile(fileext = ".csv")
profile = lapply(0:10, function(p) {
  GET(
    "https://financialmodelingprep.com/stable/profile-bulk",
    query = list(part = p, apikey = "6c390b6279b8a67cc7f63cbecbd69430"),
    write_disk(tmp_file, overwrite = TRUE)
  )
  Sys.sleep(1L)
  dt = fread(tmp_file)
  if (nrow(dt) == 0) return(NULL)
  dt
})
profile = rbindlist(profile, fill = TRUE)
profile = profile[!is.na(symbol)]
profile = profile[, .(symbol, currency, country, isin, exchange, industry,
                      sector, ipoDate, isEtf, isFund)]
setnames(profile, "symbol", "fmp_symbol")

# Save ETF and fund symbols
profile[, sort(unique(exchange))]
us_etf = profile[isEtf == 1 | isFund == 1] |>
  _[exchange %in% c("AMEX", "NASDAQ", "NYSE")] |>
  _[, .(symbol = unique(fmp_symbol))]
us_etf
fwrite(us_etf, "D:/strategies/lv/us_etf.csv")
