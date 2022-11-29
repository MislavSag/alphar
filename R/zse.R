

dates <- seq.Date(as.Date("2010-01-01"), Sys.Date(), 1)
urls <- sapply(dates, function(x) {
  paste0("https://rest.zse.hr/web/Bvt9fe2peQ7pwpyYqODM/price-list/XZAG/",
         x, "/csv?language=HR")
})

lapply(seq_along(dates), function(i) {
  tryCatch({
    download.file(urls[i],
                  file.path("D:/zse/market_data", paste0(dates[i], ".csv")),
                  mode = "wb")
  }, error = function(e) NULL)
})

