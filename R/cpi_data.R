library(data.table)
library(blscrapeR)
library(rvest)
library(stringr)
library(lubridate)
library(AzureStor)



# get cpi items
cpi_items_meta <- fread("https://download.bls.gov/pub/time.series/cu/cu.item", sep = "\t")
cpi_items_meta[, seriesID := paste0("CUUR0000", item_code)]
cpi_items_chunks <- split(cpi_items_meta$seriesID, ceiling(seq_along(cpi_items_meta$seriesID) / 30))

# get cpi urban data by category
cpi_l <- lapply(cpi_items_chunks, function(chunk) {
  Sys.sleep(1L)
  cpi <- bls_api(chunk,
                 startyear = 2004,
                 endyear = 2022,
                 Sys.getenv("BLS_KEY"),
                 calculations = TRUE,
                 annualaverage = TRUE,
                 catalog = FALSE)
})
cpi <- rbindlist(cpi_l)
cpi <- dateCast(cpi)

# merge cpi and cpi meta
DT <- merge(as.data.table(cpi), cpi_items_meta[, .(seriesID, item_name)],
            by = "seriesID", all.x = TRUE, all.y = FALSE)
head(DT)

# choose items
unique(DT$item_name)[order(unique(DT$item_name))]
items_keep <- c("All items", "Food", "Food at home", "Food away from home",
                "Energy", "Gasoline (all types)", "Electricity",
                "Utility (piped) gas service",
                "All items less food and energy",
                "Commodities less food and energy commodities", "Apparel",
                "New vehicles", "Medical care commodities",
                "Services less energy services", "Shelter",
                "Medical care services", "Education and communication")
cpi_sample <- DT[item_name %in% items_keep]

# orghanize data in the same way as in QC example
calculation_vars <- c("m_gr", "m2_gr", "m3_gr", "year_gr")
cpi_sample[, (calculation_vars) := data.table::tstrsplit(calculations, " ")]
cpi_sample[, (calculation_vars) := lapply(.SD, as.numeric), .SDcols = calculation_vars]
cpi_sample <- dcast(cpi_sample, date ~ item_name, sum, value.var = "year_gr")

# release dates
cpi_hrefs <- read_html("https://www.bls.gov/bls/news-release/cpi.htm") %>%
  html_elements("a") %>%
  html_attr("href") %>%
  .[grep("archives", .)]
cpi_release_dates <- str_extract(cpi_hrefs, "\\d+")
cpi_release_dates <- unique(cpi_release_dates)
cpi_release_dates <- as.Date(cpi_release_dates, "%m%d%Y")
last_modified_date <- read_html("https://www.bls.gov/news.release/cpi.nr0.htm") %>%
  html_elements(xpath = '//*[@id="bodytext"]/span/text()') %>%
  html_text()
last_modified_date <- as.Date(trimws(last_modified_date), "%B %d, %Y")
cpi_release_dates <- c(cpi_release_dates, last_modified_date)
cpi_release_dates <- cbind.data.frame(
  date = floor_date(cpi_release_dates, "month") %m-% months(1),
  release_date = cpi_release_dates
  )

# merge release dates and cpi data
cpi_sample <- merge(cpi_sample, cpi_release_dates, by = "date", all.x = TRUE, all.y = FALSE)
cols_rearange <- c("release_date", "date", colnames(cpi_sample)[2:(ncol(cpi_sample)-1)])
cpi_sample <- cpi_sample[, ..cols_rearange]
cols_arrange <- c("release_date", "date", items_keep)
cpi_sample <- cpi_sample[, ..cols_arrange]

# save to azure blob
cpi_qc <- copy(cpi_sample)
cpi_qc[, release_date := format.Date(release_date, format = "%m%d%Y")]
cpi_qc[, release_date := paste0(release_date, " 10:00")]
endp <- storage_endpoint(Sys.getenv("BLOB-ENDPOINT-SNP"), Sys.getenv("BLOB-KEY-SNP"))
cont <- storage_container(endp, "qc-backtest")
storage_write_csv(cpi_qc, cont, "cpi.csv")
