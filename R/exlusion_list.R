library(data.table)
library(tabulizer)
library(stringr)
library(AzureStor)
library(xts)


# PDF url
url = "https://www.robeco.com/files/docm/docu-exclusion-list.pdf"

# Get PDF tables
result = list()
for (i in 2:5){
  out = as.data.table(extract_tables(url, page = i, method = 'stream'))
  result[[i-1]] = out
}

# Remove all empty rows
dt = lapply(result, function(x) {
  ns = unlist(x[, lapply(.SD, function(y) all(y == ''))], use.names = TRUE)
  col_ = names(ns[ns == TRUE])
  x[, (col_) := NULL]
  x
})

# Check if all values in column are numbers. If yes, bind with next column
dt = lapply(dt, function(x) {
  vector_delete = c()
  for (i in 1:(ncol(x)-1)) {
    if (all(str_detect(x[[i]], "^[0-9]+$"))) {
      x[[i]] = paste0(x[[i]], " ", x[[i+1]])
      vector_delete = c(vector_delete, i+1)
    }
  }
  x[, (colnames(x)[vector_delete]) := NULL]
  x
})
dt[[2]]

# Make multicolumn data.table one column data.table
dt = lapply(dt, function(x) {
  if (ncol(x) > 1) {
    x = melt(x, measure.vars = colnames(x))
  }
  x[, 2]
})
dt = rbindlist(dt)
dt = dt[value != ""]

# Check if row contain number
dt[, number := str_extract(value, "^[0-9]+")]
dt[200:250]
dt[250:300]
dt[, meta_logical := is.na(number) &
     ((shift(number, 1, type = "lead") == 1 & !grepl("level", value, ignore.case = TRUE)) |
        grepl("level", shift(value, 1, type = "lead"), ignore.case = TRUE))]
dt[meta_logical == TRUE, meta := value]
dt[, meta := na.locf(meta)]
dt = dt[, .(meta, company = value)]

# Check
dt[130:200]

# Remove first numbersequence tokenized by spce form comapny column
dt[, company := str_remove(company, "^[0-9]+\\s+")]

# Remove meta meta
dt = dt[meta != company]

# Remove all rows that contain onlz umbers in company column
dt = dt[!grepl("^[0-9]+$", company)]

# Exclude list
dt[, unique(meta)]
pat = "Controversial weapons|Thermal coal mining|Thermal coal power|Coal power expansion plans"
dt[, exclude := grepl(pat, meta)]

# Save
fwrite(dt, "F:/meta/exclusion_list.csv")

# Add to Azure
bl_endp_key = storage_endpoint(Sys.getenv("BLOB-ENDPOINT-SNP"), Sys.getenv("BLOB-KEY-SNP"))
cont = storage_container(bl_endp_key, "qc-live")
storage_write_csv(dt, cont, "exclusion_list.csv")
