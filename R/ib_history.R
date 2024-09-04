library(ibrestr)
library(data.table)
library(AzureStor)


# Init IB
ib = IB$new(
  host = "cgspaperpra.eastus.azurecontainer.io",
  port = 5000,
  strategy_name = "Least Volatile Local",
  account_id = "DU6474915",
  email_config = list(
    email_from = "mislav.sagovac@contentio.biz",
    email_to = "mislav.sagovac@contentio.biz",
    smtp_host = "mail.contentio.biz",
    smtp_port = 587,
    smtp_user = "mislav.sagovac@contentio.biz",
    smtp_password = "s8^t5?}r-x&Q"
  ),
  logger = NULL
)

# Check gateway
ib$check_gateway()

# Find VIX conid
vix_symbols = ib$search_contract_by_symbol("VIX")
vix_symbols[[1]]

# Get history data for VIX
dates = seq.POSIXt(as.POSIXct("2005-09-01 16:00:00"), Sys.time(), by = "130 days")
vix_history = list()
for (i in seq_along(dates)) {
  print(dates[i])
  test_ = tryCatch({
    vix_history[[i]] = ib$get_historical_data_hmds(
      conid = "13455763",
      barType = "Last",
      period = "150d",
      bar = "1h",
      startTime = format.POSIXct(dates[i], "%Y%m%d-%H:%M:%S"),
      clean = TRUE
    )
  }, error = function(e) NULL)
  if (is.null(test_)) {
    Sys.sleep(15L)
    vix_history[[i]] = ib$get_historical_data_hmds(
      conid = "13455763",
      barType = "Last",
      period = "150d",
      bar = "1h",
      startTime = format.POSIXct(dates[i], "%Y%m%d-%H:%M:%S"),
      clean = TRUE
    )
  }
}
vix_history_dt = rbindlist(vix_history)
vix_history_dt = unique(vix_history_dt)

# Checks
head(vix_history_dt[as.Date(datetime) > as.Date("2020-01-01")], 20)

# Save to Azure
qc_data = vix_history_dt[, .(date = datetime, open = o, high = h, low = l, close = c)]
qc_data[, date := as.character(date)]
bl_endp_key = storage_endpoint(Sys.getenv("BLOB-ENDPOINT-SNP"), Sys.getenv("BLOB-KEY-SNP"))
cont = storage_container(bl_endp_key, "qc-backtest")
storage_write_csv(qc_data, cont, "vix.csv", col_names = FALSE)
