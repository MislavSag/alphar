library(data.table)
library(httr)
library(rvest)
library(dplyr)
library(plyr)
library(janitor)
library(ggplot2)
library(AzureStor)



# params
base_url <- "https://www.quantconnect.com/forum/discussion/"
last_id <- 30000
first_id <- 1

# get all backtest urls
backtests <- c()
for (i in last_id:first_id) {
  backtest_urls <- RETRY("GET", paste0(base_url, i), times = 3)
  backtest_urls <- read_html(backtest_urls) %>%
    html_nodes('iframe') %>%
    html_attr('src') %>%
    .[grepl('backtest', .)] %>%
    ifelse(is.null(.), NA, .)
  backtests <- c(backtest_urls, backtests)
  Sys.sleep(0.1)
}
backtests <- c(backtests)
backtests <- backtests[!is.na(backtests)]
backtests <- unique(backtests)
file_name <- paste0("qc_backtestes-",
                    format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S"),
                    ".rda")
save(backtests, file = file_name)

# get backtest from urls
backtests_results <- list()
for (i in seq_along(backtests)) {

  # debug
  print(i)

  req_retry <- RETRY("GET", backtests[i], times = 3)
  if (req_retry$status != 200) {
    print('No 200 status')
    backtests_results[[i]] <- NULL
    next()
  }
  if (is.null(content(req_retry))) {
    print('Content NULL')
    backtests_results[[i]] <- NULL
    next()
  }

  # debug
  print("Read html")
  p <- read_html(req_retry)
  node_table <- html_nodes(p, 'table')
  node_table <- node_table %>%
    html_elements("div") %>%
    html_text()

  # debug
  print("Clean html")
  if (length(node_table) == 0) {
    backtests_results[[i]] <- NULL
  } else {
    res <- cbind.data.frame(node_table[seq(1, length(node_table), 2)],
                            node_table[seq(2, length(node_table), 2)])
    colnames(res) <- c("Var", "Value")
    res <- as.data.table(res)
    res <- data.table::transpose(res, make.names = 1)
    backtests_results[[i]] <- res
  }
}
names(backtests_results) <- backtests
backtests_qc <- data.table::rbindlist(backtests_results, idcol = "backtest", fill = TRUE)
backtests_qc[, backtest := gsub(".*/", "", backtest)]
cols <- colnames(backtests_qc)[2:ncol(backtests_qc)]
backtests_qc[, (cols) := lapply(.SD, function(x) as.numeric(gsub("%|$", "", x))), .SDcols = cols]
backtests_qc <- janitor::clean_names(backtests_qc)
file_name <- paste0("qc_backtestes_finish-",
                    format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S"),
                    ".csv")
fwrite(backtests_qc, file = file_name)

# import backtests
list.files()
backtests_qc <- fread("qc_backtestes_finish-20221114123644.csv")

# analyse results
backtests_qc_filter <- backtests_qc[(total_trades_2 > 100 | total_trades > 100) &
                                      (sharpe_ratio > 1 | sharpe_ratio_2 > 1) &
                                      (win_rate > 0.6 | win_rate_2 > 0.6) &
                                      (drawdown < 30) &
                                      (average_win > 0.02 | average_win_2  > 0.02)]
setorder(backtests_qc_filter, -expectancy)
dim(backtests_qc_filter)
backtests_qc_filter[46:50]

# https://www.quantconnect.com/terminal/cache/embedded_backtest_f2795ae39c02e27d63f0f0e4e9bc49da.html

# save to SNP blob
SNP_KEY = "0M4WRlV0/1b6b3ZpFKJvevg4xbC/gaNBcdtVZW+zOZcRi0ZLfOm1v/j2FZ4v+o8lycJLu1wVE6HT+ASt0DdAPQ=="
SNP_ENDPOINT = "https://snpmarketdata.blob.core.windows.net/"
bl_endp_key <- storage_endpoint(SNP_ENDPOINT, key=SNP_KEY)
cont <- storage_container(bl_endp_key, "webscraping-qc")
storage_upload(cont, "qc_backtestes_finish-20221114123644.csv", "qc_backtestes_finish-20221114123644.csv")

# manual inspect
# embedded_backtest_c7e3f80567e6f6392c9e089d76281f08.html - pairs trading
# embedded_backtest_8fcadc5736f52598ba1ee079dc70175f.html - sell bubble / not good after all https://www.quantconnect.com/forum/discussion/10630/shorting-bubbles-at-the-top/p1
# embedded_backtest_7332a94843c0c7136493a86d5de4d7cb.html - semms very good, buy low VIX
