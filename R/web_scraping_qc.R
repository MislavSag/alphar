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
last_id <- 40000
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
backtests <- c(backtests) # ?
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
# list.files()
# load("qc_backtestes-20240425152335.rda")
# backtests_qc <- fread("qc_backtestes_finish-20240425180648.csv")

# analyse results
backtests_qc_filter <- backtests_qc[(total_trades_2 > 2000 | total_trades > 2000) &
                                      (sharpe_ratio > 2 | sharpe_ratio_2 > 2) &
                                      (win_rate > 0.6 | win_rate_2 > 0.6) &
                                      (drawdown < 15) &
                                      (average_win > 0.02 | average_win_2  > 0.02)]
setorder(backtests_qc_filter, -expectancy)
dim(backtests_qc_filter)
cols_keep = backtests_qc_filter[, colnames(backtests_qc_filter)[((colSums(is.na(.SD)) == nrow(backtests_qc_filter)) == FALSE)]]

# mannually checked  - not so good
remove = c(
  "embedded_backtest_2c7174d1e530e0a739ea7ed3045514f3.html", # OOS bad
  "embedded_backtest_47a1545a409fd06fe720a8e8c6f067b2.html", # second resolution, but good
  "embedded_backtest_dac873598cf3050bfe19b755c707d709.html", # trash
  "embedded_backtest_1c32ed180c2e36e2c9522407f3daa898.html", # OOS bad
  "embedded_backtest_b9254a7bd0a2e7adf0b1f922afcf953d.html", # trash
  "embedded_backtest_b9acead306e56646518f737070607698.html", # trash
  "embedded_backtest_f85389d055c02498ab5c139f7e82f35e.html", # OOS bad
  "embedded_backtest_c0386922c28878e8681c7d4927f3c902.html", # OOS bad
  "embedded_backtest_6e42fd9344e027b779677ef11e4eb475.html", # https://www.quantconnect.com/forum/discussion/11613/algo-trend0/p2
  "embedded_backtest_c188fc249bfb818ce7d11671af22432a.html", # OOS bad
  "embedded_backtest_5f29b3b1e72940bc5929f48df8793f85.html", # TA
  "embedded_backtest_b7286e7356d9d699b624e7d49aa6a0a9.html", # TA
  "embedded_backtest_2d8018d02b7ef6ab475c96676ea8a264.html", # crypto costs
  "embedded_backtest_153666d002ee071bf47343a1ce5085a0.html", # trash
  "embedded_backtest_58b4dd064e82274fb0eb8dd8237de123.html" # TA and can;t make it work
)
good = c(
  # stock sentiment with tingo
  # https://www.quantconnect.com/forum/discussion/10666/tiingo-sentiment-analysis-on-stocks-dictionary-over-64k-limit/
  "embedded_backtest_1988b6938e94b66934f2aa35758b1e38.html",
  # second resolution; Warren Harding; reversal; works on one symbol, on SPY much worse
  "embedded_backtest_5c9c1a74df40af2a393ff50f25b131f0.html"
)
backtests_qc_filter[backtest %notin% c(remove, good), ..cols_keep]

# https://www.quantconnect.com/terminal/cache/embedded_backtest_40dfff00a8ac3e3f96b911d42c7a27f0.html

# People = .ekz., Warren Harding, Vladimir,


# save to SNP blob
SNP_KEY = "0M4WRlV0/1b6b3ZpFKJvevg4xbC/gaNBcdtVZW+zOZcRi0ZLfOm1v/j2FZ4v+o8lycJLu1wVE6HT+ASt0DdAPQ=="
SNP_ENDPOINT = "https://snpmarketdata.blob.core.windows.net/"
bl_endp_key <- storage_endpoint(SNP_ENDPOINT, key=SNP_KEY)
cont <- storage_container(bl_endp_key, "webscraping-qc")
storage_upload(cont, file_name, file_name)

# manual inspect
# embedded_backtest_c7e3f80567e6f6392c9e089d76281f08.html - pairs trading
# embedded_backtest_8fcadc5736f52598ba1ee079dc70175f.html - sell bubble / not good after all https://www.quantconnect.com/forum/discussion/10630/shorting-bubbles-at-the-top/p1
# embedded_backtest_7332a94843c0c7136493a86d5de4d7cb.html - semms very good, buy low VIX
