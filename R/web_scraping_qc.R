library(httr)
library(rvest)
library(dplyr)
library(plyr)
library(janitor)
library(ggplot2)


# params
base_url <- "https://www.quantconnect.com/forum/discussion/"
last_id <- 1146  # 6500
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
backtests <- c(backtests, backtests_2, backtests_3, backtests_4, backtests_5, backtests_6)
backtests <- backtests[!is.na(backtests)]
backtests <- unique(backtests)
save(backtests, file = 'qc_backtestes.rda')

# get backtest from urls
backtests_results <- list()
for (i in seq_along(backtests)) {
  req_retry <- RETRY("GET", backtests[i], times = 3)
  if (req_retry$status != 200) {
    print('No 200 status')
    backtests_results[[i]] <- NA
    next()
  }
  p <- read_html(req_retry)
  node_table <- html_nodes(p, 'table')
  node_table <- ifelse(length(node_table) == 0, NA,
                       html_table(node_table[[1]], head = TRUE, fill = TRUE)[[1]])
  if (is.na(node_table)) {
    backtests_results[[i]] <- NA
  } else {
    cells <- strsplit(node_table, "\\n")[[1]]
    cells <- cells[cells != '']
    first_column <- trimws(cells[seq(1, length(cells), 2)])
    second_column <- trimws(cells[seq(2, length(cells), 2)])
    second_column <- as.numeric(gsub('%|\\$', '', second_column))
    backtests_results[[i]] <- data.frame(measure =first_column, value = second_column)
  }
}
qc_backtests <- do.call(rbind.fill, lapply(backtests_results, function(x) {
  x <- t(x)
  values <- as.data.frame(x)[2, , drop = FALSE]
  col_names <- as.data.frame(x)[1, , drop = FALSE]
  colnames(values) <- gsub(' ', '', col_names)
  values
  })
)
qc_backtests <- apply(qc_backtests, 2, as.numeric)
qc_backtests <- cbind.data.frame(qc_backtests, backtests)
save(qc_backtests, file = 'qc_backtestes_finish.rda')

# read file
load('C:/Users/Mislav/Documents/qc_backtestes_finish.rda')

# analyse results
nrow(qc_backtests)
sample <- qc_backtests %>%
  filter(SharpeRatio > 0.2 & SharpeRatio < 10, TotalTrades > 1)
nrow(sample)
max(sample$SharpeRatio)
ggplot(sample, aes(SharpeRatio)) +
  geom_density()
head(qc_backtests)

# get n best
x <- qc_backtests %>%
  filter(TotalTrades > 10 & NetProfit > 0 & WinRate > 55 & SharpeRatio > 1 & Drawdown < 20) %>%
  arrange(desc(SharpeRatio))

# good ones
x %>%
  slice((nrow(x)):(nrow(x) - 10))
