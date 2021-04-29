library(detectR)


# get sp500 companies
url <- 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
sp500 <- read_html(url) %>%
  html_nodes('table') %>%
  .[[1]] %>%
  html_table(.)
sp500_changes <- read_html(url) %>%
  html_nodes('table') %>%
  .[[2]] %>%
  html_table(., fill = TRUE)
head(sp500_changes)
sp500_symbols <- c(sp500$Symbol, sp500_changes$Added[-1], sp500_changes$Removed[-1])
sp500_symbols <- unique(sp500_symbols)

# import data
sp500_stocks <- import_mysql(
  symbols = sp500_symbols,  # 'IWM'
  upsample = 1,
  trading_hours = TRUE,
  use_cache = TRUE,
  combine_data = FALSE,
  save_path = 'D:/market_data/equity/usa/hour/trade',
  RMySQL::MySQL(),
  dbname = 'odvjet12_equity_usa_hour_trade',
  username = 'odvjet12_mislav',
  password = 'Theanswer0207',
  host = '91.234.46.219'
)
spy <- import_mysql(
  symbols = 'SPY',
  upsample = 1,
  trading_hours = TRUE,
  use_cache = TRUE,
  combine_data = FALSE,
  save_path = 'D:/market_data/equity/usa/hour/trade',
  RMySQL::MySQL(),
  dbname = 'odvjet12_equity_usa_hour_trade',
  username = 'odvjet12_mislav',
  password = 'Theanswer0207',
  host = '91.234.46.219'
)[[1]]
sp500_stocks <- compact(sp500_stocks)
sp500_stocks <- lapply(sp500_stocks, as.data.table)
sp500_stocks <- rbindlist(sp500_stocks, idcol = TRUE)

# calculate returns
spy <- na.omit(as.data.table(spy)[, returns := (close / shift(close)) - 1][])
sp500_stocks[, returns := (close / shift(close)) - 1, by = .(`.id`)]
sp500_stocks <- na.omit(sp500_stocks, cols = c("returns"))

# transform table to bi suitable dor the model
sp500_returns <- sp500_stocks[, .(.id, index, returns)]
sp500_returns <- data.table::dcast(sp500_returns, index ~ .id, value.var = 'returns')
spy_returns <- spy[sp500_returns, on = 'index'][, .(index, returns, close)]

# takesample for test
sample_ <- sp500_returns[, .SD, .SDcols = as.vector(which(colMeans(!is.na(sp500_returns)) > 0.90))]
sample_ <- as.matrix(sample_[, 2:ncol(sample_)])
sample_ <- sample_[1:10000, 1:20]
sample_ <- na.omit(sample_)
plot(1:10000, unlist(spy[1:10000, 'close']), type = 'l')

# Change point detection using PCA and binary segmentation
cp_binary <- detectBinary(sample_, L=2, n.cl = 8)
cp_binary$tstathist
cp_binary$Brhist
cp_binary$crit
cp_binary

# Change point detection using Graphical lasso as in Cribben et al.
cp_glasso <- detectGlasso(sample_, p=2, n.cl = 16)

# Change point detection using max-type statistic as in Jeong et.  al
cp_maxchange <- detectMaxChange(sample_, m=c(8*5, 8*10, 8*20, 8*30), n.cl = 8)

# Change point detection using PCA and sliding method
cp_sliding <- detectSliding(sample_, 800, L = 2, alpha = 0.01, n.cl = 16)

# visulalize
plot_data <- cbind(spy_returns[cp_sliding$time.seq, .(index, close)], cp_sliding$sW)
ggplot(plot_data, aes(x = index)) +
  geom_line(aes(y = close), color = 'blue') +
  geom_line(aes(y = V2), color = 'red')


