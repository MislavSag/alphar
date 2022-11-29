library(data.table)
library(AzureStor)
library(pins)
library(fasttime)
library(runner)
library(univariateML)
library(ggplot2)



# setup
ENDPOINT=storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), Sys.getenv("BLOB-KEY"))

# boards
board_market_data <- board_azure(
  container = storage_container(ENDPOINT,
                                "equity-usa-minute-trades-fmplcoud-adjusted"),
  path = "",
  n_processes = 6L,
  versioned = NULL,
  cache = NULL
)

# import hour market data
symbols <- c("AAPL")
market_data_l <- lapply(tolower(symbols), pin_read, board = board_market_data)
names(market_data_l) <- tolower(symbols)
market_data <- rbindlist(market_data_l, idcol = "symbol")
market_data[, datetime := fastPOSIXct(as.character(datetime), tz = "GMT")]
market_data[, datetime := as.POSIXct(as.numeric(datetime),
                                     origin=as.POSIXct("1970-01-01", tz="EST"),
                                     tz="EST")]
market_data <- unique(market_data, by = c("symbol", "datetime"))
market_data[, returns := close / shift(close, 1L) - 1, by = "symbol"]
market_data <- na.omit(market_data)

# estimate univariate distributions
X <- market_data[symbol == "aapl"]
dists <- runner(
  x = X$returns,
  f = function(x) {
    model_select(x)
  },
  k = 7 * 22,
  lag = 0L,
  na_pad = TRUE,
  simplify = FALSE
)
models <- unlist(lapply(dists, function(x) attributes(x)$model))

# save
models <- readRDS("./data/univar_models.rds")

# visualize
table(models)
data <- cbind(X, models)
ggplot(data, aes(x = datetime)) +
  geom_line(aes(y = close)) +
  geom_point(data = data[models == "Student-t" | models == "Laplace"], aes(x = datetime, y = close), color = "red")
data_ <- data[1:10000]
ggplot(data_, aes(x = datetime)) +
  geom_line(aes(y = close)) +
  geom_point(data = data_[models == "Student-t" | models == "Laplace"], aes(x = datetime, y = close), color = "red")
data_ <- data[10000:20000]
ggplot(data_, aes(x = datetime)) +
  geom_line(aes(y = close)) +
  geom_point(data = data_[models == "Student-t" | models == "Laplace"], aes(x = datetime, y = close), color = "red")
data_ <- data[20000:30000]
ggplot(data_, aes(x = datetime)) +
  geom_line(aes(y = close)) +
  geom_point(data = data_[models == "Student-t" | models == "Laplace"], aes(x = datetime, y = close), color = "red")
