library(financeScrap)
library(crypto2)
library(DBI)
library(RMySQL)
library(fst)


# crypto list
cryptos <- crypto2::crypto_list()

# quotes
quotes <- lapply(cryptos$id, function(x) {
  get_crypto_quotes(x, "USD", "1h", as.Date("2015-01-01"), Sys.Date())
})

# merge all
crypto_quotes <- rbindlist(quotes)
setnames(crypto_quotes, 'datetime', 'date_time')
crypto_quotes$date_time <- as.POSIXct(gsub('T', ' ', substr(crypto_quotes$date_time, 1, 19)),
                                      '%Y-%m-%d %H:%M:%S', tz = 'EST')
crypto_quotes[, currency := NULL]
crypto_quotes[, date_time := as.character(date_time)]
head(crypto_quotes)

# save localy
fst::write_fst(crypto_quotes, './data/crypto.fst')

# read fst file
crypto_quotes <- read_fst('./data/crypto.fst')
crypto_quotes <- crypto_quotes[1155173 + 1, ]

# function for MySQL connection
db_connect <- function() {
  db <- DBI::dbConnect(
    MySQL(),
    user = "odvjet12_mislav",
    password = "Theanswer0207",
    host = "91.234.46.219",
    port = 3306L,
    dbname = "odvjet12_crypto"
  )
}

# add to databas
db <- db_connect()
DBI::dbRemoveTable(db, "quotes")
query <- paste0(
  "CREATE TABLE quotes (
  id int NOT NULL AUTO_INCREMENT,
  date_time datetime,
  price float,
  vol float,
  market_cap float,
  crypto_id int(8),
  PRIMARY KEY (id)
) CHARACTER SET utf8 COLLATE utf8_general_ci;
  "
)
dbSendQuery(db, query)
dbSendQuery(db, 'set character set "utf8"')
dbSendQuery(db, 'SET NAMES utf8')
for (i in 1:nrow(crypto_quotes)) {
  query <- paste0("INSERT INTO quotes VALUES('0', '",
                  paste0(str_replace_all(str_replace_all(crypto_quotes[i, ], "^NA$", "NULL"), "'", ""),
                         collapse = "', '"), "')")
  dbSendQuery(db, query)
}
DBI::dbDisconnect(db)
