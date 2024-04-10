library(data.table)
library(arrow)
library(fs)


# Import Databento minute data
prices = read_parquet("F:/databento/minute.parquet")
setDT(prices)

# Remove column we don't need
prices[, unique(rtype)] # all 33
prices[, unique(publisher_id)] # all 2
prices[, let(rtype = NULL, publisher_id = NULL)]

# Remove duplicates by symbol and date
prices = unique(prices, by = c("symbol", "ts_event"))

# Create Date column
prices[, date := as.Date(ts_event)]

# Import QC factor files and check symbol match with Databento
factor_files = dir_ls("F:/lean/data/equity/usa/factor_files")
symbols_factors = path_ext_remove(path_file(factor_files))
symbols = prices[, tolower(unique(symbol))]
symbols_matched = intersect(symbols, symbols_factors)
sprintf("Symbols in Databento but not in QC: %s",
        paste0(setdiff(symbols, symbols_matched), collapse = ", "))
ff = factor_files[symbols_factors %in% symbols_matched]
ffdt = lapply(ff, fread, col.names = c("date", "div", "split", "lastp"))
names(ffdt) = path_ext_remove(path_file(ff))
ffdt = rbindlist(ffdt, idcol = "symbol")
ffdt[, symbol := toupper(symbol)]
ffdt[, date := as.Date(as.character(date), format = "%Y%m%d")]
ffdt[symbol == "TSLA"]

# Merge factor files with prices
prices = ffdt[prices, on = c("symbol", "date"), roll = -Inf]
setorder(prices, symbol, date)
prices[symbol == "TSLA" & !is.na(split)]

# Adjuste close prices
prices[, close_adj := close * split * div]
plot(as.xts.data.table(prices[symbol == "TSLA", .(ts_event, close, close_adj)][seq(1, .N, 1000)]))
plot(as.xts.data.table(prices[symbol == "SPY", .(ts_event, close, close_adj)][seq(1, .N, 1000)]))

# Adjust all other columns
prices[, adj_rate := close_adj / close]
prices[, let(
  open = open * adj_rate,
  high = high * adj_rate,
  low  = low  * adj_rate
)]
setnames(prices, "close", "close_raw")
setnames(prices, "close_adj", "close")
prices[, let(adj_rate = NULL)]

# Remove some columns and reorder them
prices = prices[, .(instrument_id, symbol, ts_event, open, high, low, close, volume, close_raw)]

# Save minute data
write_parquet(prices, "F:/databento/minute_dt.parquet")
