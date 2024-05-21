library(data.table)
library(arrow)


# DATA --------------------------------------------------------------------
# Import benefits ownership data
bo = read_parquet("F:/data/equity/us/fundamentals/beneficial.parquet")

#
bo[symbol == "AAPL"]
bo[symbol == "AAPL"][filingDate == "2024-02-14"]

