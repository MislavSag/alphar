library(data.table)
library(arrow)


# DATA --------------------------------------------------------------------
# Import benefits ownership data
bo = read_parquet("F:/data/equity/us/fundamentals/beneficial.parquet")

# Set data types
bo[, filingDate := as.Date(filingDate)]
bo[, percentOfClass := as.numeric(percentOfClass)]

# Keep unique rows
bo = unique(bo)

# Sort
setorder(bo, symbol, filingDate)

#
bo[symbol == "AAPL"]
bo[symbol == "AAPL"][filingDate == "2024-02-14"]

# Aggregate percent of class for every symbol and date
bo_agg = bo[, .(symbol, filingDate, nameOfReportingPerson, percentOfClass)]
bo_agg = unique(bo_agg)
bo_agg = bo_agg[, .(total  = sum(percentOfClass)), by = .(symbol, filingDate)]

# Test
bo_agg[, all(total < 100)]
bo_agg[total > 100]
unique(bo[, .(symbol, filingDate, nameOfReportingPerson, percentOfClass)])[symbol == "RELL" & filingDate == "2013-06-10"]

# Remove observations where percent isgreater than 100
bo_agg = bo_agg[total < 100]

# Check
plot(as.xts.data.table(bo_agg[symbol == "AAPL", .(filingDate, total)]))
plot(as.xts.data.table(bo_agg[symbol == "V", .(filingDate, total)]))
