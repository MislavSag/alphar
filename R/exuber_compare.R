library(data.table)
library(jsonlite)


# read old strategy
strtaegy_old <- read_json("C:/Users/Mislav/Downloads/5e6fec6bdc2b71112f705bdf1b1a2518.json")
exuber_indicator <- strtaegy_old$Charts$`Exuber Indicators`$Series$`Radf Sum SD`$Values
exuber_indicator <- rbindlist(exuber_indicator)
exuber_indicator[, date := as.POSIXct(x, origin = as.Date("1970-01-01"))]
exuber_indicator <- exuber_indicator[, .(date = date, sum_radf = y)]
fwrite(exuber_indicator, "C:/Users/Mislav/Documents/radf_values.csv")

# new strategy
strtategy_new <- read_json("C:/Users/Mislav/Downloads/352ae6a70e6879584f6eb9259cae961c.json")
exuber_indicator_new <- strtategy_new$Charts$`Exuber Indicators`$Series$`Radf Sum SD`$Values
exuber_indicator_new <- rbindlist(exuber_indicator_new)
exuber_indicator_new[, date := as.POSIXct(x, origin = as.Date("1970-01-01"))]

head(exuber_indicator_new)
head(exuber_indicator)
all.equal(exuber_indicator, exuber_indicator_new)


# options
strtaegy_opt <- read_json("C:/Users/Mislav/Downloads/2cd738c6dac99ded6b68a74b21b97f19.json")
orders <- strtaegy_opt$Orders
orders <- lapply(orders, as.data.table)
orders <- rbindlist(orders, fill = TRUE)
orders[, date := as.Date(Time)]
orders[date %between% c("2015-08-19", "2015-08-20")]
