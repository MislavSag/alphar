library(data.table)
library(fs)
library(arrow)
library(lubridate)
library(ggplot2)


# setup
DATA_PATH = "F:/equity/usa/minute-adjusted"

# import sample of minute data
files =dir_ls(DATA_PATH)
files_sample = sample(files, 5)
dt = lapply(files_sample, read_parquet)
names(dt) = path_ext_remove(path_file(names(dt)))
dt = rbindlist(dt, idcol = "symbol")

# check timezone
dt[, date := with_tz(date, "America/New_York")]

# viualize close series
3seq_ind = seq(1, nrow(dt), by = 100)
ggplot(dt[seq_ind, .(date, symbol = symbol, close)], aes(x = date, y = close, color = as.factor(symbol))) +
  geom_line() +
  labs(title = "Close prices", x = "", y = "Close price in $")

#

