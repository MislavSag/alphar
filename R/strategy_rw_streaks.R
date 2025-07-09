library(finutils)
library(fastverse)


# Import daily data
prices = qc_daily(
  file_path = "F:/lean/data/stocks_daily.csv",
  min_obs = 252,
  price_threshold = 0.0000001,
  add_dv_rank = TRUE,
  add_day_of_month = FALSE
)

# Calculate down and up move
prices[, let(
  down_move = fifelse(returns < 0, -1, 0),
  up_move = fifelse(returns > 0, 1, 0)
), by = symbol]

# Count strikes. If change sign, reset to 0
prices[, let(
  down_streaks = rleid(down_move),
  up_streaks   = rleid(up_move)),
  by = symbol
]
prices[, let(
  down_streaks = seq_along(down_move) ,
  up_streaks = seq_along(up_move)),
  by = .(symbol, down_streaks)]
prices[down_move == 0, down_streaks := 0]
prices[up_move == 0, up_streaks := 0]

# Checks
head(prices[, .(symbol, date, close, returns, down_move, down_streaks)], 30)
tail(prices[, .(symbol, date, close, returns, down_move, down_streaks)], 30)


DT = data.table(grp=rep(c("A", "B", "C", "A", "B"), c(2,2,3,1,2)), value=1:10)
rleid(DT$grp) # get run-length ids
rleidv(DT, "grp") # same as above

rleid(DT$grp, prefix="grp") # prefix with 'grp'

# get sum of value over run-length groups
DT[, sum(value), by=.(grp, rleid(grp))]
DT[, sum(value), by=.(grp, rleid(grp, prefix="grp"))]
