library(httr)
library(fastverse)
library(ggplot2)
library(PerformanceAnalytics)


# DATA --------------------------------------------------------------------
# Function to clean data
clean = function(res, symbol = "crobex") {
  dt = lapply(res$rows, as.data.table)
  dt = rbindlist(dt)
  dt[, date := as.Date(date, format = "%d.%m.%Y")]
  cols = c("open_value", "high_value", "low_value", "last_value", "turnover")
  dt[, (cols) := lapply(.SD, function(x) gsub("\\.", "", x)), .SDcols = cols]
  dt[, (cols) := lapply(.SD, function(x) gsub(",", ".", x)), .SDcols = cols]
  dt[, (cols) := lapply(.SD, as.numeric), .SDcols = cols]
  dt[, change_prev_close_percentage := NULL]
  dt[, symbol := symbol]
  setorder(dt, date)
  return(dt)
}

# CROBEX
url = "https://zse.hr/json/indexHistory/HRZB00ICBEX6/2010-02-01/2025-02-05/hr?restAPI=https://rest.zse.hr/web/Bvt9fe2peQ7pwpyYqODM/"
p = GET(
  url,
  add_headers(
    "accept-encoding" = "gzip, deflate, br, zstd",
    "user-agent" = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.0.0",
    "Referer" = "https://zse.hr/hr/indeks/365?isin=HRZB00ICBEX6&tab=index_history&date_from=2010-02-01&date_to=2025-02-05"
  )
)
res = content(p)
crobex = clean(res)

# CROBIS
url = "https://zse.hr/json/indexHistory/HRZB00ICRBS8/2010-02-02/2025-02-05/hr?restAPI=https://rest.zse.hr/web/Bvt9fe2peQ7pwpyYqODM/"
p = GET(
  url,
  add_headers(
    "accept-encoding" = "gzip, deflate, br, zstd",
    "user-agent" = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.0.0",
    "Referer" = "https://zse.hr/hr/indeks/365?isin=HRZB00ICRBS8&tab=index_history&date_from=2010-02-02&date_to=2025-02-05"
  )
)
res = content(p)
crobis = clean(res, "crobis")

# Merge CROBEX and CROBIS
dt = rbind(crobex, crobis)

# Plot series
ggplot(dt[symbol == "crobex"], aes(x = date, y = last_value)) +
  geom_line() +
  labs(title = "CROBEX and CROBIS", x = "Date", y = "Value") +
  theme_minimal() +
  theme(legend.position = "bottom")
ggplot(dt[symbol == "crobis"], aes(x = date, y = last_value)) +
  geom_line() +
  labs(title = "CROBEX and CROBIS", x = "Date", y = "Value") +
  theme_minimal() +
  theme(legend.position = "bottom")

# Define datys of month
dt[, month := data.table::yearmon(date)]
setorder(dt, symbol, date)
month_date = unique(dt[, .(month, date)])
setorder(month_date, date)
month_date[, day_of_month := 1:.N, by = .(month)]
dt = month_date[, .(date, day_of_month)][dt, on = c("date")]
dt[, day_of_month := as.factor(day_of_month)]
dt[, .N, by = day_of_month][order(day_of_month)]
dt[day_of_month == 23, day_of_month := 22] # 23 day to 22 day
dt[day_of_month == 22, day_of_month := 21] # not sure about this but lets fo with it

# Calculate returns 5 day before end of the month and returns last 5 days of the month
ret_1 = dt[, .(ret_1 = .SD[day_of_month == 1, open_value] / .SD[day_of_month == 16, last_value] - 1),
   by = .(symbol, month)]
ret_2 = dt[, .(ret_2 = data.table::last(.SD[, last_value]) / .SD[day_of_month == 17, open_value] - 1),
   by = .(symbol, month)]

# Merge ret_1 and ret_2
returns = merge(ret_1, ret_2, by = c("symbol", "month"))
returns = dcast(returns, month ~ symbol, value.var = c("ret_1", "ret_2"))
returns = na.omit(returns)

# Strategy
returns[, strategy := ifelse(ret_1_crobex > ret_1_crobis, ret_2_crobis, ret_2_crobex)]
returns[, zoo::as.Date.yearmon(month)]
returns_xts = as.xts.data.table(returns[, .(zoo::as.Date.yearmon(month), strategy)])
charts.PerformanceSummary(returns_xts)
SharpeRatio.annualized(returns_xts)
Return.cumulative(returns_xts)
Return.annualized(returns_xts)
min(Drawdowns(returns_xts))

# Earn interest on cash
returns[, strategy := ifelse(ret_1_crobex > ret_1_crobis, ret_2_crobis + 0.001, ret_2_crobex + 0.001)]
returns[, zoo::as.Date.yearmon(month)]
returns_xts = as.xts.data.table(returns[, .(zoo::as.Date.yearmon(month), strategy)])
charts.PerformanceSummary(returns_xts)
SharpeRatio.annualized(returns_xts)
Return.cumulative(returns_xts)
Return.annualized(returns_xts)
min(Drawdowns(returns_xts))

# With leverage
charts.PerformanceSummary(returns_xts * 2)
SharpeRatio.annualized(returns_xts * 2)
Return.cumulative(returns_xts * 2)
min(Drawdowns(returns_xts * 2))

# Crobex and CROBIS
crobex_xts = na.omit(as.xts.data.table(crobex[, .(date, ret = last_value / shift(last_value) - 1)]))
charts.PerformanceSummary(crobex_xts)
SharpeRatio.annualized(crobex_xts)
Return.cumulative(crobex_xts)
min(Drawdowns(crobex_xts))

crobis_xts = na.omit(as.xts.data.table(crobex[, .(date, ret = last_value / shift(last_value) - 1)]))
charts.PerformanceSummary(crobex_xts)
SharpeRatio.annualized(crobex_xts)
Return.cumulative(crobex_xts)
min(Drawdowns(crobex_xts))



# ROBUST ANALYSIS ---------------------------------------------------------
# Calculate returns 5 day before end of the month and returns last 5 days of the month
ret_1 = dt[, .(ret_1 = data.table::last(.SD[, last_value]) / .SD[day_of_month == 6, open_value] - 1),
           by = .(symbol, month)]
ret_2 = dt[, .(ret_2 = .SD[day_of_month == 5, last_value] / .SD[day_of_month == 1, open_value] - 1),
           by = .(symbol, month)]

# Merge ret_1 and ret_2
returns = merge(ret_1, ret_2, by = c("symbol", "month"))
returns[, ret_1 := shift(ret_1)]
returns = dcast(returns, month ~ symbol, value.var = c("ret_1", "ret_2"))
returns = na.omit(returns)

# Strategy
returns[, strategy := ifelse(ret_1_crobex > ret_1_crobis, ret_2_crobis, ret_2_crobex)]
returns[, zoo::as.Date.yearmon(month)]
returns_xts = as.xts.data.table(returns[, .(zoo::as.Date.yearmon(month), strategy)])
charts.PerformanceSummary(returns_xts)
SharpeRatio.annualized(returns_xts)
Return.cumulative(returns_xts)
Return.annualized(returns_xts)
min(Drawdowns(returns_xts))

# Quater rebalancing
# Define days of month
dt[, month := data.table::yearmon(date)]
setorder(dt, symbol, date)
month_date = unique(dt[, .(month, date)])
setorder(month_date, date)
month_date[, day_of_month := 1:.N, by = .(month)]
dt = month_date[, .(date, day_of_month)][dt, on = c("date")]
dt[, day_of_month := as.factor(day_of_month)]
dt[, .N, by = day_of_month][order(day_of_month)]
dt[day_of_month == 23, day_of_month := 22] # 23 day to 22 day
dt[day_of_month == 22, day_of_month := 21] # not sure about this but lets fo with it



