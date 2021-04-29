library(data.table)
library(lubridate)
library(midasml)
library(alfred)
library(BatchGetSymbols)


# sp500 symbols
symbols <- c("AAPL", "T") # for test

# get market data
prices <- BatchGetSymbols(symbols, first.date = as.Date("1990-01-01"))
prices <- as.data.table(na.omit(prices$df.tickers))
setnames(prices, c("ticker", "ref.date"), c("symbol", "date"))

# alfred data
permits <- as.data.table(get_alfred_series("PERMIT"))[realtime_period == max(realtime_period), c(1, 3)]
tcu <- as.data.table(get_alfred_series("TCU"))[realtime_period == max(realtime_period), c(1, 3)]
unrate <- as.data.table(get_alfred_series("UNRATE"))[realtime_period == max(realtime_period), c(1, 3)]

# macro data
macro_data <- merge(permits, tcu, by = c('date'), all.x = TRUE, all.y = FALSE)
macro_data <- merge(macro_data, unrate, by = c('date'), all.x = TRUE, all.y = FALSE)
macro_vars_to_growth_rates <- colnames(macro_data)[c(2, 3)]
macro_data[, (macro_vars_to_growth_rates) := lapply(.SD, function(x) x / shift(x) - 1), .SDcols = macro_vars_to_growth_rates]
macro_data <- na.omit(macro_data)

# dependent variable
prices[, future_return := shift(price.adjusted, 23, type = 'lead') / price.adjusted - 1]
prices[, date_end_month := ceiling_date(date, "month") - days(1)]
dependent_variable <- prices[, .SD[which.max(date)], by = .(symbol, date_end_month)]
dependent_variable <- dependent_variable[, .(symbol, future_return, date_end_month)]

# realtime covariets
realtime_x <- prices[, .(symbol, date, ret.adjusted.prices, volume)]

# define estimation period
est.start <- as.Date("2005-01-01")
est.end <- as.Date("2016-01-01")


# parameters (this will go to function arguments later). I will filter only one firm.
y_data = dependent_variable[symbol == "AAPL", .(date_end_month, future_return)]
x_macro_data = as.data.frame(macro_data)
x_real_time = as.data.frame(realtime_x[symbol == "AAPL", .(date, ret.adjusted.prices, volume)])
horizon = 1L
macro_delay = 0 # always use realtime data!
est_end = est.end
est_start = est.start
x_lag = 5 # same lags for all covariates for simplicity
legendre_degree = 3L
disp_flag = FALSE
x.quarterly_group = NULL
group_ar_lags = FALSE
real_time_predictions = FALSE

# checks and init
dim_macro <- dim(x_macro_data)[2]-1
dim_real_time <- dim(x_real_time)[2]-1

# storage
x_str_out <- x_str <- NULL
x_unstr_out <- x_unstr <- NULL
x_average_out <- x_average <- NULL
group_index <- group_index_un <- group_index_av <- 0
ref_in <- ref_out <- NULL

# # macro covarites
# x_macro_date <- x_macro_data$date
# x_macro_data <- x_macro_data[,-1]
# data.refdate <- dependent_variable$date_end_month
# data.refdate <- data.refdate %m-% months(macro_delay)
# est.start_m <- est_start
# est.end_m <- est_end
# est.start_m <- est.start_m %m-% months(macro_delay)
# est.end_m <- est.end_m %m-% months(macro_delay)
# data.refdate <- data.refdate  %m-% months(horizon)
# est.end_m <- est.end_m  %m-% months(horizon)
#
# for (j_macro in seq(dim_macro)){
#   j_data <- scale(x_macro_data[,j_macro], center = TRUE, scale = TRUE)
#   # get MIDAS structure:
#   tmp <- mixed_freq_data_single(data.refdate = data.refdate, data.x = j_data, data.xdate = x_macro_date,
#                                 x.lag = x_lag, horizon = 0, est_start, est.end_m, disp.flag = disp_flag)
#
#   if(j_macro==1){
#     ref_in <- tmp$est.refdate
#     ref_out <- tmp$out.refdate
#     ref_in <- ref_in %m+% months(macro_delay) %m+% months(horizon)
#     if(!is.null(ref_out))
#       ref_out <- ref_out %m+% months(macro_delay) %m+% months(horizon)
#   }
#   # get Legendre weights:
#   tmp_w <- lb(legendre_degree,a=0,b=1,jmax=x_lag)
#   # store aggregated case:
#   x_str <- cbind(x_str, tmp$est.x%*%tmp_w)
#   x_str_out <- cbind(x_str_out, tmp$out.x%*%tmp_w)
#   # store unrestricted case:
#   x_unstr <- cbind(x_unstr, tmp$est.x)
#   x_unstr_out <- cbind(x_unstr_out, tmp$out.x)
#   # store averages:
#   x_average <- cbind(x_average, rowMeans(tmp$est.x))
#   x_average_out <- cbind(x_average_out, rowMeans(tmp$out.x))
#   # store group indices:
#   group_index <- c(group_index, rep(max(group_index)+1,times=legendre_degree+1))
#   group_index_un <- c(group_index_un, rep(max(group_index_un)+1,times=x_lag))
#   group_index_av <- c(group_index_av, max(group_index_av)+1)
# }

# realtime covarites
x_realtime_date <- x_real_time$date
x_realtime_data <- x_real_time[,-1]
data.refdate <- dependent_variable$date_end_month
est_end <- est_end  %m-% months(horizon)
data.refdate <- data.refdate  %m-% months(horizon)

for (j_real_time in seq(dim_real_time)){
  j_data <- scale(x_realtime_data[,j_real_time], center = TRUE, scale = TRUE)
  # get MIDAS structure:
  tmp <- mixed_freq_data_single(data.refdate = data.refdate, data.x = j_data, data.xdate = x_realtime_date,
                                x.lag = x_lag, horizon = 0, est_start, est_end, disp.flag = disp_flag)

  if(j_real_time==1){
    ref_in <- tmp$est.refdate
    ref_out <- tmp$out.refdate
    lubridate::month(ref_in) <- lubridate::month(ref_in)+horizon
    if(!is.null(ref_out))
      lubridate::month(ref_out) <- lubridate::month(ref_out)+horizon
  }
  # get Legendre weights:
  tmp_w <- lb(legendre_degree,a=0,b=1,jmax=x_lag)
  # store aggregated case:
  x_str <- cbind(x_str, tmp$est.x%*%tmp_w)
  x_str_out <- cbind(x_str_out, tmp$out.x%*%tmp_w)
  # store unrestricted case:
  x_unstr <- cbind(x_unstr, tmp$est.x)
  x_unstr_out <- cbind(x_unstr_out, tmp$out.x)
  # store averages:
  x_average <- cbind(x_average, rowMeans(tmp$est.x))
  x_average_out <- cbind(x_average_out, rowMeans(tmp$out.x))
  # store group indices:
  group_index <- c(group_index, rep(max(group_index)+1,times=legendre_degree+1))
  group_index_un <- c(group_index_un, rep(max(group_index_un)+1,times=x_lag))
  group_index_av <- c(group_index_av, max(group_index_av)+1)
}

if(is.null(ref_in) || is.null(ref_out)) {
  message("ref dates were not computed. likely that both macro and real time datasets where not inputed. at least one dataset must be inputed.")
}

# drop initializing zero:
group_index <- group_index[-1]
group_index_un <- group_index_un[-1]
group_index_av <- group_index_av[-1]

# sort quarterly group of covariates and lags
y.data_in <- y_data[as.Date(y_data$date_end_month) %in% ref_in,]
y.data_out <- y_data[as.Date(y_data$date_end_month) %in% ref_out,]

y_in <- y.data_in$future_return
y_in_dates <- y.data_in$date_end_month
y_out <- y.data_out$future_return
y_out_dates <- y.data_out$date_end_month

output <- list(y_in = y_in, y_in_dates = y_in_dates, y_out = y_out, y_out_dates = y_out_dates,
               x_str = x_str, x_str_out = x_str_out, x_unstr = x_unstr, x_unstr_out = x_unstr_out, x_average = x_average, x_average_out = x_average_out,
               group_index = group_index, group_index_un = group_index_un, group_index_av = group_index_av)



# FUNDAMENTAL DATA --------------------------------------------------------

# set api token
APIKEY = "15cd5d0adf4bc6805a724b4417bbaafc"
fmpc_set_token(APIKEY)

# utils

trading_days_year <- 256
trading_days_halfyear <- trading_days_year / 2
trading_days_q <- trading_days_year / 4

# market capitalization
market_cap <- import_daily(path = 'D:/fundamental_data/market_cap', extension = 'csv')
market_cap <- market_cap[symbol %in% symbols]

# import daily data
# prices <- import_daily(path = 'D:/market_data/equity/usa/day/trades', extension = 'csv')
prices <- market_cap[prices, on = c('symbol', 'date'), roll = -Inf]
prices[, `:=`(year = year(date), month = month(date))]

# Average volumes
prices[, Advt_12M_Usd := frollmean(volume, trading_days_year, na.rm = TRUE), by = .(symbol)]
prices[, Advt_6M_Usd := frollmean(volume, trading_days_halfyear, na.rm = TRUE), by = .(symbol)]
prices[, Advt_3M_Usd := frollmean(volume, trading_days_q, na.rm = TRUE), by = .(symbol)]

# Rolling volatility
prices[, Vol1Y_Usd := roll_sd(adjusted, trading_days_year), by = list(symbol)]
prices[, Vol3Y_Usd := roll_sd(adjusted, trading_days_year * 3), by = list(symbol)]
tail(prices[, .(date, close, Vol1Y_Usd, Vol3Y_Usd)])

# Average market capitalization
prices[, Mkt_Cap_12M_Usd := frollmean(as.numeric(marketCap), trading_days_year, na.rm = TRUE), by = list(symbol)]
prices[, Mkt_Cap_6M_Usd := frollmean(as.numeric(marketCap), trading_days_halfyear, na.rm = TRUE), by = list(symbol)]
prices[, Mkt_Cap_3M_Usd := frollmean(as.numeric(marketCap), trading_days_q, na.rm = TRUE), by = list(symbol)]
tail(prices[, .(date, marketCap, Mkt_Cap_12M_Usd, Mkt_Cap_6M_Usd, Mkt_Cap_3M_Usd)], 10)

# filter last observation in month and convert it to end of month date
prices <- prices[, .SD[which.max(date)] , by = .(symbol, year, month)]
prices[, date := ceiling_date(date, "month") - days(1)]

# Momentum
prices[, Mom_11M_Usd := data.table::shift(close, 12)/data.table::shift(close, 1)-1, by = list(symbol)]
prices[, Mom_5M_Usd := data.table::shift(close, 5)/data.table::shift(close, 1)-1, by = list(symbol)]
head(prices[, c('date', 'close', 'Mom_11M_Usd', 'Mom_5M_Usd')], 15)

# Labels
prices <- prices[,`:=`(R1M_Usd = future_return(adjusted, 1),
                       R3M_Usd = future_return(adjusted, 3),
                       R6M_Usd = future_return(adjusted, 6),
                       R12M_Usd = future_return(adjusted, 12)), by = .(symbol)]
head(prices[, .(date, adjClose, R1M_Usd, R3M_Usd, R6M_Usd, R12M_Usd)])

# labels for classification
prices[, `:=`(R1M_Usd_C = as.factor(R1M_Usd > median(R1M_Usd, na.rm = TRUE)),
              R12M_Usd_C = as.factor(R12M_Usd > median(R12M_Usd, na.rm = TRUE))), by = date]

# import balance sheet data
balance <- import_daily(path = "D:/fundamental_data/balance_sheet", extension = 'csv')
balance <- as.data.table(balance)
balance <- balance[symbol %in% symbols]

# import ratios
ratios <- import_daily(path = "D:/fundamental_data/ratios", extension = 'csv')
ratios <- as.data.table(ratios)
ratios <- ratios[symbol %in% symbols]
ratios <- ratios[order(symbol, date)]

# ratios
ratios[, price := pbRatio * bookValuePerShare]
ratios[, eps_ttm := frollsum(netIncomePerShare, 4), by = .(symbol)]
ratios[, pe_ttm := price / eps_ttm]

# merge ratios and financial statements
ratios <- balance[ratios, on = .(symbol, date)]

# merge ratios and prices
ratios[, date_end_month := ceiling_date(fillingDate, "month") - days(1)]
data <- merge(prices, ratios, by.x = c('symbol', 'date'), by.y = c('symbol', 'date_end_month'), all.x = TRUE, all.y = FALSE)

# define features and labels
labels_reg <- c("R1M_Usd", "R3M_Usd", "R6M_Usd", "R12M_Usd")
labels_clf <- c("R1M_Usd_C", "R12M_Usd_C")
features <- c(
  "Advt_3M_Usd", "Advt_6M_Usd", "Advt_12M_Usd",
  "bookValuePerShare",
  "cashPerShare", "dividendYield", "eps_ttm",
  "Mkt_Cap_3M_Usd", "Mkt_Cap_6M_Usd", "Mkt_Cap_12M_Usd",
  "Vol1Y_Usd", "Vol3Y_Usd",
  "enterpriseValue",
  "freeCashFlowPerShare",
  "debtToEquity",
  "Mom_5M_Usd", "Mom_11M_Usd",
  # 'netDebtToEBITDA',
  "netIncomePerShare",
  "operatingCashFlowPerShare",
  "pbRatio", "pe_ttm",
  "revenuePerShare",
  # non ml factor features
  "tangibleBookValuePerShare"
)

# final table
cols <- c("symbol", "date", features, labels_reg, labels_clf)
DT <- data[, ..cols]
variables <- colnames(DT)[3:ncol(DT)]
DT <- DT[, (variables) := lapply(.SD, function(x) na.locf(x, na.rm = FALSE)), by = .(symbol), .SDcols = variables]
tail(DT, 20)






# TEST --------------------------------------------------------------------

data(macro_midasml)
est.start <- as.Date("1990-12-01")
est.end <- as.Date("2017-03-01")
rgdp.data <- macro_midasml$rgdp.data
rgdp.data <- rgdp.data[rgdp.data$DATE<=as.Date("2017-06-01"),]
qtarget.sort_midasml(y.data = rgdp.data, x.macro.data = macro_midasml$md.data,
                     x.real.time = macro_midasml$text.data, x.quarterly_group = macro_midasml$survey.data,
                     x.lag = 12, legendre_degree = 3,
                     horizon = 1, macro_delay = 1, est.start, est.end,
                     standardize = TRUE, group_ar_lags = FALSE, disp.flag = FALSE)
tail(macro_midasml$md.data)

# params
y.data = rgdp.data
x.macro.data = macro_midasml$md.data
x.real.time = macro_midasml$text.data
x.quarterly_group = macro_midasml$survey.data
x.lag = 12
legendre_degree = 3
horizon = 1
macro_delay = 1
est.start
est.end
standardize = TRUE
group_ar_lags = FALSE
disp.flag = FALSE


if(is.null(x.macro.data)&&is.null(x.real.time))
  stop("both macro and real time data were not inputed. program stops as you need monthly data to compute nowcasts. please input either x.macro.data or x.real.time on the next run.")
dim_macro <- dim_real.time <- dim_quarterly <- 0
if(!is.null(x.macro.data))
  dim_macro <- dim(x.macro.data)[2]-1 # dimension od macro data; -1 because one column is date
if(!is.null(x.real.time))
  dim_real.time <- dim(x.real.time)[2]-1

dim_x <- dim_macro + dim_real.time + 1
if(is.null(x.lag))
  stop("x.lag variable, which defines the lag structure of each covariate, must be specified.")
if(length(x.lag)!=1 && length(x.lag)!=(dim_macro + dim_real.time)) # it is possible to define xlag for every covariate!!!
  stop(paste0("x.lag variable length must be either of size 1 (the same lag structure for x.macro.data and/or x.real.time) or must be of the length size equal to the total number of high-frequency covariates: ",dim_macro + dim_real.time,"."))
if(length(x.lag)==1)
  x.lag <- rep(x.lag,times=(dim_macro + dim_real.time))

if(length(legendre_degree)!=1 && length(legendre_degree)!=dim_x){
  message(paste0("Legendre polynomial degree must be specified the same for all covariates (one number) or a seperate value for each (the length size equals to the total number of covariates). the length of legendre_degree: ", length(legendre_degree), ", number of covaraites that are inputed: ", dim_x,". Legendre degree is set to (for all covariates): ",  legendre_degree[1], " - the first entry in legendre_degree"))
  legendre_degree <- legendre_degree[1]
}
if (length(legendre_degree)==1)
  legendre_degree <- rep(legendre_degree,times=dim_x)

if(macro_delay!=1)
  message("typically macro series is published with 1 month lag. please check if your inputed macro data has different publication lag. the program does not stop, you need to re-run by re-setting macro_delay input.")

# storage

# storage
x_str_out <- x_str <- NULL
x_unstr_out <- x_unstr <- NULL
x_average_out <- x_average <- NULL
group_index <- group_index_un <- group_index_av <- 0
ref_in <- ref_out <- NULL
# computing the macro data if inputed
if(!is.null(x.macro.data)){
  x.macro_date <- x.macro.data$DATE
  x.macro_data <- x.macro.data[,-1]
  data.refdate <- y.data$DATE
  lubridate::month(data.refdate) <- lubridate::month(data.refdate)-macro_delay
  est.start_m <- est.start
  est.end_m <- est.end
  lubridate::month(est.start_m) <- lubridate::month(est.start_m)-macro_delay
  lubridate::month(est.end_m) <- lubridate::month(est.end_m)-macro_delay
  lubridate::month(data.refdate) <- lubridate::month(data.refdate) - horizon
  lubridate::month(est.end_m) <- lubridate::month(est.end_m) - horizon
  if(real_time_predictions){
    if(horizon>=0){
      tmp_date <- data.refdate[length(data.refdate)]
      lubridate::month(tmp_date) <- lubridate::month(tmp_date) + 3
      data.refdate <- c(data.refdate, tmp_date)
    }
  }
  for (j_macro in seq(dim_macro)){
    if(standardize){
      j_data <- scale(x.macro_data[,j_macro], center = TRUE, scale = TRUE)
    } else {
      j_data <- scale(x.macro_data[,j_macro], center = TRUE, scale = TRUE)
    }
    # get MIDAS structure:
    tmp <- mixed_freq_data_single(data.refdate = data.refdate, data.x = j_data, data.xdate = x.macro_date,
                                  x.lag[j_macro], horizon = 0, est.start, est.end_m, disp.flag = disp.flag) #horizon is taken into account by shifting est.end_m back
    if(j_macro==1){
      ref_in <- tmp$est.refdate
      ref_out <- tmp$out.refdate
      lubridate::month(ref_in) <- lubridate::month(ref_in)+macro_delay+horizon
      if(!is.null(ref_out))
        lubridate::month(ref_out) <- lubridate::month(ref_out)+macro_delay+horizon
    }
    # get Legendre weights:
    tmp_w <- lb(legendre_degree[j_macro],a=0,b=1,jmax=x.lag[j_macro])
    # aggregate in-sample:
    x_str <- cbind(x_str, tmp$est.x%*%tmp_w)
    x_str_out <- cbind(x_str_out, tmp$out.x%*%tmp_w)
    # store unrestricted case:
    x_unstr <- cbind(x_unstr, tmp$est.x)
    x_unstr_out <- cbind(x_unstr_out, tmp$out.x)
    # store averages:
    x_average <- cbind(x_average, rowMeans(tmp$est.x))
    x_average_out <- cbind(x_average_out, rowMeans(tmp$out.x))
    # get group indices
    group_index <- c(group_index, rep(max(group_index)+1,times=legendre_degree[j_macro]+1))
    group_index_un <- c(group_index_un, rep(max(group_index_un)+1,times=x.lag[j_macro]))
    group_index_av <- c(group_index_av, max(group_index_av)+1)
  }
}





# library(httr)
#
# data <- content(GET(
#   "https://oss.uredjenazemlja.hr/rest/katHr/lrInstitutions/position?id=2432593&status=1332094865186&x=undefined&y=undefined",
#   add_headers(origin = "https://www.katastar.hr")
# ), as = "parsed", type = "application/json")
#
# print(data)
#
#
# data <- content(GET(
#   "https://oss.uredjenazemlja.hr/rest/katHr/parcelDetails?id=9048388&status=47204326483802",
#   add_headers(origin = "https://www.katastar.hr")
# ), as = "parsed", type = "application/json")
#
# print(data)
#
#
# number = 5447310
# 13314810894620
#
# rand = 9048388
# randStr = as.character(rand)
# n = 0
#
#
# library(V8)
#
# ct <- v8()
#
# x <- ct$eval("(function() {return Math.floor(1e7 * Math.random())})()")
# ct$eval(paste0("(function(e,t){for(var n=0,i=0;i<e.length;i++)n=(n<<5)-n+e.charAt(i).charCodeAt(0),n&=n;return null==t&&(t=e),Math.abs(n).toString().substring(0,6)+(Number(t)<<1)})(",
#                x,
#                ", null)"))
