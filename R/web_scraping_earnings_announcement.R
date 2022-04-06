library(data.table)
library(fmpcloudr)
library(httr)
library(tidyr)
library(xts)
library(eventstudies)
library(future.apply)


# set fmpcloudr api token
API_KEY = "15cd5d0adf4bc6805a724b4417bbaafc"
fmpc_set_token(API_KEY)



# EARNING ANNOUNCEMENT ----------------------------------------------------

# get earnings announcements
url <- "https://financialmodelingprep.com/api/v3/earning_calendar"
get_earnings_announcement <- function(from, to) {
  p <- GET(url, query = list(from = from, to = to, apikey = API_KEY))
  result <- rbindlist(httr::content(p))
  return(result)
}

# scrap data
dates_from <- seq.Date(as.Date("1980-01-01"), Sys.Date(), by = 3)
dates_to <- dates_from + 3
earnings_announcements <- lapply(seq_along(dates_from), function(i) {
  get_earnings_announcement(dates_from[i], dates_to[i])
})

# save data
results <- rbindlist(earnings_announcements)
fwrite(results, paste0("D:/fundamental_data/earnings_announcement/ea-", Sys.Date(), ".csv"))



# TRANSCRIPTS -------------------------------------------------------------

# load above data
results <- fread(paste0("D:/fundamental_data/earnings_announcement/ea-2021-07-09.csv"))

# get transcripts
get_earning_call_transcript <- function(symbol, year = 2020, api_key) {
  url = paste0("https://financialmodelingprep.com/api/v4/batch_earning_call_transcript/", symbol)
  req <- content(GET(url, query = list(year = year, apikey = api_key)))
  result <- rbindlist(req)
}

# scrap and save data
scraped <- gsub(".csv", "", list.files("D:/fundamental_data/transcripts"))
symbols_transcript <- setdiff(unique(results$symbol), scraped)
transcripts <- lapply(symbols_transcript, function(tick) {
  print(tick)
  transcript_symbol <- lapply(data.table::year(min(results$date)):2021, function(y) {
    get_earning_call_transcript(tick, y, API_KEY)
  })
  transcript_symbol <- rbindlist(transcript_symbol)
  fwrite(transcript_symbol, paste0("D:/fundamental_data/transcripts/", tick, ".csv"))
  transcript_symbol
})
# transcripts <- rbindlist(transcripts)
# transcripts[, datetime := as.POSIXct(date)]
# table(format.POSIXct(transcripts$datetime, "%H:%M:%S")) # frequenciese of transcript times; most are > 17:00
# transcripts[, date := as.Date(datetime)]
# transcripts[, date_transcript := date] # we will need date which will be discard in merge
# transcripts <- transcripts[!duplicated(transcripts[, .(symbol, date)])] # remove one duplicate



# PRICE DATA --------------------------------------------------------------

# get daily market data for all stocks
events_symbols <- fread(paste0("D:/fundamental_data/earnings_announcement/ea-2021-07-09.csv"))
plan(multicore(workers = 2L))
prices <- lapply(unique(events_symbols$symbol), fmpc_price_history_spldiv, startDate = min(events_symbols$date) - 60)
firm_returns <- rbindlist(prices, fill = TRUE)
fwrite(firm_returns, "D:/fundamental_data/daily_data/daily_prices.csv")



# CREATE LABELS -----------------------------------------------------------

# prices
prices <- fread("D:/fundamental_data/daily_data/daily_prices.csv")
prices[, returns := adjClose / data.table::shift(adjClose) - 1, by = symbol]
firm_returns <- as.data.table(pivot_wider(prices, id_cols = date, names_from = symbol, values_from = returns))

# events
events <- fread("D:/fundamental_data/earnings_announcement/ea-2021-07-09.csv")
events <- events[, .(symbol, date)]
data.table::setnames(events, c("name", "when"))
events <- na.omit(events)

# get stock market returns which we will use for market (or excess) model
spy <- fmpc_price_history("SPY", startDate =  min(events$when) - 60, Sys.Date())
market_returns <- xts(spy$changePercent / 100, order.by = spy$date)

# apply to every symbol
events_symbol <- na.omit(unique(events$name))
events_symbol <- setdiff(events_symbol, gsub(".csv", "", list.files("D:/fundamental_data/car/labels")))
event_horizon <- 30
for (s in events_symbol) {

  # events and price sample
  print(s)
  events_ <- events[name %in% s]
  events_ <- as.data.frame(events_)
  events_$when <- as.Date(events_$when)
  events_ <- events_[, c("when", "name")]
  prices_ <- prices[symbol %in% s]

  # check if prices data available
  if (nrow(prices_) == 0) {
    next()
  }

  # prepare prices
  firm_returns <- as.data.table(pivot_wider(prices_, id_cols = date, names_from = symbol, values_from = returns))
  firm_returns <- firm_returns[-1]
  firm_returns <- as.xts.data.table(firm_returns)

  # check if data available
  if (length(firm_returns) == 0) {
    next()
  }

  # skip some symbols
  if (s %in% c("ATC", "GAHC", "GRST", "PPCB", "MCOA", "POSH", "WOOF", "AMR",
               "LDI", "AGL", "NVOS", "AUSAF")) {
    next()
  }

  # event studies
  es_results <- lapply(1:nrow(events_), function(i) {
    subsample <- as.zoo(firm_returns[paste0(events_$when[i] - 100, "/", events_$when[i] + 100)])
    if (length(subsample) == 0) {
      NULL
    } else {
      es <- eventstudy(firm.returns = subsample,
                       event.list = events_[i, ],
                       event.window = event_horizon,
                       type = "marketModel",
                       to.remap = FALSE,
                       inference = FALSE,
                       model.args = list(
                         market.returns = as.zoo(market_returns)
                       )
      )
    }
  })

    # outcomes
  outcomes <- lapply(es_results, function(x) x$outcomes)
  outcomes[unlist(lapply(outcomes,is.null))] <- "nodata"
  abnormal_returns <- lapply(es_results, function(x) x$result)
  abnormal_returns_test <- lapply(abnormal_returns, function(x) x[(length(x)-event_horizon-1):length(x)])
  abnormal_returns_test_last <- lapply(abnormal_returns_test, function(x) prod(1 + x) - 1)

  # labels
  labels <- cbind.data.frame(symbol = s,
                             datetime = events_$when,
                             outcomes = unlist(outcomes),
                             abnormal_returns_test_last = unlist(abnormal_returns_test_last))
  fwrite(labels, paste0("D:/fundamental_data/car/labels/", s, ".csv"))

  # returns
  abnormal_returns_dt <- lapply(abnormal_returns, as.data.table)
  abnormal_returns_dt <- lapply(abnormal_returns_dt, t)
  abnormal_returns_dt <- lapply(abnormal_returns_dt, function(x) {
    print(length(x) == 0)
    if (length(x) == 0) {
      x <- t(as.matrix(rep(NA, 60)))
    }
    x
  })
  abnormal_returns_dt <- as.data.table(do.call(rbind, abnormal_returns_dt))
  abnormal_returns_dt <- cbind(symbol = s,
                               datetime = events_$when,
                               outcomes = unlist(outcomes), abnormal_returns_dt)
  fwrite(abnormal_returns_dt, paste0("D:/fundamental_data/car/returns/", s, ".csv"))
}



# CREATE FEATURES ---------------------------------------------------------

#### THERE IS A MEMORY LEAK PROBLEM SO I HAVE TO CLOSE AND OPEN ER STUDIO SEVERAL TIMES ###

# import prices
prices <- fread("D:/fundamental_data/daily_data/daily_prices.csv")
prices[, returns := adjClose / data.table::shift(adjClose) - 1, by = symbol]

# create catch22 features
price_sybmols <- unique(prices$symbol)
price_sybmols_scraped <- gsub(".csv|-22", "", list.files("D:/fundamental_data/catch22"))
price_sybmols <- setdiff(price_sybmols, price_sybmols_scraped)
for (s in price_sybmols[1:800]) {
    print(s)

    # data sample
    sample_ <- copy(prices)
    sample_ <- sample_[symbol == s]

    # create catch 22 features
    n <- 22
    sample_[, `:=`(
      CO_Embed2_Dist_tau_d_expfit_meandiff = frollapply(adjClose, n, Rcatch22::CO_Embed2_Dist_tau_d_expfit_meandiff),
      CO_f1ecac = frollapply(adjClose, n, Rcatch22::CO_f1ecac),
      CO_FirstMin_ac = frollapply(adjClose, n, Rcatch22::CO_FirstMin_ac),
      CO_HistogramAMI_even_2_5 = frollapply(adjClose, n, Rcatch22::CO_HistogramAMI_even_2_5),
      CO_trev_1_num = frollapply(adjClose, n, Rcatch22::CO_trev_1_num),
      DN_HistogramMode_10 = frollapply(adjClose, n, Rcatch22::DN_HistogramMode_10),
      DN_HistogramMode_5 = frollapply(adjClose, n, Rcatch22::DN_HistogramMode_5),
      DN_OutlierInclude_n_001_mdrmd = frollapply(adjClose, n, Rcatch22::DN_OutlierInclude_n_001_mdrmd),
      DN_OutlierInclude_p_001_mdrmd = frollapply(adjClose, n, Rcatch22::DN_OutlierInclude_p_001_mdrmd),
      FC_LocalSimple_mean1_tauresrat = frollapply(adjClose, n, Rcatch22::FC_LocalSimple_mean1_tauresrat),
      FC_LocalSimple_mean3_stderr = frollapply(adjClose, n, Rcatch22::FC_LocalSimple_mean3_stderr),
      IN_AutoMutualInfoStats_40_gaussian_fmmi = frollapply(adjClose, n, Rcatch22::IN_AutoMutualInfoStats_40_gaussian_fmmi),
      MD_hrv_classic_pnn40 = frollapply(adjClose, n, Rcatch22::MD_hrv_classic_pnn40),
      PD_PeriodicityWang_th0_01 = frollapply(adjClose, n, Rcatch22::PD_PeriodicityWang_th0_01),
      SB_BinaryStats_diff_longstretch0 = frollapply(adjClose, n, Rcatch22::SB_BinaryStats_diff_longstretch0),
      SB_BinaryStats_mean_longstretch1 = frollapply(adjClose, n, Rcatch22::SB_BinaryStats_mean_longstretch1),
      SB_MotifThree_quantile_hh = frollapply(adjClose, n, Rcatch22::SB_MotifThree_quantile_hh),
      SB_TransitionMatrix_3ac_sumdiagcov = frollapply(adjClose, n, Rcatch22::SB_TransitionMatrix_3ac_sumdiagcov),
      SC_FluctAnal_2_dfa_50_1_2_logi_prop_r1 = frollapply(adjClose, n, Rcatch22::SC_FluctAnal_2_dfa_50_1_2_logi_prop_r1),
      SC_FluctAnal_2_rsrangefit_50_1_logi_prop_r1 = frollapply(adjClose, n, Rcatch22::SC_FluctAnal_2_rsrangefit_50_1_logi_prop_r1),
      SP_Summaries_welch_rect_area_5_1 = frollapply(adjClose, n, Rcatch22::SP_Summaries_welch_rect_area_5_1),
      SP_Summaries_welch_rect_centroid = frollapply(adjClose, n, Rcatch22::SP_Summaries_welch_rect_centroid)
    )]

    # save
    cols <- c("symbol", "date",
              colnames(sample_)[which(colnames(sample_) == "CO_f1ecac"):ncol(sample_)])
    fwrite(sample_[, ..cols], paste0("D:/fundamental_data/catch22/", s, "-", n, ".csv"))

    # remove hgarbage collection
    gc()
  }



# FUNDMAENTAL DATA ---------------------------------------------------------

# get events data
events <- fread(paste0("D:/fundamental_data/earnings_announcement/ea-2021-07-09.csv"))

# get financial growth ratios
get_fundamental_data <- function(symbol, api_tag) {
  url <- paste0("https://financialmodelingprep.com/api/v3/", api_tag, "/", symbol)
  req <- GET(url, query = list(period = "quarter", limit = 1000, apikey = API_KEY))
  fdata <- rbindlist(content(req))
  return(fdata)
}

# get income statement reports
get_pl <- function(symbol) {
  url <- paste0("https://financialmodelingprep.com/api/v3/income-statement/", symbol)
  req <- GET(url, query = list(period = "quarter", limit = 1000, apikey = API_KEY))
  fdata <- rbindlist(content(req))
  return(fdata)
}

# get fundamental data
pl <- lapply(unique(events$symbol), get_pl)
pl_dt <- rbindlist(pl)
fwrite(pl_dt, "D:/fundamental_data/pl.csv")

# key metrics
metrics <- lapply(unique(events$symbol), function(x) get_fundamental_data(x, "key-metrics"))
metrics_dt <- rbindlist(metrics)
fwrite(metrics_dt, "D:/fundamental_data/key_metrics.csv")

# financial growth
fg <- lapply(unique(events$symbol), function(x) get_fundamental_data(x, "financial-growth"))
fg_dt <- rbindlist(fg)
fwrite(fg_dt, "D:/fundamental_data/fin_growth.csv")
