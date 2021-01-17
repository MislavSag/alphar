library(data.table) # fundamental
library(simfinapi)  # fundamental and dailymarket data
library(quantmod)   # quant usefull functions
library(janitor)    # for cleaning column names
library(naniar)     # handling missing values


# sample data
load('./data/data_ml.RData')
head(data_ml)

# get most liquid companies (for al comanies resultsPerPage has to be lower than 1)
data_req <- '
{
  "search": [
    {
      "indicatorId": "4-11",
      "meta": [
        {
          "id": 6,
          "value": "ttm",
          "operator": "eq"
        }
      ],
      "condition": {
        "operator": "be",
        "value": 1000000000
      }
    }
  ],
  "resultsPerPage": 500
}
'
req <- httr::POST('https://simfin.com/api/v1/finder?api-key=8qk9Xc9scFc0Rbpfrx6PLdaiomvi7Dxc',
                  body = data_req, encode = 'json')
filtered_firms <- httr::content(req)
print(paste0('Total number of filtered companies: ', filtered_firms$totalItems))
ids <- as.vector(unlist(lapply(filtered_firms$results, '[', 'simId')))

# get all firms
firms_info <- simfinapi::sfa_get_info(SimFinId = ids, api_key = "8qk9Xc9scFc0Rbpfrx6PLdaiomvi7Dxc")
prices <- sfa_get_prices(SimFinId = ids, ratios = TRUE, api_key = "8qk9Xc9scFc0Rbpfrx6PLdaiomvi7Dxc")
statements <- sfa_get_statement(SimFinId = ids,
                                statement = 'all',
                                period = 'quarters',
                                ttm = TRUE,
                                api_key = "8qk9Xc9scFc0Rbpfrx6PLdaiomvi7Dxc")
DT_statements <- as.data.table(statements)
DT_statements <- janitor::clean_names(DT_statements)

# utils
future_return <- function(x, n) {
  (data.table::shift(x, n, type = 'lead') - x) / x
}

# Date aggregates
DT <- as.data.table(prices)
DT[, `:=`(month=month(Date), quarter=quarter(Date), year=year(Date))]
DT[, halfyear := ifelse(quarter %in% c(1, 2), 1, 2)]

# clean names
DT <- janitor::clean_names(DT)

# Average volumes
DT[, Advt_12M_Usd := frollmean(volume, trading_days_year, na.rm = TRUE), by = list(sim_fin_id)]
DT[, Advt_6M_Usd := frollmean(volume, trading_days_halfyear, na.rm = TRUE), by = list(sim_fin_id)]
DT[, Advt_3M_Usd := frollmean(volume, trading_days_q, na.rm = TRUE), by = list(sim_fin_id)]
tail(DT[, .(date, volume, Advt_12M_Usd, Advt_6M_Usd, Advt_3M_Usd)])

# Average market capitalization
DT[, Mkt_Cap_12M_Usd := frollmean(market_cap, trading_days_year, na.rm = TRUE), by = list(sim_fin_id)]
DT[, Mkt_Cap_6M_Usd := frollmean(market_cap, trading_days_halfyear, na.rm = TRUE), by = list(sim_fin_id)]
DT[, Mkt_Cap_3M_Usd := frollmean(market_cap, trading_days_q, na.rm = TRUE), by = list(sim_fin_id)]
tail(DT[, .(date, market_cap, Mkt_Cap_12M_Usd, Mkt_Cap_6M_Usd, Mkt_Cap_3M_Usd)])

# Volatility
DT[, Vol1Y_Usd := frollapply(close, trading_days_year, sd), by = list(sim_fin_id)]
DT[, Vol3Y_Usd := frollapply(close, trading_days_year * 3, sd), by = list(sim_fin_id)]
tail(DT[, .(date, close, Vol1Y_Usd, Vol3Y_Usd)])

# Labels
DT <- DT[, .SD[date == data.table::last(date)] , by = .(sim_fin_id, year, month)]
DT <- DT[,`:=`(R1M_Usd = future_return(adj_close, 1),
               R3M_Usd = future_return(adj_close, 3),
               R6M_Usd = future_return(adj_close, 6),
               R12M_Usd = future_return(adj_close, 12)), by = .(sim_fin_id)]
head(DT[, .(date, adj_close, R1M_Usd, R3M_Usd, R6M_Usd, R12M_Usd)])

# merge statements and prices
factors <- DT_statements[DT, on = c(sim_fin_id = 'sim_fin_id', publish_date = "date"), roll = Inf]
factors[, .(publish_date, year, month)]

# add other ratios
factors[, Div_Yld := dividends_per_share / close]
factors[, Div_Yld := ifelse(Div_Yld)]
factors[, .(sim_fin_id, publish_date, close, dividends_per_share, Div_Yld)]

# Momentum
factors[, Mom_11M_Usd := data.table::shift(close, 12)/data.table::shift(close, 1)-1, by = list(sim_fin_id)]
factors[, Mom_5M_Usd := data.table::shift(close, 5)/data.table::shift(close, 1)-1, by = list(sim_fin_id)]
head(factors[, c('publish_date', 'close', 'Mom_11M_Usd', 'Mom_5M_Usd')], 15)

# help indicators
factors[, `:=`(
  capital_employed = total_assets - short_term_debt
)]

# main indicators
factors[, `:=`(
  opearintg_assets = cash_cash_equivalents + inventories + accounts_notes_receivable,
  net_opearintg_assets = cash_cash_equivalents + inventories + accounts_notes_receivable + payables_accruals

)]


factors[, `:=`(
  # free cash flow indicators
  fcf_total_assets = free_cash_flow / total_assets,             # free cash flow on total assets
  fcf_margin = free_cash_flow / net_revenue_after_provisions,   # free cash flow margin
  fcf_capital_employed = free_cash_flow / capital_employed      # free cash flow on capital employed

)]

colnames(factors)[grep('paya', colnames(factors))]


# PREPARE DATA FOR ANALYSIS ----------------------------------------------------------------

library(ggplot2)

# prepare data for anallysis
data <- as_tibble(factors)
colnames(data)[which(colnames(data) == 'publish_date')] <- 'date'

# number of firms
data %>%
  group_by(date) %>%                                   # Group by date
  dplyr::summarize(nb_assets = sim_fin_id %>%                   # Count nb assets
              as.factor() %>% nlevels()) %>%
  ungroup() %>%
  ggplot(aes(x = date, y = nb_assets)) + geom_col()

# features
features <- c("Div_Yld", "earnings_per_share_diluted",
              "Mkt_Cap_12M_Usd", "Mom_11M_Usd",
              "net_cash_from_operating_activities",
              "price_to_book_value_ttm", "Vol1Y_Usd")
features_short <- c("Div_Yld", "earnings_per_share_diluted",
                    "Mkt_Cap_12M_Usd", "Mom_11M_Usd",
                    "net_cash_from_operating_activities",
                    "price_to_book_value_ttm", "Vol1Y_Usd")

# labels
data <- data %>%
  group_by(date) %>%                                   # Group by date
  mutate(R1M_Usd_C = R1M_Usd > median(R1M_Usd),        # Create the categorical labels
         R12M_Usd_C = R12M_Usd > median(R12M_Usd)) %>%
  ungroup() %>%
  mutate_if(is.logical, as.factor)

# training / test set
separation_date <- as.Date("2014-01-15")
training_sample <- filter(data, date < separation_date)
testing_sample <- filter(data, date >= separation_date)

# keep key variables in memory
stock_ids <- levels(as.factor(data$sim_fin_id)) # A list of all stock_ids
stock_days <- data %>%                          # Compute the number of data points per stock
  group_by(sim_fin_id) %>%
  dplyr::summarize(nb = n()) %>%
  dplyr::ungroup()
stock_ids_short <- stock_ids[which(stock_days$nb == max(stock_days$nb))] # Stocks with full data
returns <- data %>%                              # Compute returns, in matrix format, in 3 steps:
  filter(sim_fin_id %in% stock_ids_short) %>%    # 1. Filtering the data
  dplyr::select(date, sim_fin_id, R1M_Usd) %>%   # 2. Keep returns along with dates & firm names
  dplyr::distinct() %>%
  tidyr::spread(key = sim_fin_id, value = R1M_Usd)      # 3. Put in matrix shape



# DESCRIPTIVE ANALYSIS ----------------------------------------------------

# size portfolios
data %>%
  select(date, year, Mkt_Cap_12M_Usd, R1M_Usd) %>%
  na.omit(.) %>%
  group_by(date) %>%
  mutate(large = Mkt_Cap_12M_Usd > median(Mkt_Cap_12M_Usd)) %>% # Creates the cap sort
  ungroup() %>%                                                 # Ungroup
  group_by(year, large) %>%                                     # Analyze by year & cap
  dplyr::summarize(avg_return = mean(R1M_Usd)) %>%                     # Compute average return
  ggplot(aes(x = year, y = avg_return, fill = large)) +         # Plot!
  geom_col(position = "dodge") +                                # Bars side-to-side
  theme(legend.position = c(0.8, 0.2)) +                        # Legend location
  theme(legend.title=element_blank()) +      # x/y aspect ratio
  scale_fill_manual(values=c("#F87E1F", "#0570EA"), name = "",  # Colors
                    labels=c("Small", "Large"))  +
  ylab("Average returns") + theme(legend.text=element_text(size=9))



# DATA PREPROCESSING ------------------------------------------------------

# choose
data_short <- data[, c('sim_fin_id', 'date', features_short,
                       'R1M_Usd', 'R3M_Usd', 'R6M_Usd', 'R12M_Usd')]

# scaling predictors
gg_miss_var(data_short)
vis_miss(data_short)
dim(data_short)

head(data[, c('sim_fin_id', 'date', 'dividends_per_share')], 30)


