library(httr)
library(rvest)
library(data.table)
library(fs)
library(ecb)
library(lubridate)
library(OECD)
library(readxl)
library(fredr)
library(MultiATSM)
library(duckdb)
library(PerformanceAnalytics)
library(findata)
library(imfr)
library(duckdb)


# globals
key = "fb7e8cbac4b84762980f507906176c3c"
fredr_set_key(key)
save_path = "F:/macro/yield_curves"

# utils
na_columns <- function(X) apply(X, 2, function(x) any(is.na(x)))
na_columns_p <- function(X, p) !colSums(is.na(X)) < (nrow(X) * (p / 100))


# YIELDS ------------------------------------------------------------------
# help function to convert yields to MultiATSM format
yields_prepare = function(dt_, remove = c("label", "dates")) {
  dt_ = dt_[, tail(.SD, 1), by = .(label, month)]
  if (remove == "label") {
    dt_[, remove := any(is.na(yield)), by = label]
    dt_ = dt_[remove == FALSE]
  }
  dt_ = dcast(dt_, label ~ month, value.var = "yield")
  dt_
}

# yields data
countries = c("US", "China", "EU")
yields_gen = Yields$new(countries = c("US", "China", "EU"))
yields_raw = yields_gen$get_yields("F:/macro/yield_curves/eu_history.csv")
yields_raw[, month := ceiling_date(date, "month")]
yields = yields_raw[, tail(.SD, 1), by = .(label, month)]

# remove labels with missing values
yields[, remove := any(is.na(yield)), by = label]
yields = yields[remove == FALSE]

# keep only labels that exists across all countries
labels_keep = unique(yields[, .(label, m = gsub("Y|_.*", "", label)),
                            by = .(gsub(".*_", "", label))])
labels_keep = labels_keep[, .(label, .N), by = m][N == length(countries), label]
yields = yields[label %in% labels_keep]

# reshape yields to be appropiate for the MultiATSM package
yields = dcast(yields, label ~ month, value.var = "yield")
yields[, `:=`(months = as.integer(gsub("Y|M_.*", "", label)),
              country = gsub(".*_", "", label))]
setorder(yields, country, months)
yields[, `:=`(months = NULL, country = NULL)]
yieldsm = as.matrix(yields, rownames = "label")
na_columns_p(yieldsm, 0.5)
yieldsm = yieldsm[, !na_columns(yieldsm)]
yieldsm[, 1:5]


# INFLATION ---------------------------------------------------------------
# inflation
search_results <- search_dataset("CPI")
print(search_results)
countries = c("CHN", "USA", "EA20", "G-20")
prices <- get_dataset(dataset = "G20_PRICES",
                      filter = list(LOCATION = countries))
prices = as.data.table(prices)
prices = prices[TIME_FORMAT == "P1M" & MEASURE == "GY"]
prices[, Time := as.Date(paste(Time, "-01", sep=""))]
prices[, ObsValue := as.numeric(ObsValue)]
prices = prices[, .(label = LOCATION, date = Time, inflation = ObsValue)]
prices[, inflation := inflation / 100]

# visualize
# dates_paper = c(as.IDate("2006-06-01"), as.IDate("2019-09-01"))
countries_xts = prices[label %in% c("CHN", "USA", "EA20"), .(label, date, inflation)]
countries_xts = dcast(countries_xts, date ~ label, value.var = "inflation")
plot(as.xts.data.table(countries_xts))
plot(as.xts.data.table(countries_xts)["2006-06-01/2023-11-01"],
     main = "Yoy Inflation",
     legend.loc = "top")
# plot(as.xts.data.table(prices[LOCATION == "G-20", .(Time, ObsValue)]))
# plot(as.xts.data.table(prices[LOCATION == "G-20" & Time %between% dates_paper, .(Time, ObsValue)]))

# add 2 month lag
prices[, unique(label)]
prices[label == "USA", max(date)]
last_date = prices[, max(date)]
prices = rbind(prices, data.table(
  label = c("USA", "USA", "CHN", "CHN", rep("EA20", 2), rep("G-20", 2)),
  date = c(last_date %m+% months(1), last_date %m+% months(2),
           last_date %m+% months(1), last_date %m+% months(2),
           last_date %m+% months(1), last_date %m+% months(2),
           last_date %m+% months(1), last_date %m+% months(2)),
  inflation = c(rep(NA, 8))
))
setorder(prices, label, date)
prices[, inflation := shift(inflation, 2), by = label]

# rearrange prices to suitable format
prices_m = dcast(prices, label ~ date, value.var = "inflation")
prices_m[, label := fcase(
  label == "CHN",  "Inflation China",
  label =="EA20",  "Inflation EU",
  label =="USA",   "Inflation US",
  label =="G-20",   "CPI_OECD"
)]
pricesm = as.matrix(prices_m, rownames = "label")


# ECONOMIC ACTIVITY -------------------------------------------------------
# global economic ativity
url_act = "https://www.dallasfed.org/-/media/Documents/research/igrea/igrea.xlsx"
temp_file <- tempfile(fileext = ".xlsx")
GET(url_act, write_disk(temp_file))
act_global = read_excel(temp_file)
act_global = as.data.table(act_global)
act_global[, variable := "GBC"]
setnames(act_global, c("date", "econ_act", "label"))
act_global[, date := as.Date(date)]
last_date = act_global[, max(date)]
act_global = rbind(act_global, data.table(
  label = rep("GBC", 2),
  date = c(last_date %m+% months(1), last_date %m+% months(2)),
  econ_act = rep(NA, 2)
))
setorder(act_global, label, date)
act_global[, econ_act := shift(econ_act, 2), by = label]
act_global[, econ_act := econ_act / shift(econ_act) -1]
act_global = dcast(act_global, label ~ date, value.var = "econ_act")
act_globalm = as.matrix(act_global, rownames = "label")

# help function to get data from FRED
get_series = function(id_) {
  # id_ = "USALOLITONOSTSAM"
  # vin_dates = tryCatch(fredr_series_vintagedates(id_), error = function(e) NULL)
  # print(vin_dates)
  # if (is.null(vin_dates) || length(vin_dates[[1]]) == 1) {
  obs = fredr_series_observations(
    series_id = id_,
    observation_start = as.Date("1900-01-01"),
    observation_end = Sys.Date()
  )
  obs$vintage = 0L
  # }
  # else {
  #   date_vec = vin_dates[[1]]
  #   num_bins <- ceiling(length(date_vec) / 2000)
  #   bins <- cut(seq_along(date_vec),
  #               breaks = c(seq(1, length(date_vec), by = 1999),
  #                          length(date_vec)), include.lowest = TRUE, labels = FALSE)
  #   split_dates <- split(date_vec, bins)
  #   split_dates = lapply(split_dates, function(d) as.Date(d))
  #   obs_l = lapply(split_dates, function(d) {
  #     obs = fredr_series_observations(
  #       series_id = id_,
  #       observation_start = head(d, 1),
  #       observation_end = tail(d, 1),
  #       realtime_start=head(d, 1),
  #       realtime_end=tail(d, 1),
  #       limit = 2000
  #     )
  #   })
  #   obs = rbindlist(obs_l)
  #   obs$vintage = 1L
  # }
  return(obs)
}

# economic activity by country from FRED
# https://fred.stlouisfed.org/searchresults/?st=Leading%20Indicators%20OECD
codes = c("USALOLITONOSTSAM", "CHNLOLITONOSTSAM", "EA19LOLITONOSTSAM")
econact_l = lapply(codes, get_series)
names(econact_l) = codes
econact = rbindlist(econact_l, idcol = "country")
econact = econact[, .(country, date, value)]
setnames(econact, c("label", "date", "econ_act"))
econact[, econ_act := econ_act / shift(econ_act) - 1]

# add 2 month lag
econact[, unique(label)]
econact[label == "USALOLITONOSTSAM", max(date)]
last_date = econact[, max(date)]
econact = rbind(econact, data.table(
  label = c(rep("USALOLITONOSTSAM", 2),
            rep("CHNLOLITONOSTSAM", 2),
            rep("EA19LOLITONOSTSAM", 2)),
  date = c(last_date %m+% months(1), last_date %m+% months(2),
           last_date %m+% months(1), last_date %m+% months(2),
           last_date %m+% months(1), last_date %m+% months(2)),
  econ_act = rep(NA, 6)
))
setorder(econact, label, date)
econact[, econ_act := shift(econ_act, 2), by = label]

# rearrange prices to suitable format
econact_m = dcast(econact, label ~ date, value.var = "econ_act")
econact_m[, label := fcase(
  label == "CHNLOLITONOSTSAM",  "Eco_Act China",
  label == "EA19LOLITONOSTSAM", "Eco_Act EU",
  label == "USALOLITONOSTSAM",  "Eco_Act US"
)]
econactm = as.matrix(econact_m, rownames = "label")


# TRADE FLOWS -------------------------------------------------------------
# search for dataset on import / exports in $
# imf_app_name("quant_app2")
# databases <- imf_databases()
# trades <- databases[grepl("DOTS", tolower(databases$description), ignore.case = TRUE),]
# params = imf_parameters("DOT")
# params$freq
# params$ref_area
# params$indicator
# params$counterpart_area

# DOTS's data
imf_app_name("imf")
dot_raw <- imf_dataset(
  database_id = "DOT",
  freq = "A",
  ref_area = c("US", "CN", "B0"),
  indicator = c("TXG_FOB_USD", "TMG_CIF_USD"),
  counterpart_area = c("US", "CN", "B0"),
  print_url = TRUE
)
dot = setDT(dot_raw)
dot = dot[, .(date = as.integer(date), ref_area, indicator, counterpart_area,
              value = as.numeric(value))]
dot = dot[date > 1978]
dot = dot[ref_area != counterpart_area]
dot[, ref_area := fcase(ref_area == "CN", "China",
                        ref_area == "B0", "EU",
                        ref_area == "US", "US")]
dot[, counterpart_area := fcase(counterpart_area == "CN", "China",
                                counterpart_area == "B0", "EU",
                                counterpart_area == "US", "US")]
dot = dot[indicator == "TXG_FOB_USD", .(ref_area, counterpart_area, date, exports = value)][
  dot[indicator == "TMG_CIF_USD", .(ref_area, counterpart_area, date, imports = value)],
  on = c("ref_area", "counterpart_area", "date")]
dot[, trades_flow := imports + exports]
dot = na.omit(dot)
dot[, `:=`(exports = NULL, imports = NULL)]

# save to excel
sheets_ = dot[, unique(ref_area)]
trades_l = list()
for (i in seq_along(sheets_)) {
  # i = 1
  sheet_ = sheets_[i]
  m = dot[ref_area == sheet_]
  m = dcast(m, counterpart_area ~ date, value.var = "trades_flow")
  m = rbindlist(list(m, data.table(counterpart_area = sheet_)), fill = TRUE)
  cols_ = colnames(m)[2:ncol(m)]
  m[, (cols_) := lapply(.SD, nafill, fill = 0), .SDcols = cols_]
  m = as.data.frame(m)
  rownames(m) = m$counterpart_area
  m$counterpart_area = NULL
  trades_l[[i]] = m
}
names(trades_l) = sheets_
trades_l_xlsx = lapply(trades_l, function(x) {
  x[, "country"] = rownames(x)
  x = x[, c("country", colnames(x)[-ncol(x)])]
  colnames(x) = ''
  x
})
writexl::write_xlsx(trades_l_xlsx, fs::path("data", "MyTradesData", ext = "xlsx"))

# import data because IMF api doesnt work
trades_l = list()
get_tradeflows = function(tag) {
  x = read_excel(fs::path("data", "MyTradesData", ext = "xlsx"),
                 sheet = tag)
  x = as.data.frame(x)
  colnames(x) = c("label", 1979:2022)
  rownames(x) = x$label
  x$label = NULL
  x
}
trades_l[["China"]] = get_tradeflows("China")
trades_l[["EU"]] = get_tradeflows("EU")
trades_l[["US"]] = get_tradeflows("US")

# save again



# GLOBAL VARS -------------------------------------------------------------
# SPY data using duckdb from QC
conn <- dbConnect(duckdb::duckdb(), ":memory:")
query <- sprintf("
SELECT *
FROM read_csv_auto('F:/lean_root/data/all_stocks_daily.csv')
WHERE Symbol == '%s'
", "spy")
sp500_raw = dbGetQuery(conn, query)
dbDisconnect(conn, shutdown=TRUE)

# clean SP500 data
sp500 = as.data.table(sp500_raw)
sp500 = sp500[, .(label = toupper(Symbol), date = Date, close = `Adj Close`)]
sp500[, month := lubridate::ceiling_date(date, "month")]
sp500_low = sp500[, tail(.SD, 1), by = (date = month)]
sp500_low[, returns := close / shift(close) - 1]

# rearrange prices to suitable format
sp500_m = dcast(sp500_low, label ~ date, value.var = "returns")
sp500m = as.matrix(sp500_m, rownames = "label")


# FACTORS -----------------------------------------------------------------
# create spanned factors for all countries
SpaFact <- Spanned_Factors(yieldsm, c("US", "China", "EU"), 2)

# merge all factors
factors_l = list(act_globalm, sp500m, pricesm, econactm, SpaFact)
factors_l = lapply(factors_l, as.data.table, keep.rownames = "label")
factors_dt = rbindlist(factors_l, fill = TRUE)
factors_dt[, 1:3]
factors = factors_dt[, .SD, .SDcols = colSums(is.na(factors_dt)) == 0]
factors[, country := gsub(".* ", "", label)]
setorder(factors, country)
factors[, country := NULL]
factors[, 1:3]
factorsm = as.matrix(factors, rownames = "label")

# rearange factors - global variables first
global_vars = c("GBC", "CPI_OECD", "SPY")
rownmaes_arrange = c(global_vars, setdiff(rownames(factorsm), global_vars))
factorsm = factorsm[match(rownmaes_arrange, rownames(factorsm)), ]
factorsm[, 1:3]


# GVAR SAMPLE -------------------------------------------------------------
# prepare data
cols_yields  = colnames(yieldsm)
cols_factors = colnames(factorsm)
cols_all = intersect(cols_yields, cols_factors)
yieldsm_final  = yieldsm[, cols_all]
# colnames(yieldsm_final) = NULL # TODO not sure if something like this is necessary
factorsm_final = factorsm[, cols_all]


# SAVE TO EXCEL -----------------------------------------------------------
# help function to save to excel
save_to_xlsx = function(m, countries = c("US", "China", "EU"),
                        name = "MyYieldsData") {
  # m = yieldsm
  m = t(m)
  df_l = list()
  for (i in seq_along(countries)) {
    # country = "US"
    country = countries[i]
    print(country)
    if (country == "Global") {
      keep_ = global_vars
    } else {
      keep_ = colnames(m)[grepl(country, colnames(m))]
    }
    keep_ = keep_[!grepl("Level|Slope|Curva", keep_)]
    m_ = m[, keep_]
    m_ = as.data.frame(m_)
    m_$Period = as.Date(rownames(m_))
    m_ = m_[, c("Period", colnames(m_)[-ncol(m_)])]
    pat_ = paste0(" .*|", paste0("_", countries, collapse = "|"))
    colnames(m_) = gsub(pat_, "", colnames(m_))
    as.Date(m_[, "Period"])
    if (grepl("yield", name, ignore.case = TRUE)) {
      m_[, 2:ncol(m_)] = lapply(m_[, 2:ncol(m_)], function(x) x / 100)
    }
    df_l[[i]] = m_
  }
  names(df_l) = countries
  writexl::write_xlsx(df_l, fs::path("data", name, ext = "xlsx"))
}
save_to_xlsx(yieldsm_final, countries = c("US", "China", "EU"))
save_to_xlsx(factorsm_final, countries = c("Global", "US", "China", "EU"), name = "MyMacroData")

# remove column from yields TODO: check later if this can be removed
# colnames(yieldsm_final) = NULL
# colnames(factorsm_final) = strftime(as.Date(colnames(factorsm_final)),
#                                     format = "%d-%m-%Y")


# GVAR joint Q ------------------------------------------------------------
# # A) Load database data
# data("CM_Factors_2023")
# RiskFactors[, 1:5]
# data('CM_Factors_GVAR_2023')
# GVARFactors
# data('CM_Trade_2023')
# Trade_Flows
# data('CM_Yields_2023')
# Yields

# general model inputs
ModelType <- "GVAR jointQ"
StationarityUnderQ <- 0
Economies <- c("China", "EU", "US")
GlobalVar <- c("GBC", "CPI_OECD", "SPY") # c("US_Output_growth", "China_Output_growth", "SP500")
DomVar <- c("Eco_Act", "Inflation") # c("Inflation","Output_growth", "CDS", "COVID")
N <- 2
OutputLabel <- "Test"
DataFrequency <- "Monthly"
UnitMatYields <- "Month"
BiasCorrection <- 0
WishForwardPremia <- 0 # 1 in other example
FPmatLim <- c(60,120)  #  If the forward premia is desired, then choose the Min and max maturities of the forward premia. Otherwise set NA

# Decide on specific model inputs
t_First_Wgvar <- "2006"
t_Last_Wgvar <-  "2022"
W_type <- 'Sample Mean'
# VARXtype <- "constrained: Inflation"
VARXtype = "unconstrained"

# settings for numerical outputs
Horiz <- 12 # 25 in other exmaple
DesiredGraphs <- c("Fit", "IRF", "FEVD")
WishGraphRiskFac <- 0
WishGraphYields <- 1
WishOrthoJLLgraphs <- 0 # Wish to estimate the forward premia: YES: 1, NO:0

# c) bootstrap settings
WishBootstrap <- 1 #  YES: 1; No = 0.
Bootlist <- list()
Bootlist$methodBS <- 'bs'
Bootlist$BlockLength <- 4
Bootlist$ndraws <- 5
Bootlist$pctg <- 95

# d) Out-of-sample forecast features
WishForecast <- 1
ForecastList <- list()
ForecastList$ForHoriz <- 1
ForecastList$t0Sample <- 1
ForecastList$t0Forecast <- 12 * 17

# 2) Minor preliminary work
C <- length(Economies)
FactorLabels <- LabFac(N, DomVar, GlobalVar, Economies, ModelType)
ZZ <- factorsm_final

# create GVAR factors
if(any(ModelType == c("GVAR sepQ", "GVAR jointQ"))){
  Data <- list()
  # data('CM_Factors_GVAR')
  # FactorsGVAR
  # Data$GVARFactors <- FactorsGVAR
  w = Transition_Matrix(
    t_First = 2006, # data.table::year(head(colnames(factorsm_final), 1)),
    t_Last = 2022, # data.table::year(tail(colnames(yieldsm_final), 1)) - 1,
    Economies = Economies,
    type = "Sample Mean",
    DataPath = NULL,
    Data = trades_l
  )
  gvar = DatabasePrep(
    t_First = "2006-04-01", # colnames(factorsm_final)[1],
    t_Last = "2023-11-01", # colnames(factorsm_final)[ncol(factorsm_final)],
    Economies = Economies,
    N = N,
    FactorLabels = FactorLabels,
    ModelType = ModelType,
    Wgvar = w,
    DataPathMacro = "./data/MyMacroData.xlsx",
    DataPathYields = "./data/MyYieldsData.xlsx"
  )
  Data$GVARFactors <- gvar
  Data$Wgvar <- trades_l
  mat <- Maturities(yieldsm_final, Economies, UnitYields = UnitMatYields)
}

# continute wit steps in the examaple
# Data <- list()
# gvar$Global$GBC = as.matrix(gvar$Global$GBC)
# rownames(gvar$Global$GBC) = colnames(factorsm_final)
# gvar$Global$CPI_OECD = as.matrix(gvar$Global$CPI_OECD)
# rownames(gvar$Global$CPI_OECD) = colnames(factorsm_final)
# gvar$China$Yields = NULL
# gvar$EU$Yields = NULL
# gvar$US$Yields = NULL
# gvar$China$Wpca = NULL
# gvar$EU$Wpca = NULL
# gvar$US$Wpca = NULL
# gvar$China$Factors$Eco_Act = as.matrix(gvar$China$Factors$Eco_Act)
# rownames(gvar$China$Factors$Eco_Act) = colnames(factorsm_final)
# gvar$China$Factors$Inflation = as.matrix(gvar$China$Factors$Inflation)
# rownames(gvar$China$Factors$Inflation) = colnames(factorsm_final)
# gvar$China$Factors$Level = as.matrix(gvar$China$Factors$Level)
# rownames(gvar$China$Factors$Level) = colnames(factorsm_final)
# gvar$China$Factors$Slope = as.matrix(gvar$China$Factors$Slope)
# rownames(gvar$China$Factors$Slope) = colnames(factorsm_final)
#
# gvar$EU$Factors$Eco_Act = as.matrix(gvar$EU$Factors$Eco_Act)
# rownames(gvar$EU$Factors$Eco_Act) = colnames(factorsm_final)
# gvar$EU$Factors$Inflation = as.matrix(gvar$EU$Factors$Inflation)
# rownames(gvar$EU$Factors$Inflation) = colnames(factorsm_final)
# gvar$EU$Factors$Level = as.matrix(gvar$EU$Factors$Level)
# rownames(gvar$EU$Factors$Level) = colnames(factorsm_final)
# gvar$EU$Factors$Slope = as.matrix(gvar$EU$Factors$Slope)
# rownames(gvar$EU$Factors$Slope) = colnames(factorsm_final)
#
# gvar$US$Factors$Eco_Act = as.matrix(gvar$US$Factors$Eco_Act)
# rownames(gvar$US$Factors$Eco_Act) = colnames(factorsm_final)
# gvar$US$Factors$Inflation = as.matrix(gvar$US$Factors$Inflation)
# rownames(gvar$US$Factors$Inflation) = colnames(factorsm_final)
# gvar$US$Factors$Level = as.matrix(gvar$US$Factors$Level)
# rownames(gvar$US$Factors$Level) = colnames(factorsm_final)
# gvar$US$Factors$Slope = as.matrix(gvar$US$Factors$Slope)
# rownames(gvar$US$Factors$Slope) = colnames(factorsm_final)

# 2.1) Generate GVARinputs, JLLinputs and BRWinputs
ModInputs <- ListModelInputs(ModelType, Data, Economies, VARXtype, t_First_Wgvar, t_Last_Wgvar, W_type)

GVARinputs <- ModInputs$GVARinputs
JLLinputs <- NULL
BRWinputs <- NULL

# 3) Prepare the inputs of the likelihood function
# 3.1) Compute the inputs that go directly into the log-likelihood function
ATSMInputs <- InputsForMLEdensity(ModelType, yieldsm_final, ZZ, FactorLabels,
                                  mat, Economies, DataFrequency,
                                  JLLinputs, GVARinputs, BRWinputs = BRWinputs)

# 3.2) Initial guesses for Variables that will be concentrared out of from the log-likelihood function
K1XQ <- ATSMInputs$K1XQ
SSZ <- ATSMInputs$SSZ

# 4) Build the objective function
f <- Functionf(ATSMInputs, Economies, mat, DataFrequency, FactorLabels, ModelType)

# 5) Set the optimization settings
VarLab <- ParaLabels(ModelType, StationarityUnderQ)

varargin <- list()
varargin$K1XQ <-list(K1XQ, VarLab[[ModelType]][["K1XQ"]] , NULL , NULL)
varargin$SSZ <- list(SSZ, VarLab[[ModelType]][["SSZ"]], NULL, NULL)
varargin$r0 <- list(NULL, VarLab[[ModelType]][["r0"]], NULL, NULL)
varargin$se <- list(NULL, VarLab[[ModelType]][["se"]], 1e-6, NULL)
varargin$K0Z <- list(NULL, VarLab[[ModelType]][["K0Z"]], NULL, NULL)
varargin$K1Z <- list(NULL, VarLab[[ModelType]][["K1Z"]], NULL, NULL)
varargin$OptRun <-  c("iter off")

LabelVar<- c('Value', 'Label', 'LB', 'UB') # Elements of each parameter
for (d in 1:(length(varargin)-1)){ names(varargin[[d]]) <-  LabelVar}

tol <- 1e-3
# 6) Optimization of the model
ModelParaList <- list()
ModelParaList[[ModelType]] <- Optimization(f, tol, varargin, FactorLabels,
                                           Economies, ModelType,
                                           JLLinputs, GVARinputs)


GVARinputs


f
tol
varargin
FactorLabels
Economies
ModelType
JLLinputs = NULL
GVARinputs

Jmisc::tic()
print("#########################################################################################################")
print(paste("###########################", "Optimization  (Point Estimate) -",
            ModelType, "#################################"))
print("#########################################################################################################")
NP <- floor(sum(lengths(varargin))/4)
if (sum(lengths(varargin)) > NP * 4) {
  EstType <- varargin[NP + 1]
} else {
  EstType <- NULL
}
iter <- "iter"
if (grepl("iter off", EstType)) {
  iter <- "off"
}
if (!exists("tol") || is.null(tol)) {
  tol <- 1e-04
}
Max_AG_Iteration <- 10000
Previous_Optimal_Obj <- -1e+20
AuxVal <- MultiATSM:::getx(con = "concentration", varargin, Economies,
               FactorLabels, JLLinputs)
FFvec <- functional::Curry(MultiATSM:::f_with_vectorized_parameters,
                           sizex = AuxVal$sizex, f, con = "concentration", varargin,
                           ModelType, FactorLabels, Economies, JLLinputs, GVARinputs,
                           nargout = 1)
FF <- function(x0) {
  mean(FFvec(x = x0))
}
options200 <- neldermead::optimset(MaxFunEvals = 200 * numel(AuxVal$x0),
                                   Display = iter, MaxIter = 200, GradObj = "off", TolFun = 10^-8,
                                   TolX = 10^-8)
options1000 <- options200
options1000$MaxIter <- 1000
converged <- (tol > 1e+05)
oldF <- FF(x = AuxVal$x0)
scaling_vector <- NULL
count <- 0
while (!converged) {
  if (!MultiATSM:::contain("fminsearch only", EstType)) {
    if (!MultiATSM:::contain("no rescaling", EstType)) {
      if (isempty(scaling_vector)) {
        dFFvec <- MultiATSM:::df__dx(f = FFvec, x = AuxVal$x0)
        vv <- 1/rowMeans(abs(dFFvec))
        vv[is.infinite(vv)] <- max(vv[!is.infinite(vv)])
        vv[vv == 0] <- min(vv[vv > 0])
        scaling_vector <- t(t(vv))
      }
      FFtemporary <- function(xtemp, scaling_vector) {
        FF(x = scaling_vector * xtemp)
      }
      FFtemp <- functional::Curry(FFtemporary, scaling_vector = scaling_vector)
      x1 <- fminunc(x0 = AuxVal$x0/scaling_vector,
                    FFtemp, gr = NULL, tol = options200$TolFun,
                    maxiter = options200$MaxIter, maxfeval = options200$MaxFunEvals)
      x1 <- x1$par * scaling_vector
    }
    else {
      x1 <- fminunc(x0 = AuxVal$x0, FF, gr = NULL,
                    tol = options200$TolFun, maxiter = options200$MaxIter,
                    maxfeval = options200$MaxFunEvals)
      x1 <- x1$par
    }
    if (FF(x = x1) < FF(x = AuxVal$x0)) {
      AuxVal$x0 <- x1
    }
  }
  if (!contain("fminunc only", EstType)) {
    x1 <- neldermead::fminsearch(FF, AuxVal$x0, options1000)$optbase$xopt
    if (FF(x = x1) < FF(x = AuxVal$x0)) {
      AuxVal$x0 <- x1
    }
  }
  newF <- FF(x = AuxVal$x0)
  print(newF)
  converged <- (abs(oldF - newF) < tol) || (count > Max_AG_Iteration &&
                                              newF > Previous_Optimal_Obj)
  oldF <- newF
  count <- count + 1
}
ud <- update_para(AuxVal$x0, sizex = AuxVal$sizex, ii = NULL,
                  con = "concentration", FactorLabels, Economies, JLLinputs,
                  GVARinputs, varargin)
FF <- functional::Curry(f_with_vectorized_parameters, sizex = AuxVal$sizex,
                        f, con = "concentration", varargin = ud, ModelType, FactorLabels,
                        Economies, JLLinputs, GVARinputs, nargout = 2)
tryCatch({
  out <- FF(x = AuxVal$x0)$out
  VarLabInt <- intersect(names(varargin), names(out$ests))
  for (i in 1:NP) {
    if (contain("@", ud[[i]]$Label)) {
      namexi <- killa(ud[[i]]$Label)
      ud[[i]]$Label <- namexi
      si <- strfind(namexi, ":")
      if (!isempty(si)) {
        namexi <- substr(namexi, start = 1, stop = si -
                           1)
      }
      ud[[i]]$Value <- out$ests[[VarLabInt[i]]]
    }
  }
}, error = function(e) {
})
x0 <- getx(con = "", ud, Economies, FactorLabels, JLLinputs)$x0
sizex <- getx(con = "", ud, Economies, FactorLabels, JLLinputs)$sizex
namex <- getx(con = "", ud, Economies, FactorLabels, JLLinputs)$namex
x.vec_ <- x0
FF <- functional::Curry(f_with_vectorized_parameters, sizex = sizex,
                        f = f, con = "", varargin = ud, ModelType, FactorLabels,
                        Economies, JLLinputs, GVARinputs, nargout = 2)
out <- FF(x = x0)$out
x <- vector(mode = "list", length = length(namex) + 1)
x[[1]] <- x.vec_
for (i in 1:dim(sizex)[1]) {
  Fi <- functional::Curry(update_para, sizex = sizex, ii = i,
                          con = "", FactorLabels, Economies, JLLinputs, GVARinputs,
                          varargin = ud)
  if (!isempty(namex[i])) {
    x[[i + 1]] <- Fi(x = x.vec_)
  }
}
names(x) <- c("vec_", namex)
outputs <- list(x, out)
names(outputs) <- c("FinalEstimates", "Summary")
Jmisc::toc()
return(outputs)








# 7) Numerical Outputs
InputsForOutputs <- InputsForOutputs(ModelType, Horiz, DesiredGraphs, OutputLabel, StationarityUnderQ,
                                     UnitMatYields, WishGraphYields, WishGraphRiskFac, WishOrthoJLLgraphs,
                                     WishForwardPremia, FPmatLim, WishBootstrap, Bootlist, WishForecast,
                                     ForecastList)
# A) Fit, IRF, FEVD, GIRF, GFEVD, and Term Premia
NumericalOutputs <- NumOutputs(ModelType, ModelParaList, InputsForOutputs, FactorLabels, Economies)

# B) Bootstrap
Bootstrap <- Bootstrap(ModelType, ModelParaList, NumericalOutputs, mat, Economies, InputsForOutputs,
                       FactorLabels, DataFrequency, varargin, JLLinputs, GVARinputs)
#
#
#
#
#
# Data <- list()
# Data$GVARFactors <- FactorsGVAR
# FactorsGVAR$China$Factors$
#
# if (ModelType =="JLL original" ||
#     ModelType == "JLL NoDomUnit" ||
#     ModelType == "JLL jointSigma") {
#   DominantCountry <- "US"
# }
#
# JLLinputs <- NULL
# GVARinputs <- NULL
#
#
# ModelParaList <- list()
# for (i in 1:C) {
#   if ((
#     ModelType == "GVAR jointQ" || ModelType == "VAR jointQ"
#     ||
#     ModelType == "JLL original" || ModelType == "JLL NoDomUnit"
#     || ModelType == "JLL jointSigma"
#   ) & i > 1) {
#     break
#   }
#
#   # rownames(yields_us_m_)
#   # rownames(ZZ)
#   ATSMInputs <-
#     InputsForMLEdensity(
#       ModelType,
#       yields_us_final, # data('CM_Yields'); Yields
#       ZZ, # rownames(RiskFactors); rownames(ZZ)
#       FactorLabels,
#       mat,
#       Economies,
#       DataFrequency,
#       JLLinputs,
#       GVARinputs
#     )
#
#   K1XQ <- ATSMInputs$K1XQ
#   if (ModelType == "JLL original" ||
#       ModelType == "JLL NoDomUnit") {
#     SSZ <- NULL
#   } else {
#     SSZ <- ATSMInputs$SSZ
#   }
#
#   f <- Functionf(ATSMInputs,
#                  Economies,
#                  mat,
#                  DataFrequency,
#                  FactorLabels,
#                  ModelType)
#
#   VarLab <- ParaLabels(ModelType, StationarityUnderQ)
#   varargin <- list()
#   varargin$K1XQ <-
#     list(K1XQ, VarLab[[ModelType]][["K1XQ"]], NULL, NULL)
#   varargin$SSZ <-
#     list(SSZ, VarLab[[ModelType]][["SSZ"]], NULL, NULL)
#   varargin$r0 <-
#     list(NULL, VarLab[[ModelType]][["r0"]], NULL, NULL)
#   varargin$se <-
#     list(NULL, VarLab[[ModelType]][["se"]], 1e-6, NULL)
#   varargin$K0Z <-
#     list(NULL, VarLab[[ModelType]][["K0Z"]], NULL, NULL)
#   varargin$K1Z <-
#     list(NULL, VarLab[[ModelType]][["K1Z"]], NULL, NULL)
#   varargin$OptRun <- c("iter off")
#
#   LabelVar <- c('Value', 'Label', 'LB', 'UB')
#   for (d in 1:(length(varargin) - 1)) {
#     names(varargin[[d]]) <- LabelVar
#   }
#   tol <- 1e-4
#
#   if (ModelType == 'JPS' || ModelType == 'JPS jointP' ||
#       ModelType == "GVAR sepQ") {
#     ModelParaList[[ModelType]][[Economies[i]]] <- Optimization(f,
#                                                                tol,
#                                                                varargin,
#                                                                FactorLabels,
#                                                                Economies,
#                                                                ModelType,
#                                                                JLLinputs,
#                                                                GVARinputs)$Summary
#   } else{
#     ModelParaList[[ModelType]] <- Optimization(f,
#                                                tol,
#                                                varargin,
#                                                FactorLabels,
#                                                Economies,
#                                                ModelType,
#                                                JLLinputs,
#                                                GVARinputs)$Summary
#   }
#
# }
#
# # 7) Numerical and graphical outputs
# InputsForOutputs <- InputsForOutputs(ModelType, Horiz, DesiredGraphs, OutputLabel, StationarityUnderQ,
#                                      UnitMatYields, WishGraphYields, WishGraphRiskFac, WishOrthoJLLgraphs,
#                                      WishForwardPremia, FPmatLim, WishBootstrap, Bootlist, WishForecast,
#                                      ForecastList)
# # # # A) Fit, IRF, FEVD, GIRF, GFEVD, and Term Premia
# # NumericalOutputs <- NumOutputs(ModelType, ModelParaList, InputsForOutputs, FactorLabels, Economies)
# #
# # # B) Bootstrap
# # Bootstrap <- Bootstrap(ModelType, ModelParaList, NumericalOutputs, mat, Economies, InputsForOutputs,
# #                        FactorLabels, DataFrequency, varargin, JLLinputs, GVARinputs, BRWinputs = NULL)
#
#
#
#
# # ModelPara = ModelParaList
# #
# # MyForecastYieldsSepQ =  function (ModelType, ModelPara, InputsForOutputs, FactorLabels,
# #           Economies, DataFrequency, JLLinputs, GVARinputs, BRWinputs)
# # {
# #   print("#########################################################################################################")
# #   print(paste("#################################", "Forecasting",
# #               ModelType, "#################################"))
# #   print("#########################################################################################################")
# #   StationarityUnderQ <- InputsForOutputs$StationaryQ
# #   t0Sample <- InputsForOutputs[[ModelType]]$Forecasting$t0Sample
# #   t0Forecast <- InputsForOutputs[[ModelType]]$Forecasting$t0Forecast
# #   H <- InputsForOutputs[[ModelType]]$Forecasting$ForHoriz
# #   if (t0Forecast < t0Sample) {
# #     stop("The first forecast cut-off date is earlier than the start of the sample.")
# #   }
# #   FullModelParaList <- list()
# #   OutofSampleForecast <- list()
# #   T <- ncol(ModelPara[[ModelType]][[Economies[1]]]$inputs$Y)
# #   N <- length(FactorLabels$Spanned)
# #   M <- length(FactorLabels$Domestic) - N
# #   G <- length(FactorLabels$Global)
# #   C <- length(Economies)
# #   dt <- ModelPara[[ModelType]][[Economies[1]]]$inputs$dt
# #   mat <- ModelPara[[ModelType]][[Economies[1]]]$inputs$mat
# #   J <- length(mat)
# #   nloops <- T - t0Forecast - H + 1
# #   if (t0Forecast > T) {
# #     stop("The first forecast cut-off date is longer than the sample length.")
# #   }
# #   for (tt in 1:nloops) {
# #     # tt = 8
# #     if (nloops <= 0) {
# #       stop("Impossible to generate forecast errors: sample period is extrapolated!")
# #     }
# #     if (tt == 1) {
# #       tlastObserved <- t0Forecast
# #     }
# #     else {
# #       tlastObserved <- tlastObserved + 1 # tlastObserved = 384 + 8
# #     }
# #     Ttemp <- tlastObserved
# #     for (i in 1:C) {
# #       # i = 1
# #       ZZfull <- ModelPara[[ModelType]][[Economies[i]]]$inputs$AllFactors
# #       YieldsFull <- ModelPara[[ModelType]][[Economies[i]]]$inputs$Y
# #       i <<- i
# #       YYtemp <- YieldsFull[, t0Sample:tlastObserved]
# #       PPall <- MultiATSM:::SpannedFactorsSepQ(ModelType, ModelPara,
# #                                   Economies, t0Sample, tlastObserved)
# #       IdxSpa <- MultiATSM:::IdxAllSpanned(ModelType, FactorLabels,
# #                               Economies)
# #       ZZtemp <- ZZfull[, t0Sample:tlastObserved]
# #       ZZtemp[IdxSpa, ] <- PPall
# #       if (ModelType == "GVAR sepQ") {
# #         GVARinputs$GVARFactors <- DataSet_BS(ModelType,
# #                                              ZZtemp, GVARinputs$Wgvar, Economies, FactorLabels)
# #       }
# #       invisible(utils::capture.output(ATSMInputs <- InputsForMLEdensity(ModelType,
# #                                                                         YYtemp, ZZtemp, FactorLabels, mat, Economies,
# #                                                                         DataFrequency, JLLinputs, GVARinputs, BRWinputs=NULL)))
# #       K1XQ <- ATSMInputs$K1XQ
# #       SSZ <- ATSMInputs$SSZ
# #       f <- Functionf(ATSMInputs, Economies, mat, DataFrequency,
# #                      FactorLabels, ModelType)
# #       VarLab <- ParaLabels(ModelType, StationarityUnderQ)
# #       varargin <- list()
# #       varargin$K1XQ <- list(K1XQ, VarLab[[ModelType]][["K1XQ"]],
# #                             NULL, NULL)
# #       varargin$SSZ <- list(SSZ, VarLab[[ModelType]][["SSZ"]],
# #                            NULL, NULL)
# #       varargin$r0 <- list(NULL, VarLab[[ModelType]][["r0"]],
# #                           NULL, NULL)
# #       varargin$se <- list(NULL, VarLab[[ModelType]][["se"]],
# #                           1e-06, NULL)
# #       varargin$K0Z <- list(NULL, VarLab[[ModelType]][["K0Z"]],
# #                            NULL, NULL)
# #       varargin$K1Z <- list(NULL, VarLab[[ModelType]][["K1Z"]],
# #                            NULL, NULL)
# #       varargin$OptRun <- c("iter off")
# #       LabelVar <- c("Value", "Label", "LB", "UB")
# #       for (d in 1:(length(varargin) - 1)) {
# #         names(varargin[[d]]) <- LabelVar
# #       }
# #       tol <- 1e-04
# #       invisible(utils::capture.output(FullModelParaList[[ModelType]][[Economies[i]]] <- Optimization(f,
# #                                                                                                      tol, varargin, FactorLabels, Economies, ModelType,
# #                                                                                                      JLLinputs, GVARinputs)$Summary))
# #       A <- FullModelParaList[[ModelType]][[Economies[i]]]$rot$P$A
# #       Bfull <- MultiATSM:::BUnspannedAdapSep(G, M, FullModelParaList,
# #                                  Economies, Economy = Economies[i], ModelType)
# #       K0Z <- FullModelParaList[[ModelType]][[Economies[i]]]$ests$K0Z
# #       K1Z <- FullModelParaList[[ModelType]][[Economies[i]]]$ests$K1Z
# #       ForecastYields <- matrix(NA, nrow = J, ncol = H)
# #       LabelForecastPeriod <- colnames(YieldsFull[, (Ttemp + 1):(Ttemp + H)])
# #       rownames(ForecastYields) <- rownames(YieldsFull)
# #       colnames(ForecastYields) <- LabelForecastPeriod
# #       ZZtt <- ZZtemp[, Ttemp]
# #       K1ZsumOld <- 0
# #       for (hh in 1:H) {
# #         K1Znew <- powerplus::Matpow(K1Z, numer = hh -
# #                                       1)
# #         VARforecast <- (K1ZsumOld + K1Znew) %*% K0Z +
# #           powerplus::Matpow(K1Z, numer = hh) %*% ZZtt
# #         ForecastYields[, hh] <- A + Bfull %*% (VARforecast)
# #         K1ZsumOld <- K1ZsumOld + K1Znew
# #       }
# #       YieldsObsForPer <- YieldsFull[, (Ttemp + 1):(Ttemp + H)]
# #       ForecastError <- YieldsObsForPer - ForecastYields
# #       ForecastDate <- colnames(ZZfull)[Ttemp]
# #       OutofSampleForecast[[ModelType]][[Economies[i]]][[ForecastDate]]$Forcast <- ForecastYields
# #       OutofSampleForecast[[ModelType]][[Economies[i]]][[ForecastDate]]$Error <- ForecastError
# #       print(paste(ModelType, Economies[i], ": Out-of-sample forecast for information set available until",
# #                   ForecastDate))
# #       saveRDS(OutofSampleForecast, paste(tempdir(), "/Forecast_",
# #                                          InputsForOutputs$"Label Outputs", ".rds", sep = ""))
# #     }
# #   }
# #   RMSE <- list(RMSEsep(OutofSampleForecast))
# #   names(RMSE) <- "RMSE"
# #   OutofSampleForecast <- append(OutofSampleForecast[[ModelType]],
# #                                 RMSE)
# #   saveRDS(OutofSampleForecast, paste(tempdir(), "/Forecast_",
# #                                      InputsForOutputs$"Label Outputs", ".rds", sep = ""))
# #   return(OutofSampleForecast)
# # }
# #
# # MyForecasts = MyForecastYieldsSepQ(
# #   ModelType, ModelParaList, InputsForOutputs, FactorLabels,
# #   Economies, DataFrequency, JLLinputs, GVARinputs, BRWinputs = NULL
# # )
#
# # C) Out-of-sample forecasting
# Forecasts <- ForecastYields(ModelType, ModelParaList, InputsForOutputs, FactorLabels, Economies,
#                             DataFrequency, JLLinputs, GVARinputs, BRWinputs = NULL)



# save forecasts
# saveRDS(Forecasts, "forecasts/forecasts.rds")
Forecasts = readRDS("forecasts/forecasts.rds")
Forecasts$US$`2023-10-01`

# BACKTEST ----------------------------------------------------------------
# merge all forecasts
forecasts_us = lapply(Forecasts$US, function(x) x[[1]])
forecasts_us = lapply(forecasts_us, function(x) {
  # x = forecasts_us[[1]]
  x = as.data.table(x, keep.rownames = "label")
  setnames(x, c("label", "forecast"))
  x
})
forecasts_us = rbindlist(forecasts_us, idcol = "month")
forecasts_us[, month := as.Date(month)]

# get securities data from QC
get_sec = function(symbol) {
  con <- dbConnect(duckdb::duckdb())
  query <- sprintf("
    SELECT *
    FROM 'F:/lean_root/data/all_stocks_daily.csv'
    WHERE Symbol = '%s'
", symbol)
  data_ <- dbGetQuery(con, query)
  dbDisconnect(con)
  data_ = as.data.table(data_)
  data_ = data_[, .(date = Date, close = `Adj Close`)]
  data_[, returns := close / shift(close) - 1]
  data_ = na.omit(data_)
  dbDisconnect(con)
  return(data_)
}
spy = get_sec("spy")
tlt = get_sec("tlt")

# tlt to monthly data
tlt[, month := ceiling_date(date, unit = "month")]
tlt = tlt[, tail(.SD, 1), by = .(month)]
tlt[, returns := close / shift(close) - 1]
tlt = na.omit(tlt)

# merge tlt and forecasts
backtest_dt = merge(tlt, forecasts_us, by = "month")
yields_m_melted = melt(as.data.table(yields_us_final, keep.rownames = "label"),
                       variable.name = "month", value.name = "yield_val")
yields_m_melted[, month := as.Date(as.character(month))]
backtest_dt = merge(backtest_dt, yields_m_melted, by = c("month", "label"))
setorder(backtest_dt, month, date)

# create signals
backtest_dt[, unique(label)]
backtest_sample = backtest_dt[label == "Y12M_US"]
backtest_sample = backtest_sample[, .(signal = !any((forecast - yield_val) > 0),
                                      returns = tail(returns, 1)),
                                  by = month]
backtest_sample[, strategy := signal * returns]
# backtest_sample[, strategy := signal * shift(returns, -1, type = "shift")]
backtest_sample = as.xts.data.table(backtest_sample[, .(month, returns, strategy)])
charts.PerformanceSummary(backtest_sample)

# create signals
backtest_sample = backtest_dt[, .(signal = !any((forecast - yield_val) > 0),
                                  returns = tail(returns, 1)),
                              by = .(month, label)]
backtest_sample = backtest_sample[, .(signal = sum(signal) < 4,
                                      returns = tail(returns, 1)),
                                  by = .(month)]
# backtest_sample[, strategy := shift(signal) * returns]
backtest_sample[, strategy := shift(signal) * shift(returns, -1, type = "shift")]
backtest_sample = as.xts.data.table(backtest_sample[, .(month, returns, strategy)])
charts.PerformanceSummary(backtest_sample)

# create signals
backtest_dt[, unique(label)]
backtest_sample = backtest_dt[label == "Y120M_US"]
backtest_sample[, yield_val_shift := shift(yield_val)]
backtest_sample = na.omit(backtest_sample)
backtest_sample = backtest_sample[, signal := (forecast - yield_val_shift) > 0]
backtest_sample = backtest_sample[, signal := !signal]
backtest_sample[, strategy := signal * shift(returns, -1, type = "shift")]
# backtest_sample[, strategy := signal * returns]
backtest_sample = as.xts.data.table(backtest_sample[, .(month, returns, strategy)])
charts.PerformanceSummary(backtest_sample)







# A) Load database data
data("CM_Factors_2023")
RiskFactors[, 1:5]
data('CM_Factors_GVAR_2023')
GVARFactors
data('CM_Trade_2023')
Trade_Flows
data('CM_Yields_2023')
Yields

# B) Decide on general model inputs
ModelType <- "GVAR jointQ"

StationarityUnderQ <- 0
BiasCorrection <- 0

WishForwardPremia <- 1
FPmatLim <- c(47,48)

Economies <- c("Brazil", "India", "Russia", "Mexico") # Names of the economies from the economic system
GlobalVar <- c("US_Output_growth", "China_Output_growth") # Global Variables
DomVar <- c("Inflation","Output_growth", "CDS", "COVID") # Country-specific variables

N <- 2 # Number of spanned factors per country

OutputLabel <- "CM_2023"
DataFrequency <- "Weekly"
UnitMatYields <- "Month"

# C.1) Decide on specific model inputs
#################################### GVAR-based models ##################################################
t_First_Wgvar <- "2015"
t_Last_Wgvar <-  "2020"
W_type <- 'Sample Mean'
VARXtype <- "constrained: COVID"
###################################### BRW inputs  ######################################################

# D) Decide on Settings for numerical outputs
Horiz <- 12
DesiredGraphs <- c("GIRF", "GFEVD", "TermPremia")
WishGraphRiskFac <- 0
WishGraphYields <- 1
WishOrthoJLLgraphs <- 0

# E) Bootstrap settings
WishBootstrap <- 0 #  YES: 1; No = 0.
Bootlist <- list()
Bootlist$methodBS <- 'bs'
Bootlist$BlockLength <- 4 # necessary input if one chooses the block bootstrap
Bootlist$ndraws <-  100
Bootlist$pctg   <-  95 # confidence level

# F) Out-of-sample forecast
WishForecast <- 0
ForecastList <- list()
ForecastList$ForHoriz <- 12 # forecast horizon
ForecastList$t0Sample <- 1 # initial sample date
ForecastList$t0Forecast <- 50 # last sample date for the first forecast

##########################################################################################################
############################### NO NEED TO MAKE CHANGES FROM HERE ########################################
##########################################################################################################
# 2) Minor preliminary work
C <- length(Economies)
FactorLabels <- LabFac(N, DomVar,GlobalVar, Economies, ModelType)
RiskFactors_ = RiskFactors[rownames(RiskFactors) != "SP500", ]
ZZ <- RiskFactors_

Data <- list()
GVARFactors_ = GVARFactors
GVARFactors_$Global$SP500 = NULL
Data$GVARFactors <- GVARFactors_
Data$Wgvar <-  Trade_Flows
mat <- Maturities(Yields, Economies, UnitYields = UnitMatYields)

# 2.1) Generate GVARinputs, JLLinputs and BRWinputs
ModInputs <- ListModelInputs(ModelType, Data, Economies, VARXtype, t_First_Wgvar, t_Last_Wgvar, W_type)

GVARinputs <- ModInputs$GVARinputs
JLLinputs <- NULL

# 3) Prepare the inputs of the likelihood function
# 3.1) Compute the inputs that go directly into the log-likelihood function
ATSMInputs <- InputsForMLEdensity(ModelType, Yields, ZZ, FactorLabels, mat, Economies, DataFrequency,
                                  JLLinputs, GVARinputs)

# 3.2) Initial guesses for Variables that will be concentrared out of from the log-likelihood function
K1XQ <- ATSMInputs$K1XQ
SSZ <- ATSMInputs$SSZ

# 4) Build the objective function
f <- Functionf(ATSMInputs, Economies, mat, DataFrequency, FactorLabels, ModelType)

# 5) Set the optimization settings
VarLab <- ParaLabels(ModelType, StationarityUnderQ)

varargin <- list()
varargin$K1XQ <-list(K1XQ, VarLab[[ModelType]][["K1XQ"]] , NULL , NULL)
varargin$SSZ <- list(SSZ, VarLab[[ModelType]][["SSZ"]], NULL, NULL)
varargin$r0 <- list(NULL, VarLab[[ModelType]][["r0"]], NULL, NULL)
varargin$se <- list(NULL, VarLab[[ModelType]][["se"]], 1e-6, NULL)
varargin$K0Z <- list(NULL, VarLab[[ModelType]][["K0Z"]], NULL, NULL)
varargin$K1Z <- list(NULL, VarLab[[ModelType]][["K1Z"]], NULL, NULL)
varargin$OptRun <-  c("iter off")

# my test
x = c(3.4061529, -0.1673041, rep(0, 24))
y = c(-0.1673041, 0.6775677, rep(0, 24))
z = cbind(x, y)
cor(varargin$SSZ[[1]])

LabelVar<- c('Value', 'Label', 'LB', 'UB') # Elements of each parameter
for (d in 1:(length(varargin)-1)){ names(varargin[[d]]) <-  LabelVar}

tol <- 1e-4
# 6) Optimization of the model
ModelParaList <- list()
ModelParaList[[ModelType]] <- Optimization(f, tol, varargin, FactorLabels, Economies, ModelType,
                                           JLLinputs, GVARinputs)$Summary

# 7) Numerical Outputs
InputsForOutputs <- InputsForOutputs(ModelType, Horiz, DesiredGraphs, OutputLabel, StationarityUnderQ,
                                     UnitMatYields, WishGraphYields, WishGraphRiskFac, WishOrthoJLLgraphs,
                                     WishForwardPremia, FPmatLim, WishBootstrap, Bootlist, WishForecast,
                                     ForecastList)

# A) Fit, IRF, FEVD, GIRF, GFEVD, and Term Premia
NumericalOutputs <- NumOutputs(ModelType, ModelParaList, InputsForOutputs, FactorLabels, Economies)

# B) Bootstrap
Bootstrap <- Bootstrap(ModelType, ModelParaList, NumericalOutputs, mat, Economies, InputsForOutputs,
                       FactorLabels, DataFrequency, varargin, JLLinputs, GVARinputs)



x = factorsm_final[c("GBC", "CPI_OECD"), 1:5]
x = t(x)
x[, "GBC"] = c(NA, diff(log(x[, "GBC"])))
