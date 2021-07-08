library(data.table)
library(exuber)
library(httr)
library(lubridate)
library(fmpcloudr)
library(leanr)
library(mrisk)
library(httr)
library(jsonlite)
library(future.apply)
library(rdrop2)
library(emayili)      # sending e-mails



# set up
APIKEY = "15cd5d0adf4bc6805a724b4417bbaafc"
fmpc_set_token(APIKEY, noBulkWarn = TRUE)
base_url = "https://financialmodelingprep.com/api/v4"
radf_path = "D:/risks/radfagg-live"
drop_auth()

# parameters
exuber_length <- 100
frequency_in_minutes <- "1 min" # hour minute 5 mins
min_w <- psy_minw(exuber_length)

# get NY current time
get_ny_time <- function() {
  s <- Sys.time()
  s <- .POSIXct(s, "America/New_York")
  return(s)
}

# email template
send_email <- function(message = enc2utf8("Order for ExuberrAgg Strategy."),
                       sender = "mislav.sagovac@contentio.biz",
                       recipients = c("mislav.sagovac@contentio.biz")) {
  email <- emayili::envelope()
  email <- email %>%
    emayili::from(sender) %>%
    emayili::to(recipients) %>%
    emayili::subject("ExuberrAgg Strategy.") %>%
    emayili::text(message)
  smtp <- server(host = "mail.contentio.biz",
                 port = 587,
                 username = "mislav.sagovac+contentio.biz",
                 password = "Contentio0207")
  smtp(email, verbose = TRUE)
}


# M-FILES -----------------------------------------------------------------

# GET REST API function
mfiles_get <- function(token, resource){
  req <- GET(url = paste0('http://server.contentio.biz/REST', resource),
             add_headers('X-Authentication' = token, 'content-type' = "application/json"))
  json <- httr::content(req, as = "text", type = "application/json", encoding = "UTF-8")
  result <- jsonlite::fromJSON(json)
  return(result)
}

# GET token
request <- POST(url = 'http://server.contentio.biz/REST/server/authenticationtokens.aspx',
                config = add_headers('content-type' = "application/json"),
                body = list(Username = "msagovac", Password = "Wc8O10TaHz40",
                            VaultGuid = "{452444F1-3175-43E5-BACF-5CD0159BFE97}",
                            ComputerName	= "CT-VM-01"),
                encode = "json", verbose())
token <- httr::content(request, as = "text", type = "application/json", encoding = "UTF-8")
token <- fromJSON(token)[[1]]

# M-files properties and classes
prop <- mfiles_get(token, "/structure/properties")
prop <- prop[, c("DataType", "ID", "Name", "ObjectType")]
objs <- mfiles_get(token, "/structure/objecttypes")
mfilesClass <- mfiles_get(token, "/structure/classes")
sharedLinks <- mfiles_get(token, "/sharedlinks")

# crate M-files object
create_object <- function(object_name, file_id, file_extension, file_size, single_file = TRUE) {
  json_query <- list(
    PropertyValues = I(list(
      list(PropertyDef = 100,
           TypedValue = list(
             DataType = 9,
             HasValue = FALSE,
             Value = NA,
             Lookup = list(
               Deleted = FALSE,
               DisplayValue = NA,
               Hidden = FALSE,
               Item = 0,
               Version = -1
             ),
             Lookups = NA,
             DisplayValue = NA,
             SortingKey = NA,
             SerializedValue = NA
           )),
      list(PropertyDef = 22,
           TypedValue = list(
             DataType = 8,
             HasValue = FALSE,
             Value = single_file,
             Lookup = NA,
             Lookups = NA,
             DisplayValue = NA,
             SortingKey = NA,
             SerializedValue = NA
           )),
      list(PropertyDef = 0,
           TypedValue = list(
             DataType = 1,
             HasValue = FALSE,
             Value = object_name,
             Lookup = NA,
             Lookups = NA,
             DisplayValue = NA,
             SortingKey = NA,
             SerializedValue = NA
           ))
      # list(PropertyDef = 1048,
      #      TypedValue = list(
      #        DataType = 1,
      #        HasValue = FALSE,
      #        Value = ime_i_prezime_stecajnog_duznika,
      #        Lookup = NA,
      #        Lookups = NA,
      #        DisplayValue = NA,
      #        SortingKey = NA,
      #        SerializedValue = NA
      #      ))
    )),
    Files = list(I(list(
      UploadID = file_id,
      Title = NA,
      Extension = file_extension,
      Size = file_size
    )))
  )
  json_query <- jsonlite::toJSON(json_query, auto_unbox = TRUE, pretty = TRUE)
  json_query <- gsub("\\.1", "", json_query)
}



# IMPORT DATA -------------------------------------------------------------

# import and prepare market data for which we will calculate exuber aggregate indicator
sp500_stocks <- GET("https://financialmodelingprep.com/api/v3/sp500_constituent?apikey=15cd5d0adf4bc6805a724b4417bbaafc")
sp500_stocks <- rbindlist(httr::content(sp500_stocks))
market_data <- import_lean("D:/market_data/equity/usa/hour/trades", sp500_stocks$symbol)
setorderv(market_data, c('symbol', 'datetime'))
keep_symbols <- market_data[, .N, by = symbol][N > exuber_length, symbol]
market_data <- market_data[symbol %in% keep_symbols]
market_data <- market_data[, tail(.SD, exuber_length), by = .(symbol)]
symbols <- unique(market_data$symbol)



# LIVE TRADE --------------------------------------------------------------

# radf point
radf_point <- function(ticker, window, price_lag, use_log, api_key, time = "hour") {

  # set start and end dates
  data_ <- market_data[symbol == ticker]
  start_dates <- as.Date(max(data_$datetime))
  end_dates <- start_dates + 5

  # get market data
  ohlcv <- get_market_equities(ticker,
                               from = as.character(start_dates),
                               to = as.character(end_dates),
                               time = time,
                               api_key = api_key)
  ohlcv$symbol <- ticker
  ohlcv$formated <- as.POSIXct(ohlcv$formated, tz = "EST")
  ohlcv <- ohlcv[, .(symbol, datetime = formated, open = o, high = h, low = l, close = c, volume = v)]
  ohlcv <- rbind(data_, ohlcv)
  prices <- unique(ohlcv)
  prices <- prices[format(datetime, "%H:%M:%S") %between% c("10:00:00", "15:00:00")]
  setorderv(prices, c("symbol", "datetime"))

  # calculate exuber
  if (use_log) {
    close <- log(prices$close)
  } else {
    close <- prices$close
  }

  # add newest data becuase FP cloud can't reproduce newst data that fast
  nytime <- format(Sys.time(), tz="America/New_York", usetz=TRUE)
  if (time == "hour" && hour(nytime) > hour(max(prices$datetime))) {
    last_price <- GET(paste0("https://financialmodelingprep.com/api/v3/quote-short/", ticker, "?apikey=", api_key))
    last_price <- content(last_price)[[1]]$price
    close <- c(close, last_price)
    max_datetime <- round.POSIXt(nytime, units = "hours")
  } else {
    max_datetime <- max(prices$datetime)
  }

  # calculate exuber
  close <- close[(length(close)-window+1):length(close)]
  y <- exuber::radf(close, lag = price_lag)
  stats <- exuber::tidy(y)
  bsadf <- data.table::last(exuber::augment(y))[, 4:5]
  max_datetime <- as.POSIXct(as.character(max_datetime), tz = 'EST')
  # attributes(max_datetime)$tzone <- 'UCT'
  result <- cbind(datetime = max_datetime, stats, bsadf)
  result$id <- NULL
  return(result)
}

# set closing time
closing_time <- as.POSIXct(paste0(Sys.Date(), "16:30:00"), tz = "America/New_York")
open_time <- as.POSIXct(paste0(Sys.Date(), "08:30:00"), tz = "America/New_York")

# main function which calculates exuber aggregate indicator
s <- get_ny_time()
next_time_point <- ceiling_date(s, frequency_in_minutes)
repeat {
  s <- get_ny_time()
  print(s)
  if (s >= next_time_point) {
    print("Exuber Aggregate calculate")
    # get current market data for all stocks
    start_time <- Sys.time()
    exuber_symbol <- lapply(seq_along(symbols), function(i) { # seq_along(symbols)
      radf_point(symbols[i], exuber_length, 1, 1, APIKEY, "hour")
    })
    end_time <- Sys.time()
    end_time - start_time
    exubers <- rbindlist(exuber_symbol)
    exubers[, radf := adf + sadf + gsadf + badf + bsadf]
    std_exuber <- exubers[, lapply(.SD, sd, na.rm = TRUE), by = c('datetime'), .SDcols = colnames(exubers)[2:ncol(exubers)]]

    # send mail notification if main indicator is above threshold (we have to sell)
    if (std_exuber$radf > 3.4) {
      send_email(message = "Exuber Agg is Above Threshold",
                 sender = "mislav.sagovac@contentio.biz",
                 recipients = c("mislav.sagovac@contentio.biz"))
    }

    # save result to file locally and to M-files
    file_name_date <- file.path(radf_path, paste0("radfagg-", Sys.Date(), ".csv"))
    file_name <- file.path(radf_path, paste0("radfagg.csv"))
    file_name_one <- file.path(radf_path, paste0("radfagg_one.csv"))
    if (!file.exists(file_name)) {

      # save locally
      fwrite(std_exuber, file_name)
      fwrite(std_exuber, file_name_date)

      # upload file to M-files
      # req_up <- POST(url = 'http://server.contentio.biz/REST/files.aspx',
      #                config = add_headers('X-Authentication' = token),
      #                body = list(StreamContent = upload_file(file_name)))
      # content_up <- httr::content(req_up, as = "text", type = "application/json", encoding = "UTF-8")
      # file_info <- unlist(jsonlite::fromJSON(content_up, simplifyVector = TRUE))
      # file_id <- as.integer(file_info[which(names(file_info) == "UploadID")])
      # file_size <- as.numeric(file_info[which(names(file_info) == "Size")])
      # file_extension <- as.character(file_info[which(names(file_info) == "Extension")])
      # object_name <- as.character(file_info[which(names(file_info) == "Title")])
      # json_q <- create_object(object_name, file_id, file_extension, file_size)
      # created_object <- POST('http://server.contentio.biz/REST/objects/0.aspx',
      #                        config = add_headers('X-Authentication' = token, 'content-type' = "application/json"),
      #                        body = json_q)
      # created_object <- content(created_object)
      # file_id <- created_object$ObjVer$ID

      # upload file to dropbox if it doesnt'exists
      drop_upload(file = file_name, path = "exuber")

    } else {

      ########## OLD WAY ##########
      # append table calculation
      # old_data <- fread(file_name)
      # colnames(old_data) <- colnames(std_exuber)
      # std_exuber <- rbind(old_data, std_exuber)
      # std_exuber <- unique(std_exuber)
      # fwrite(std_exuber, file_name, col.names = FALSE)
      ########## OLD WAY ##########

      ########## NEW WAY ##########
      # prepare data for dropbox
      fwrite(tail(std_exuber, 1), file_name, col.names = FALSE)
      ########## NEW WAY ##########

      # updating existing file
      #   x <- POST(paste0("http://server.contentio.biz/REST/objects/0/", file_id, "/latest/checkedout.aspx?_method=PUT"),
      #             config = add_headers('X-Authentication' = token, 'content-type' = "application/json"),
      #             body = "{ \"Value\" : \"2\" }")
      #   checkedOutObjectVersion <- content(x)
      #   update_url <- paste0("http://server.contentio.biz/REST/objects/0/",
      #                        checkedOutObjectVersion$ObjVer$ID,
      #                        "/files/",
      #                        checkedOutObjectVersion$Files[[1]]$ID,
      #                        "/content.aspx?_method=PUT"
      #   )
      #   POST(update_url, config = add_headers('X-Authentication' = token),
      #        body = upload_file(file_name))
      #   POST(paste0("http://server.contentio.biz/REST/objects/0/", file_id, "/latest/checkedout.aspx?_method=PUT"),
      #        config = add_headers('X-Authentication' = token, 'content-type' = "application/json"),
      #        body = "{ \"Value\" : \"0\" }")
      # }

      # update dropbox
      drop_upload(file = file_name, path = "exuber")
    }
  }
  if (s > closing_time) {
    print("Market is closed")
    break
  }
  Sys.sleep(5L)
  next_hour <- ceiling_date(s, frequency_in_minutes)
}
