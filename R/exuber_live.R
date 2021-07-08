library(data.table)
library(exuber)
library(httr)
library(lubridate)
library(fmpcloudr)
library(leanr)
library(mrisk)
library(httr)
library(jsonlite)


# set up
APIKEY = "15cd5d0adf4bc6805a724b4417bbaafc"
fmpc_set_token(APIKEY, noBulkWarn = TRUE)
base_url = "https://financialmodelingprep.com/api/v4"
radf_path = "D:/risks/radf-live"

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

# set closing time
closing_time <- as.POSIXct(paste0(Sys.Date(), "16:00:00"), tz = "America/New_York")
open_time <- as.POSIXct(paste0(Sys.Date(), "09:00:00"), tz = "America/New_York")

# import and prepare market data for which we will calculate exuber aggregate indicator
spy <- import_lean("D:/market_data/equity/usa/hour/trades_adjusted", "SPY")

# main function which calculates exuber aggregate indicator
s <- get_ny_time()
next_time_point <- ceiling_date(s, frequency_in_minutes)
repeat {
  s <- get_ny_time()
  print(s)
  if (s >= next_time_point) {
    print("Exuber calculate")
    x <- mrisk::radf_point("SPY", as.character(as.Date(s)), 100, 1, 1, api_key = APIKEY, time = "minute")
    x$datetime <- as.character(x$datetime)
    file_name <- file.path(radf_path, paste0("radf-spy-", Sys.Date(), ".csv"))
    if (!file.exists(file_name)) {
      fwrite(x, file_name)
      # upload file to M-files
      req_up <- POST(url = 'http://server.contentio.biz/REST/files.aspx',
                     config = add_headers('X-Authentication' = token),
                     body = list(StreamContent = upload_file(file_name)))
      content_up <- httr::content(req_up, as = "text", type = "application/json", encoding = "UTF-8")
      file_info <- unlist(jsonlite::fromJSON(content_up, simplifyVector = TRUE))
      file_id <- as.integer(file_info[which(names(file_info) == "UploadID")])
      file_size <- as.numeric(file_info[which(names(file_info) == "Size")])
      file_extension <- as.character(file_info[which(names(file_info) == "Extension")])
      object_name <- as.character(file_info[which(names(file_info) == "Title")])
      json_q <- create_object(object_name, file_id, file_extension, file_size)
      created_object <- POST('http://server.contentio.biz/REST/objects/0.aspx',
           config = add_headers('X-Authentication' = token, 'content-type' = "application/json"),
           body = json_q)
      created_object <- content(created_object)
      file_id <- created_object$ObjVer$ID
    } else {
      old_data <- fread(file_name)
      x <- rbind(old_data, x)
      fwrite(x, file_name)

      # updating existing file
      x <- POST(paste0("http://server.contentio.biz/REST/objects/0/", file_id, "/latest/checkedout.aspx?_method=PUT"),
                config = add_headers('X-Authentication' = token, 'content-type' = "application/json"),
                body = "{ \"Value\" : \"2\" }")
      checkedOutObjectVersion <- content(x)
      update_url <- paste0("http://server.contentio.biz/REST/objects/0/",
                           checkedOutObjectVersion$ObjVer$ID,
                           "/files/",
                           checkedOutObjectVersion$Files[[1]]$ID,
                           "/content.aspx?_method=PUT"
      )
      POST(update_url, config = add_headers('X-Authentication' = token),
           body = upload_file(file_name))
      POST(paste0("http://server.contentio.biz/REST/objects/0/", file_id, "/latest/checkedout.aspx?_method=PUT"),
           config = add_headers('X-Authentication' = token, 'content-type' = "application/json"),
           body = "{ \"Value\" : \"0\" }")
    }
  }
  if (s > closing_time || s < open_time) {
    print("Market is closed")
    break
  }
  Sys.sleep(1L)
  next_time_point <- ceiling_date(s, frequency_in_minutes)
}

