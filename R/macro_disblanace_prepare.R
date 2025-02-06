library(findata)
library(finutils)
library(fastverse)

# Init MacroData
path_ = "D:/fred"
fred = MacroData$new(path_to_dump = path_)

# Get fred metadata
meta = fred$get_fred_metadata()

# Choose series with daily data and filter active
meta[, unique(frequency)]
meta[, unique(frequency_short)]

# Save meta
fwrite(meta, "D:/fred/meta.csv")

# Get daily meta
metad = meta[frequency_short == "D"]
metad = metad[observation_end > (max(observation_end) - 2)]

# Checks
metad[popularity == min(popularity), .(popularity, id, title)]

# Download all data
fred$bulk_fred(ids = metad[, id])

# Download biased data to compare with vintage
dir_ = "D:/fredbiased"
if (!fs::dir_exists(dir_)) fs::dir_create(dir_)
vapply(metad[, id], function(id_) {
  # id_ = "MKT4180MKTAMT"
  # print(id_)
  file_name_ = file.path(dir_, paste0(id_, ".csv"))
  if (fs::file_exists(file_name_)) return(1L)
  obs = fredr_series_observations(
    series_id = id_,
    observation_start = as.Date("1990-01-01"),
    observation_end = Sys.Date()
  )
  fwrite(obs, file_name_)
  Sys.sleep(1L)
  return(1L)
}, FUN.VALUE = integer(1L))

