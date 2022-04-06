library(data.table)
library(equityData)

rm(list = ls())

# update earning announcements
events <- update_earning_announcements(blob_file = "earnings-calendar.rds")

# update earning announcements

# update prices
