library(data.table)
library(httr)
library(janitor)
library(stringr)
library(writexl)


options(scipen = FALSE)

# globals
host <- "https://sudreg-api.pravosudje.hr"
host_api <- "https://api.data-api.io/v1"

# utils
get_sreg <- function(url) {
  res <- GET(url,
             add_headers("Ocp-Apim-Subscription-Key" = "0c7a9bbd34674a428e4218340fba732b",
                         "Host" = "sudreg-api.pravosudje.hr"))
  content(res)
}

# subjekti
url <- modify_url(host, path = "javni/subjekt",
                  query = list(offset = 0,
                               limit = format(1000000, scientific = FALSE),
                               only_active = FALSE))
subjekti <- get_sreg(url)
subjekti <- rbindlist(subjekti, fill = TRUE)

# emails
url <- modify_url(host, path = "javni/email_adrese",
                  query = list(offset = 0,
                               limit = format(1000000, scientific = FALSE)))
emails <- get_sreg(url)
emails <- rbindlist(emails, fill = TRUE)

# merge all data
dt <- Reduce(function(x , y) merge(x, y, by = "mbs", all.x = TRUE, all.y = FALSE),
             list(subjekti, emails))

# test
dt[oib == "96659557658"]

# save
write_xlsx(as.data.frame(dt), "D:/ds_projects/versus/outputs/versus002.xlsx")
