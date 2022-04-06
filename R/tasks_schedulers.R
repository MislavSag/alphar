library(emayili)


# globals
package_path <- file.path("C:/Users/Mislav/Documents/GitHub/alphar/R")

# email template
send_email <- function(message = enc2utf8("Task Scheduler: Market data and exuber update."),
                       sender = "mislav.sagovac@contentio.biz",
                       recipients = c("mislav.sagovac@contentio.biz")) {
  email <- emayili::envelope()
  email <- email %>%
    emayili::from(sender) %>%
    emayili::to(recipients) %>%
    emayili::subject("Monitoring") %>%
    emayili::text(message)
  smtp <- server(host = "mail.contentio.biz",
                 port = 587,
                 username = "mislav.sagovac+contentio.biz",
                 password = "Contentio0207")
  smtp(email, verbose = TRUE)
}

# run script and send e-mail if error occurs
a <- tryCatch({
  source(file.path(package_path, "get_market_data_update.R"))
}, error = function(e) {
  send_email("There is an error in eoglasna_code_no_ocr script. Market data not updated.")
  return(NA)
})

# update exuber if market data are successfully finished
if (!all(is.na(a))) {
  exuber_update <- tryCatch({
    source(file.path(package_path, "strategy_exuber_update.R"))
  }, error = function(e) {
    send_email("There is an error in stratefy_exuber_update script. Exuber not updated.")
    return(NA)
  })
}
