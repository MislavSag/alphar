library(emayili)


# email template
send_email <- function(message = enc2utf8("There is an error in exuber agg live file."),
                       sender = "mislav.sagovac@contentio.biz",
                       recipients = c("mislav.sagovac@contentio.biz")) {
  email <- emayili::envelope()
  email <- email %>%
    emayili::from(sender) %>%
    emayili::to(recipients) %>%
    emayili::subject("Exuber Aggregate Live Error") %>%
    emayili::text(message)
  smtp <- server(host = "mail.contentio.biz",
                 port = 587,
                 username = "mislav.sagovac+contentio.biz",
                 password = "Contentio0207")
  smtp(email, verbose = TRUE)
}

# run script and send e-mail if error occurs
a <- tryCatch({
  source("C:/Users/Mislav/Documents/GitHub/alphar/R/exuberagg_live.R")
}, error = function(e) {
  send_email()
})
