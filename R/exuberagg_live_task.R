library(emayili)


# globals
package_path <- file.path("C:/Users/Mislav/Documents/GitHub/alphar/R")

# email template
send_email <- function(message = enc2utf8("Task Scheduler: Exuber Aggregate Live."),
                       sender = "mislav.sagovac@contentio.biz",
                       recipients = c("mislav.sagovac@contentio.biz")) {
  email <- emayili::envelope()
  email <- email %>%
    emayili::from(sender) %>%
    emayili::to(recipients) %>%
    emayili::subject("Exuber Aggregate Live") %>%
    emayili::text(message)
  smtp <- server(host = "mail.contentio.biz",
                 port = 587,
                 username = "mislav.sagovac+contentio.biz",
                 password = "Contentio0207")
  smtp(email, verbose = TRUE)
}

# run script and send e-mail if error occurs
a <- tryCatch({
  source(file.path(package_path, "exuberagg_live.R"))
}, error = function(e) {
  send_email(paste0("There is an error in exuberagg_live script: ", e))
  return(NA)
})
