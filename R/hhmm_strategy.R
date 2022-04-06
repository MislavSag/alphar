library(fHMM)


path <- paste0(tempdir(),"/aapl.csv")
download_data(symbol = "AAPL", file = path, verbose = FALSE)


controls <- list(
  states = 3,
  sdds   = "t",
  data   = list(file        = path,
                date_column = "Date",
                data_column = "Close",
                logreturns  = TRUE,
                from        = "2000-01-01",
                to          = "2020-12-31")
)
controls <- set_controls(controls)

data <- prepare_data(controls)
summary(data)

set.seed(1)
model <- fit_model(data, ncluster = 7)

model <- decode_states(model)

summary(model)

events <- fHMM_events(
  list(dates = c("2001-09-11", "2008-09-15", "2020-01-27"),
       labels = c("9/11 terrorist attack", "Bankruptcy Lehman Brothers", "First COVID-19 case Germany"))
)
plot(model, plot_type = c("sdds","ts"), events = events)

predict(model)
