library(rmarkdown)


# params
algo = "RiskMlAlgorithm"
file_name = "risk_ml.html"

# generate lean report
rmarkdown::render(
  input = file.path("R/lean_report.Rmd"),
  output_file = file_name,
  output_dir = file.path("backtests"),
  params = list(algortihm_name = algo)
  )


