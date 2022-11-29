# execute_plumber.R
# Execute plumber
plumber::plumb(file="C:/Users/Mislav/Documents/GitHub/alphar/R/plumber_deploy/plumber.R")$run(host = '0.0.0.0', port = 8080)

