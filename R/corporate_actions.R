library(data.table)
library(tiledb)
library(ggplot2)
library(TTR)


# configure s3
config <- tiledb_config()
config["vfs.s3.aws_access_key_id"] <- Sys.getenv("AWS-ACCESS-KEY")
config["vfs.s3.aws_secret_access_key"] <- Sys.getenv("AWS-SECRET-KEY")
config["vfs.s3.region"] <- Sys.getenv("AWS-REGION")
context_with_config <- tiledb_ctx(config)

# get corporate aciotns
arr <- tiledb_array("s3://equity-usa-corporateactions", as.data.frame = TRUE)
ca_raw <- arr[]

#
ca <- as.data.table(ca_raw)
splits <- ca[query == "bankruptcies", .N, by = "date"]

# vis
ggplot(splits, aes(x = date, y = N)) +
  geom_line()

ggplot(splits[date %between% c("2020-01-01", "2021-01-01")], aes(x = date, y = N)) +
  geom_line()
ggplot(splits[date %between% c("2007-01-01", "2008-01-01")], aes(x = date, y = N)) +
  geom_line()


unique(ca$query)

# get ipos
arr <- tiledb_array("s3://equity-usa-ipo")
ipo <- arr[]
ipo_dt <- as.data.table(ipo)

# visualize
indicator <- ipo_dt[, .N, by = .(effectivenessDate)]
indicator <- indicator[effectivenessDate %between% c("2014-01-01", "2022-11-01")]
ggplot(indicator, aes(effectivenessDate)) +
  geom_line(aes(y = N))

# 2
indicator <- ipo_dt[effectivenessDate %between% c("2014-01-01", "2022-11-01")]
indicator <- indicator[, .N, by = .(year(effectivenessDate), week(effectivenessDate))]
setorder(indicator, year, week)
ggplot(indicator, aes(1:nrow(indicator))) +
  geom_line(aes(y = N))

