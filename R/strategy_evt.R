library(fastverse)
library(lubridate)
library(roll)
library(ggplot2)


# SET UP ------------------------------------------------------------------
# Globals
QCDATA   = "F:/lean/data"
DATA     = "F:/data"
# PATHSAVE = "D:/features/pra"


# MARKET DATA -------------------------------------------------------------
# Import hour data for specific symbols
prices = fread(file.path(QCDATA, "sp_500_hour.csv"))
setnames(prices, gsub(" ", "_", c(tolower(colnames(prices)))))

# Remove duplicates
prices = unique(prices, by = c("symbol", "date"))

# Remove duplicates - there are same for different symbols (eg. phun and phun.1)
prices_daily = fread("F:/lean/data/stocks_daily.csv")
setnames(prices_daily, gsub(" ", "_", c(tolower(colnames(prices_daily)))))
dups = prices_daily[, .(symbol , n = .N),
                    by = .(date, open, high, low, close, volume, adj_close,
                           symbol_first = substr(symbol, 1, 1))]
dups = dups[n > 1]
dups[, symbol_short := gsub("\\.\\d$", "", symbol)]
symbols_remove = dups[, .(symbol, n = .N),
                      by = .(date, open, high, low, close, volume, adj_close,
                             symbol_short)]
symbols_remove[n >= 2, unique(symbol)]
symbols_remove = symbols_remove[n >= 2, unique(symbol)]
symbols_remove = symbols_remove[grepl("\\.", symbols_remove)]
prices = prices[symbol %notin% symbols_remove]

# Free memory
rm(prices_daily)
gc()

# Check timezone
attr(prices$date, "tz")
prices[, data.table::first(date, 20)]
prices[, date := force_tz(date, "America/New_York")]

# Adjust all columns
prices[, adj_rate := adj_close / close]
prices[, let(
  open = open*adj_rate,
  high = high*adj_rate,
  low = low*adj_rate
)]
setnames(prices, "close", "close_raw")
setnames(prices, "adj_close", "close")
prices[, let(adj_rate = NULL)]
setcolorder(prices, c("symbol", "date", "open", "high", "low", "close", "volume"))

# Remove observations where open, high, low, close columns are below 1e-008
# This step is opional, we need it if we will use finfeatures package
prices = prices[open > 1e-008 & high > 1e-008 & low > 1e-008 & close > 1e-008]

# Remove missing values
prices = na.omit(prices)

# Keep observations with at least one month of observations
id_n = prices[, .N, by = symbol]
remove_symbols = id_n[N < (7056), unique(symbol)]
prices = prices[symbol %notin% remove_symbols]

# Sort
setorder(prices, symbol, date)

# Calculate returns
prices[, returns := close / shift(close) - 1, by = .(symbol)]

# Remove NA values
prices = na.omit(prices)

# Remove columns we don't need
prices = prices[, let(open = NULL, high = NULL, low = NULL)]

# free memory
rm(list = c("dups", "id_n"))
gc()


# EXTREMES ----------------------------------------------------------------
# take sample
dt = prices[symbol == sample(unique(prices$symbol), 1)]

# Rolling quantiles
dt[,  q_4 := roll_quantile(returns, width = nrow(dt), min_obs = 7*252, p = 0.03)]
dt[,  q_96 := roll_quantile(returns, width = nrow(dt), min_obs = 7*252, p = 0.97)]

# Extreme returns
dt[, extreme := ifelse(returns < q_4 | returns > q_96, 1, 0)]

# Plot extreme values
ggplot(dt, aes(x = date, y = returns)) +
  geom_line() +
  geom_point(aes(color = extreme)) +
  theme_minimal() +
  theme(legend.position = "none")

# Remove missing values
dt = na.omit(dt)

# Define clusters
dt[, extreme_cluster := 0]  # Initialize cluster column
cluster_id <- 0             # Initialize cluster counter

# Loop through each row to define clusters
for (i in 1:nrow(dt)) {
  # If it's an extreme event
  if (dt$extreme[i] == 1) {
    # Check if it's within 8 bars of the previous extreme
    if (i > 1 && dt$extreme[i - 1] == 1 && (i - which(dt$extreme == 1)[max(which(which(dt$extreme == 1) < i))]) <= 8) {
      # Assign the current cluster ID
      dt$extreme_cluster[i] <- cluster_id
    } else {
      # Start a new cluster
      cluster_id <- cluster_id + 1
      dt$extreme_cluster[i] <- cluster_id
    }
  }
}

# Filter out non-extreme events from cluster analysis (optional)
dt_clusters <- dt[extreme_cluster != 0]

# Plot with vertical lines indicating cluster starts
cluster_starts <- dt[extreme_cluster != 0, .(cluster_start = min(date)), by = extreme_cluster]
ggplot(dt, aes(x = date, y = returns)) +
  geom_line() +
  geom_point(aes(color = as.factor(extreme_cluster)), size = 2) +
  geom_vline(data = cluster_starts, aes(xintercept = as.numeric(cluster_start)),
             color = "red", linetype = "dashed") +
  scale_color_viridis_d(option = "plasma", name = "Cluster ID") +
  theme_minimal() +
  theme(legend.position = "none") +
  labs(title = "Clusters of Extremes with Start Indicators", x = "Date", y = "Returns")

# Number of extremes across clusters
dt[, .N, by = extreme_cluster][N > 1]


