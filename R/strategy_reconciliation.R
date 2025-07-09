library(fastverse)
library(finutils)
library(arrow)
library(dplyr)


# DATA --------------------------------------------------------------------
# Import SP500 universe
etf = "spy"
dir_ = file.path("F:/lean/data/equity/usa/universes/etf", etf)
files = list.files(dir_, full.names = TRUE)
constituents = lapply(files, fread)
constituents = rbindlist(constituents)
setnames(constituents,
         c("symbol", "qcsymbol", "date", "weight", "shares_held", "market_value"))
constituents[, symbol := tolower(symbol)]
index_symbols = constituents[, unique(symbol)]

# Import daily market data
prices = qc_daily(
  file_path = "F:/lean/data/stocks_daily.csv",
  symbols = c(index_symbols, etf),
  min_obs = 252,
  add_dv_rank = FALSE,
  add_day_of_month = FALSE
)

# Import forecasts
forecasts = open_dataset("F:/data/equity/us/predictors_daily/forecasts",
                         format = "parquet") |>
  filter(symbol %in% c(index_symbols, etf)[-405]) |>
  collect()
setDT(forecasts)





library(FoReco)

# Data
data(vndata)      # dataset
data(vnaggmat)    # Agg mat matrix

head(vndata)
head(vnaggmat)

model <- setNames(vector(mode='list', length=NCOL(vndata)), colnames(vndata))
fc_obj <- setNames(vector(mode='list', length=NCOL(vndata)), colnames(vndata))

# ETS model with log transformation
ets_log <- function(x, ...){
  x[x==0] <- min(x[x!=0])/2
  ets(x, lambda = 0, ...)
}

for(i in 1:NCOL(vndata)){
  model[[i]] <- ets_log(vndata[, i])
  fc_obj[[i]] <- forecast(model[[i]], h = 12)
}

# Point forecasts
base <- do.call(cbind, lapply(fc_obj, function(x) x$mean))
str(base, give.attr = FALSE)
#>  Time-Series [1:12, 1:525] from 2017 to 2018: 50651 21336 24567 29800 22846 ...
# Residuals
res <- do.call(cbind, lapply(fc_obj, residuals, type = "response"))
str(res, give.attr = FALSE)
#>  Time-Series [1:228, 1:525] from 1998 to 2017: 2143 -970 -115 133 951 ...











library(forecast)
library(fabletools)
library(fable)


tourism <- tsibble::tourism |>
  mutate(State = recode(State,
                        `New South Wales` = "NSW",
                        `Northern Territory` = "NT",
                        `Queensland` = "QLD",
                        `South Australia` = "SA",
                        `Tasmania` = "TAS",
                        `Victoria` = "VIC",
                        `Western Australia` = "WA"
  ))

tourism_states <- tourism |>
  aggregate_key(State, Trips = sum(Trips))

tourism_model = tourism_states |>
  model(ets = ETS(Trips))
attributes(tourism_model)
class(tourism_model)
fabletools:::forecast.lst_btmup_mdl

fabletools:::build_key_data_smat(tourism_states)
fabletools:::build_key_data_smat(attributes(tourism_states)$key)
fabletools:::build_key_data_smat(attributes(tourism_states))
fabletools:::build_key_data_smat(attributes(tourism_model)$key)

attributes(tourism_states)$key
attributes(tourism_states)$key$.rows
attributes(tourism_model)$key
attributes(tourism_model)$key$.rows


object = tourism_model$ets
key_data = attributes(tourism_model)$key
point_forecast = list(.mean = mean)
new_data = NULL

# Keep only bottom layer
agg_data <- build_key_data_smat(key_data)

S <- matrix(0L, nrow = length(agg_data$agg), ncol = max(vec_c(!!!agg_data$agg)))
S[length(agg_data$agg)*(vec_c(!!!agg_data$agg)-1) + rep(seq_along(agg_data$agg), lengths(agg_data$agg))] <- 1L

btm <- agg_data$leaf
# object <- object[btm]
# if(!is.null(new_data)){
#   new_data <- new_data[btm]
# }

point_method <- point_forecast
point_forecast <- list()

# Get base forecasts
# fc <- vector("list", nrow(S))
# fc[btm] <- NextMethod()
fc <- NextMethod()

# Add dummy forecasts to unused levels
# fc[seq_along(fc)[-btm]] <- fc[btm[1]]

P <- matrix(0L, nrow = ncol(S), ncol = nrow(S))
P[(btm-1L)*nrow(P) + seq_len(nrow(P))] <- 1L

reconcile_fbl_list(fc, S, P, W = diag(nrow(S)),
                   point_forecast = point_method)


fcasts_state <- tourism_states |>
  filter(!is_aggregated(State)) |>
  model(ets = ETS(Trips)) |>
  forecast()

# Sum bottom-level forecasts to get top-level forecasts
fcasts_national <- fcasts_state |>
  summarise(value = sum(Trips), .mean = mean(value))

tourism_states |>
  model(ets = ETS(Trips)) |>
  reconcile(bu = bottom_up(ets)) |>
  forecast()


xt = tourism_states |>
  model(ets = ETS(Trips))
x$ets[[1]]



bottom_up <- function(models){
  structure(models, class = c("lst_btmup_mdl", "lst_mdl", "list"))
}

# forecast.lst_btmup_mdl <- function(object, key_data,
#                                    point_forecast = list(.mean = mean),
#                                    new_data = NULL, ...){
#   # Keep only bottom layer
#   agg_data <- build_key_data_smat(key_data)

library(vctrs)
library(purrr)
library(rlang)

build_key_data_smat <- function(x){
  # x = attributes(xt)$key
  kv <- names(x)[-ncol(x)]
  agg_shadow <- as_tibble(purrr::map(x[kv], is_aggregated))
  grp <- as_tibble(vctrs::vec_group_loc(agg_shadow))
  num_agg <- rowSums(grp$key)
  # Initialise comparison leafs with known/guaranteed leafs
  x_leaf <- x[vec_c(!!!grp$loc[which(num_agg == min(num_agg))]),]

  # Sort by disaggregation to identify aggregated leafs in order
  grp <- grp[order(num_agg),]

  grp$match <- lapply(unname(split(grp, seq_len(nrow(grp)))), function(level){
    disagg_col <- which(!vec_c(!!!level$key))
    agg_idx <- level[["loc"]][[1]]
    pos <- vec_match(x_leaf[disagg_col], x[agg_idx, disagg_col])
    pos <- vec_group_loc(pos)
    pos <- pos[!is.na(pos$key),]
    # Add non-matches as leaf nodes
    agg_leaf <- setdiff(seq_along(agg_idx), pos$key)
    if(!is_empty(agg_leaf)){
      pos <- vec_rbind(
        pos,
        structure(list(key = agg_leaf, loc = as.list(seq_along(agg_leaf) + nrow(x_leaf))),
                  class = "data.frame", row.names = agg_leaf)
      )
      x_leaf <<- vec_rbind(
        x_leaf,
        x[agg_idx[agg_leaf],]
      )
    }
    pos$loc[order(pos$key)]
  })
  if(any(lengths(grp$loc) != lengths(grp$match))) {
    abort("An error has occurred when constructing the summation matrix.\nPlease report this bug here: https://github.com/tidyverts/fabletools/issues")
  }
  idx_leaf <- vec_c(!!!x_leaf$.rows)
  x$.rows[unlist(x$.rows)[vec_c(!!!grp$loc)]] <- vec_c(!!!grp$match)
  return(list(agg = x$.rows, leaf = idx_leaf))
  # out <- matrix(0L, nrow = nrow(x), ncol = length(idx_leaf))
  # out[nrow(x)*(vec_c(!!!x$.rows)-1) + rep(seq_along(x$.rows), lengths(x$.rows))] <- 1L
  # out
}



#' @export
forecast.lst_btmup_mdl <- function(object, key_data,
                                   point_forecast = list(.mean = mean),
                                   new_data = NULL, ...){
  # Keep only bottom layer
  agg_data <- build_key_data_smat(key_data)

  S <- matrix(0L, nrow = length(agg_data$agg), ncol = max(vec_c(!!!agg_data$agg)))
  S[length(agg_data$agg)*(vec_c(!!!agg_data$agg)-1) + rep(seq_along(agg_data$agg), lengths(agg_data$agg))] <- 1L

  btm <- agg_data$leaf
  # object <- object[btm]
  # if(!is.null(new_data)){
  #   new_data <- new_data[btm]
  # }

  point_method <- point_forecast
  point_forecast <- list()

  # Get base forecasts
  # fc <- vector("list", nrow(S))
  # fc[btm] <- NextMethod()
  fc <- NextMethod()

  # Add dummy forecasts to unused levels
  # fc[seq_along(fc)[-btm]] <- fc[btm[1]]

  P <- matrix(0L, nrow = ncol(S), ncol = nrow(S))
  P[(btm-1L)*nrow(P) + seq_len(nrow(P))] <- 1L

  reconcile_fbl_list(fc, S, P, W = diag(nrow(S)),
                     point_forecast = point_method)
}



reconcile_fbl_list <- function(fc, S, P, W, point_forecast, SP = NULL) {
  if(length(unique(map(fc, interval))) > 1){
    abort("Reconciliation of temporal hierarchies is not yet supported.")
  }
  if(!inherits(S, "matrix")) {
    # Use sparse functions
    require_package("Matrix")
    as.matrix <- Matrix::as.matrix
    t <- Matrix::t
    diag <- function(x) if(is.vector(x)) Matrix::Diagonal(x = x) else Matrix::diag(x)
    cov2cor <- Matrix::cov2cor
  } else {
    cov2cor <- stats::cov2cor
  }
  if(is.null(SP)) {
    SP <- S%*%P
  }

  fc_dist <- map(fc, function(x) x[[distribution_var(x)]])
  dist_type <- lapply(fc_dist, function(x) unique(dist_types(x)))
  dist_type <- unique(unlist(dist_type))
  is_normal <- all(map_lgl(fc_dist, function(x) all(dist_types(x) == "dist_normal")))

  fc_mean <- as.matrix(invoke(cbind, map(fc_dist, mean)))
  fc_var <- transpose_dbl(map(fc_dist, distributional::variance))

  # Apply to forecasts
  fc_mean <- as.matrix(SP%*%t(fc_mean))
  fc_mean <- split(fc_mean, row(fc_mean))
  if(identical(dist_type, "dist_normal")){
    R1 <- cov2cor(W)
    W_h <- map(fc_var, function(var) diag(sqrt(var))%*%R1%*%t(diag(sqrt(var))))
    fc_var <- map(W_h, function(W) diag(SP%*%W%*%t(SP)))
    fc_dist <- map2(fc_mean, transpose_dbl(map(fc_var, sqrt)), distributional::dist_normal)
  } else if (identical(dist_type, "dist_sample")) {
    sample_size <- unique(unlist(lapply(fc_dist, function(x) unique(lengths(distributional::parameters(x)$x)))))
    if(length(sample_size) != 1L) stop("Cannot reconcile sample paths with different replication sizes.")
    sample_horizon <- unique(lengths(fc_dist))
    if(length(sample_horizon) != 1L) stop("Cannot reconcile sample paths with different forecast horizon lengths.")
    # Extract sample paths
    samples <- lapply(fc_dist, function(x) distributional::parameters(x)$x)
    # Convert to array [samples,horizon,nodes]
    samples <- array(unlist(samples, use.names = FALSE), dim = c(sample_size, sample_horizon, length(fc_dist)))
    # Reconcile
    samples <- apply(samples, 1, function(x) as.matrix(SP%*%t(x)), simplify = FALSE)
    # Convert to array [nodes, horizon, samples]
    samples <- array(unlist(samples), dim = c(length(fc_dist), sample_horizon, sample_size))
    # Convert to distributions
    fc_dist <- apply(
      samples, 1L, simplify = FALSE,
      function(x) unname(distributional::dist_sample(split(x, row(x))))
    )
  } else {
    fc_dist <- map(fc_mean, distributional::dist_degenerate)
  }

  # Update fables
  map2(fc, fc_dist, function(fc, dist){
    dimnames(dist) <- dimnames(fc[[distribution_var(fc)]])
    fc[[distribution_var(fc)]] <- dist
    point_fc <- compute_point_forecasts(dist, point_forecast)
    fc[names(point_fc)] <- point_fc
    fc
  })
}
