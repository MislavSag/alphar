libs_to_load <- c("tidyverse", "lubridate", "purrr", "zoo", "glue", "slider", "patchwork")
pacman::p_load(char = libs_to_load, install = FALSE)

funding <- read_csv("https://storage.googleapis.com/tlaq_public/FTX_PERPETUAL_FUNDING_RATES.csv")

funding %>%
  filter(ticker %in% c("BTC-PERP", "ETH-PERP", "SOL-PERP", "DOGE-PERP", "SLP-PERP", "ATLAS-PERP")) %>%
  group_by(ticker) %>%
  ggplot(aes(x = date, y = rate)) +
  geom_line() +
  facet_wrap(~ticker, ncol = 2) +
  labs(title = "Perpetual funding rates")


daily_funding <- funding %>%
  mutate(day = date(date), hour = hour(date)) %>%
  group_by(ticker, day) %>%
  summarise(rate = sum(rate, na.rm = TRUE))


avg_daily_funding <- daily_funding %>%
  group_by(ticker) %>%
  summarise(avg_funding = mean(rate, na.rm = TRUE))

# average daily funding rate of all coins
all_coins_avg <- avg_daily_funding %>% pull(avg_funding) %>% mean(na.rm = TRUE)
all_coins_avg*100*365

# create funding model
funding_model <- daily_funding %>%
  group_by(ticker) %>%
  mutate(
    # predicted funding is 7x mean funding over last 4 days
    predicted_funding = 7*(rate + dplyr::lag(rate, 1) + dplyr::lag(rate, 2) + dplyr::lag(rate, 3))/4,
    # actual funding is the sum of funding over the forward 7 days
    actual_funding = dplyr::lead(rate, 1) + dplyr::lead(rate, 2) + dplyr::lead(rate, 3) + dplyr::lead(rate, 4) + dplyr::lead(rate, 5) + dplyr::lead(rate, 6) + dplyr::lead(rate, 7)
  )

funding_model %>%
  group_by(ticker) %>%
  filter(row_number() %% 7 == 0) %>%
  ungroup()

futs <- read_csv("https://storage.googleapis.com/tlaq_public/FTX_PERPETUALS_OHLC.csv")

head(futs)

daily_futs <- futs %>%
  separate(date, into = c("date", "time"), sep = " ") %>%
  group_by(ticker, date) %>%
  summarise(
    open = first(open),
    high = max(high),
    low = min(low),
    close = last(close),
    volume = sum(volume)
  ) %>%
  mutate(date = as_date(date))

tail(daily_futs)
setDT(daily_futs)[ticker %like% "^BTC"]
spot <- read_csv("https://storage.googleapis.com/tlaq_public/FTX_SPOT_1H_CLEAN.csv")

head(spot)

# wrangle into daily
daily_spot <- spot %>%
  separate(date, into = c("date", "time"), sep = " ") %>%
  group_by(ticker, date) %>%
  summarise(
    open = first(open),
    high = max(high),
    low = min(low),
    close = last(close),
    volume = sum(volume)
  ) %>%
  mutate(date = as_date(date))

tail(daily_spot)
