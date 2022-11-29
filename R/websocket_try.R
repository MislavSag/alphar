library(websocket)
library(jsonlite)


ws <- WebSocket$new("wss://websockets.financialmodelingprep.com", accessLogChannels = "all",
                    errorLogChannels = "all")
Sys.sleep(2L)

login = list(
  'event' = 'login',
  'data' = list(
    'apiKey' = "15cd5d0adf4bc6805a724b4417bbaafc"
  )
)

subscribe = list(
  'event' = 'subscribe',
  'data' = list(
    'ticker' = "aapl"
  )
)

unsubscribe = list(
  'event' = 'unsubscribe',
  'data' = list(
    'ticker' = "aapl"
  )
)

ws$onOpen(function(event) {
  ws$send(toJSON(login))
  Sys.sleep(2L)
  ws$send(toJSON(subscribe))
  Sys.sleep(2L)
})

ws$onMessage(function(event) {
  # print(fromJSON(event$data))
  print(event)
})

ws$onClose(function(event) {
  cat("Error: Client disconnected with code ", event$code, "\n")
})

ws$onError(function(event) {
  cat("Error: Client failed to connect: ", event$message, "\n")
})

httpuv::service(Inf)



# unsubscribe and close
ws$send(toJSON(unsubscribe))
ws$close()

