library(httr)
library(rjson)
library(jsonlite)
library(tidyr)
library(tidyjson)  
library(dplyr) 
options(stringsAsFactors = FALSE) 

user <- "paul@mediaskunkworks.com"
pass <- "cc0fa701711c91453d9a6b2e0fdc047d"

apiURL <- "https://api.reach.ai" 



body <- '{"input":{"url":"http://mediaskunkworks.com",  "maxPages" : 10}, "type" : "ai.reach.Event.ContactFinder"}'

# Form encoded
response <- POST(apiURL, body = body, encode = "json", authenticate(user, pass))

api_r <- content(response, as="text", encoding = "utf8" )   


   # gather (stack) the array by index
  spread_values(            # spread (widen) values to widen the data.frame
    name = jstring("name"), # value of "name" becomes a character column
    age = jnumber("age")    # value of "age" becomes a numeric column
  )

usable <- jsonlite::fromJSON(api_r,simplifyVector = TRUE, flatten=TRUE)
str(usable)


encQuery <- URLencode(query)
response <- GET(encQuery, authenticate(user, pass))
api_r <- content(response, as="text")
api_r <- jsonlite::fromJSON(api_r,simplifyVector = TRUE, flatten=TRUE)


r <- POST("http://httpbin.org/post", body = list(a = 1, b = 2, c = 3))

url <- "http://httpbin.org/post"
body <- list(a = 1, b = 2, c = 3)

# Form encoded
r <- POST(url, body = body, encode = "form")
# Multipart encoded
r <- POST(url, body = body, encode = "multipart")
# JSON encoded
r <- POST(url, body = body, encode = "json")


json <- '{
  "name": "bob",
  "shopping cart": 
    [
      {
        "date": "2014-04-02",
        "basket": {"books": 2, "shirts": 0}
      },
      {
        "date": "2014-08-23",
        "basket": {"books": 1}
      }
    ]
}'
json %>% as.tbl_json %>% 
  spread_values(customer = jstring("name")) %>% # Keep the customer name
  enter_object("shopping cart") %>%             # Look at their cart
  gather_array %>%                              # Expand the data.frame and dive into each array element
  spread_values(date = jstring("date")) %>%     # Keep the date of the cart
  enter_object("basket") %>%                    # Look at their basket
  gather_keys("product") %>%                    # Expand the data.frame for each product and capture it's name
  append_values_number("quantity")      
