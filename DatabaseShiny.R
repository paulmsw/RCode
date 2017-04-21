library(RMySQL)

options(mysql = list(
  "host" = "10.0.160.45",
  "port" = 3306,
  "user" = "paulreilly",
  "password" = "ut5bTEALj4Ff8WrZ"))
databaseName <- "ranking-report"
table <- "rankings-history"


now <- as.Date(Sys.Date())
lag <- 90
clientID <-  63

loadData <- function() {
  # Connect to the database
  db <- dbConnect(MySQL(), dbname = databaseName, host = options()$mysql$host, 
                  port = options()$mysql$port, user = options()$mysql$user, 
                  password = options()$mysql$password)
  #Contruct Query ========
  query <- sprintf("SELECT rh.date, kw.category, rh.keyword, rh.position, rh.host, kw.country FROM \`rankings-history\` rh join keywords kw WHERE rh.keyword = kw.keyword AND kw.client=%s AND rh.date >= \'%s\' and rh.date < \'%s\' ", clientID, (now - (lag+1)),(now-lag))
  query
   # Submit the fetch query and disconnect
  data <- dbGetQuery(db, query)
  dbDisconnect(db)
  data
}


ShowClientIDs <- function() {
  # Connect to the database
  db <- dbConnect(MySQL(), dbname = databaseName, host = options()$mysql$host, 
                  port = options()$mysql$port, user = options()$mysql$user, 
                  password = options()$mysql$password)
  #Contruct Query ========
  query <- ("SELECT * from \`clients\`")
  query
  # Submit the fetch query and disconnect
  data <- dbGetQuery(db, query)
  dbDisconnect(db)
  data
}

df <- loadData()
clients <- ShowClientIDs()


  
  
  
