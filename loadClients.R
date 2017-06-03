

options(mysql = list(
  "host" = "10.0.160.45",
  "port" = 3306,
  "user" = "paulreilly",
  "password" = "ut5bTEALj4Ff8WrZ"))
databaseName <- "ranking-report"
table <- "rankings-history"
clients <- "clients"



loadClients <- function() {
  # Connect to the database
  db <- dbConnect(MySQL(), dbname = databaseName, host = options()$mysql$host,
                  port = options()$mysql$port, user = options()$mysql$user,
                  password = options()$mysql$password)
  #Contruct Query ========
  query <- sprintf("SELECT * FROM \`%s\`", clients)
  # Submit the fetch query and disconnect
  data <- dbGetQuery(db, query)
  dbDisconnect(db)
  data[sapply(data, is.character)] <- lapply(data[sapply(data, is.character)], as.factor)
  data
}

clients <- unique(loadClients())


