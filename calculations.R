#!/usr/bin/env Rscript
library("optparse")


option_list = list(
  make_option(c("-i", "--id"), type="numeric", default=NULL,
              help="set the client ID eg: -i 63,  (will return results for MoneyGuru)", metavar="numeric"),
  make_option(c("-o", "--out"), type="numeric", default=1,
              help="set log output eg: -o 0, (will turn off logging) \n  [default= %default / ie On]", metavar="numeric")
);

opt_parser = OptionParser(option_list=option_list);
opt = parse_args(opt_parser);

if (is.null(opt$id)){
  print_help(opt_parser)
  stop("At least one argument must be supplied: clientID, using the -i switch \n\nExample: summarypipe -i 63 -o 0 (to process MoneyGuru.com's data for the past 90 days)\n\n\n", call.=FALSE)
}

# Testing Variables
opt <- NULL
opt$id <- 63
opt$lag <- 0
opt$out <- 1
opt$days <- 90
clientID <- opt$id




library(dplyr)
library(RcppRoll)
library(RollingWindow)
library(stringr)
library(foreach)
options(stringsAsFactors=T)


# Define DB Connection
library(RMySQL)
options(mysql = list(
  "host" = "10.0.160.45",
  "port" = 3306,
  "user" = "paulreilly",
  "password" = "ut5bTEALj4Ff8WrZ"))
databaseName <- "ranking-report"
table <- "rankings-history"

loadKeywords <- function() {
  # Connect to the database
  db <- dbConnect(MySQL(), dbname = databaseName, host = options()$mysql$host,
                  port = options()$mysql$port, user = options()$mysql$user,
                  password = options()$mysql$password)
  #Contruct Query ========
  query <- sprintf("SELECT * FROM \`keywords\` WHERE \`client\` = \'%s\'", clientID)
  # Submit the fetch query and disconnect
  data <- dbGetQuery(db, query)
  dbDisconnect(db)
  data[sapply(data, is.character)] <- lapply(data[sapply(data, is.character)], as.factor)
  data
}

# Pull & Name Keyword and Campaign names from Reports Database
keywords <- unique(loadKeywords())
kw_cat_tracked <- keywords %>% select(keyword, category)
colnames(kw_cat_tracked) <- c("Keyword", "Category")



# Load Keyword planner data into memory

cwd <- "~/Data/RandD/AppData/"
kwpdir <- "~/Data/RandD/AppData/kwpdata/"
setwd(kwpdir)
clientID <- opt$id
clientID


kwp <- readRDS(paste(cwd, "kwpdata/", clientID, "_kwp.RDS", sep = ""))




jdf <- inner_join(kwp, kw_cat_tracked,  by = "Keyword")
jdf$Keyword <- as.factor(jdf$Keyword)

#Compute Potential Income

jdf$Potential <- (jdf$Impressions * 0.33) * jdf$Bid

#Plot Value Distribution

#Determine Reasonable Cut Off of Organic Search keyword targets

df1K <- jdf %>% filter(Potential >= 1000)




