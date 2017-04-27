# get the Hadley Wickham's vehicles data set
library(RColorBrewer)
library(ape)

opt <- NULL
 opt$start <- 1
 opt$cores <- 20
 opt$id <- 63
 opt$days <- 90
#######################################################################
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
  query <- sprintf("SELECT * FROM \`keywords\` WHERE \`client\` = \'%s\'",  opt$id )
  # Submit the fetch query and disconnect
  data <- dbGetQuery(db, query)
  dbDisconnect(db)
  data[sapply(data, is.character)] <- lapply(data[sapply(data, is.character)], as.factor)
  data
}
########################################################

keywords <- unique(loadKeywords())

kd <- keywords %>% select(keyword, category)


library(stringdist)
library(ggdendro)
library(ape)
library(ggplot2)
library(stats)


loankws <- unique(as.character(kd[kd$category == "loans",1]))
creditcardkws <- unique(as.character(kd[kd$category == "creditcard",1]))




# call the stringdistmatrix function and request 20 groups

#
loans_dist_jw <- stringdistmatrix(loankws, loankws, method = "jw")
creditcards_dist_jw <- stringdistmatrix(creditcardkws,creditcardkws,method = "jw")
#
#
k1 = 8
k2 = 10
#
rownames(loans_dist_jw) <- loankws
rownames(creditcards_dist_jw) <- creditcardkws
#
hc_loans_jw <- hclust(as.dist(loans_dist_jw))
hc_creditcards_jw <- hclust(as.dist(creditcards_dist_jw))
#




colorsl = brewer.pal(k1,"Dark2")
colorsc = brewer.pal(k2,"Dark2")
clusloans = cutree(hc_creditcards_jw, k = k1)
cluscards =   cutree(hc_loans_jw, k = k2)
#plot(as.phylo(hc_creditcards_jw),   main = "Credit Cards, String Similarty",sub = "Jaro-Winkler Algorithm",   tip.color = colorsl[clusloans],
  #    label.offset = 0, cex = 0.6)
#plot(as.phylo(hc_loans_jw),   main = "Loans, String Similarity: Jaro-Winkler", sub = "Jaro-Winkler Algorithm",  tip.color = colorsc[cluscards],
  #   label.offset = 0, cex = 0.6)


ColAttr <- function(x, attrC, ifIsNull) {
  # Returns column attribute named in attrC, if present, else isNullC.
  atr <- attr(x, attrC, exact = TRUE)
  atr <- if (is.null(atr)) {ifIsNull} else {atr}
  atr
}

AtribLst <- function(df, attrC, isNullC){
  # Returns list of values of the col attribute attrC, if present, else isNullC
  lapply(df, ColAttr, attrC=attrC, ifIsNull=isNullC)
}

attrloan <- AtribLst(cluscards[cluscards==2], attrC="names", isNullC=NA)





library(dendextend)
