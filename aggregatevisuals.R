# #!/usr/bin/env Rscript
# library("optparse")
# 
# 
# option_list = list(
#   #make_option(c("-c", "--cores"), type="numeric", default=30,
#   #              help="set number of cores for parallelism", metavar="numeric"),
#   make_option(c("-i", "--id"), type="numeric", default=NULL,
#               help="set the client ID eg: -i 63,  (will return results for MoneyGuru)", metavar="numeric"),
#   make_option(c("-o", "--out"), type="numeric", default=54,
#               help="set log output eg: -o 0, (will turn off logging) \n  [default= %default / ie On]", metavar="numeric"),
#   make_option(c("-l", "--lag"), type="numeric", default=1,
#               help="set lag in days - [default= %default, ie today]", metavar="numeric")
# );
# 
# opt_parser = OptionParser(option_list=option_list);
# opt = parse_args(opt_parser);
# 
# if (is.null(opt$id)){
#   print_help(opt_parser)
#   stop("At least one argument must be supplied: clientID, using the -i switch \n\nExample: summarypipe -i 63 -o 0 (to process MoneyGuru.com's data for the past 90 days)\n\n\n", call.=FALSE)
# }
domains <- as.data.frame(unique(dt$Domain))
write.csv(domains, "domains.csv")
getwd()

setwd("/home/paul/Data/RandD/UK")


opt <- NULL
opt$start <- 1


opt$id <- 1
opt$lag <- 3
 opt$out <- 0
opt$days <- 90
patt <- "^smonk"


options(stringsAsFactors = FALSE) 


clientID <- opt$id
library(bookdown)
library(dplyr)
library(RcppRoll)
library(RollingWindow)
library(stringr)
library(foreach)
options(stringsAsFactors=T)

cwd <- "~/Data/RandD/AppData/"

kwpdir <- "~/Data/RandD/AppData/kwpdata/"
setwd(kwpdir)

#Change to correct folder

now <- Sys.Date() -(opt$lag)

now <- format(now, format="%Y-%m-%d")
now
clientID <- opt$id
clientID
dirname <- paste(now, clientID, sep="-")
dirname
fullpath <- paste(cwd, dirname,"/", sep = "")
fullpath
filepath <- paste(cwd, "kwpdata/", clientID, "_kwp.RDS", sep = "")
filepath
print("Processing...")
print(clientID)
print(filepath)


#Check for todays's client folder and if it's not there, put it there.
if (!dir.exists(fullpath)) {
  dir.create(fullpath, showWarnings = TRUE, recursive = FALSE, mode = "0777")
  print(paste("creating dir:", dirname, sep = " " ))
} else {
  print("Data directory exists")
}


# Do cleaning in preparation for the loop tings!

if (dir.exists(fullpath)) {
  cat("Getting list of files") #####################################
  ##################################### Load data
  
  print("file pattern:-")
  print(patt)
  
  filelist  <- list.files(path = fullpath, pattern = as.character(patt), all.files = FALSE,
                          full.names = FALSE, recursive = FALSE,
                          ignore.case = FALSE, include.dirs = FALSE, no.. = FALSE)
  numfiles <- length(filelist)
} else {
  stop("No directory of that name run 'pullrank.R' to populate with ranking data\n\n", call.=FALSE)
}
#####################################

require(stringr)

# #Go to client directory
setwd(fullpath)
print(paste("switched to:", fullpath, sep = " " ))

#Check for logs dir...  and if it's not there, put it there.
if (!dir.exists(paste(fullpath,"logs", sep="/"))) {
  dir.create(paste(fullpath,"logs", sep="/"), showWarnings = TRUE, recursive = FALSE, mode = "0777")
  print(paste("creating dir:", fullpath, sep = " " ))
} else {
  print("Log directory exists ")
}

#Test for output switch to logging console output
if (opt$out == 1){
  print(paste("piping logs to: ",fullpath,"logs/aggregated.txt", sep = ""))
  #create the summarylog.txt
  writeLines(c(""), paste(fullpath,"logs/aggregated.txt", sep = ""))
  #redirect output to summarylog.txt
  sink(paste(fullpath,"logs/summarylog.txt", sep = ""), append = TRUE)
}

file_b <- NULL 



### 

### Load the 3 monthly samples
foreach(i = 1:numfiles, .packages = c("dplyr", "RollingWindow")) %do% {
  file_i <- readRDS(paste(fullpath, filelist[i], sep = "/"))
  file_b <- rbind(file_i, file_b) 
}

df <- file_b %>% dplyr::select(-Category) 


df$Keyword <- as.factor(df$Keyword)
df$Domain <- as.factor(df$Domain)
df$Category <- as.factor(df$Category)
df$SLD <- as.factor(df$SLD)

dt <- df

##### figure out leaderboard plus and minus 5 and their change
summary(dt)
three  <- dt %>% 
  dplyr::filter(monSlice == 3) %>% 
  dplyr::select(monSlice, Keyword, Domain, rsum, pmean, isum, Product, NumberKeywords)  %>% 
  dplyr::group_by(monSlice, Domain, Product) %>% 
  summarise(rsum = sum(rsum),
            isum = sum(isum),
            pmean = mean(pmean),
            n = n()) 

Out_three<- three  %>% 
  dplyr::ungroup() %>%
  dplyr::select(Domain, rsum, pmean, isum, Product, n) %>% 
  dplyr::group_by(Product) %>% dplyr::arrange(desc(rsum))%>%
  dplyr::mutate(id = row_number())



two  <- dt %>% 
  dplyr::filter(monSlice == 2) %>% 
  dplyr::select(monSlice, Keyword, Domain, rsum, pmean, isum, Product, NumberKeywords)  %>% 
  dplyr::group_by(monSlice, Domain, Product) %>% 
  summarise(rsum = sum(rsum),
            isum = sum(isum),
            pmean = mean(pmean),
            n = n()) 


Out_two <- two  %>% 
  dplyr::ungroup() %>%
  dplyr::select(Domain, rsum, pmean, isum, Product, n) %>% 
  dplyr::group_by(Product) %>% dplyr::arrange(desc(rsum)) %>%
  dplyr::mutate(id = row_number())


one   <- dt %>% 
  dplyr::filter(monSlice == 1) %>% 
  dplyr::select(monSlice, Keyword, Domain, rsum, pmean, isum, Product, NumberKeywords)  %>% 
  dplyr::group_by(monSlice, Domain, Product) %>% 
  summarise(rsum = sum(rsum),
            isum = sum(isum),
            pmean = mean(pmean),
            n = n()) 


Out_one <- one  %>% 
  dplyr::ungroup() %>%
  dplyr::select(Domain, rsum, pmean, isum, Product, n) %>% 
  dplyr::group_by(Product) %>% dplyr::arrange(desc(rsum)) %>%
  dplyr::mutate(id = row_number())


blocklist <- c("wikipedia.org", "aol.com" , "twitter.com", "facebook.com")





################################################################################################################################################################################################
################################################################################################


if (dir.exists(fullpath)) {
  cat("Getting list of files")
  
  filelist  <- list.files(path = fullpath, pattern = "^smonk", all.files = FALSE,
                          full.names = FALSE, recursive = FALSE,
                          ignore.case = FALSE, include.dirs = FALSE, no.. = FALSE)
  numfiles <- length(filelist)
  
} else {
  stop("No directory of that name run 'pullrank.R' to populate with ranking data\n\n", call.=FALSE)
}


df <- file_b
df$Keyword <- as.factor(df$Keyword)
df$Domain <- as.factor(df$Domain)
df$Category <- as.factor(df$Category)
df$SLD <- as.factor(df$SLD)

HCL2 <- read.csv("~/Data/RandD/UK/HCL2.csv",stringsAsFactors = TRUE)

GamingTypes <- read.csv("~/Data/RandD/UK/GamingTypes.csv", stringsAsFactors = TRUE)
gt <- GamingTypes[,c(1,2)]


#######################Cont here
dtmaster <- inner_join(df, HCL2, by = "Keyword")
dt$Keyword <- as.factor(dt$Keyword)
dt$Product <- as.factor(dt$Product)
dt$monSlice <- as.factor(dt$monSlice)


mdf <- dt %>% dplyr::select(monSlice, Keyword, Domain, rsum, pmean, isum) %>%
  dplyr::group_by(monSlice, Domain, Keyword) %>%
  summarise(rsum = sum(rsum),
            isum = sum(isum),
            pmean = mean(pmean),
            n = n()) %>% top_n(10, -rsum)

##### figure out leaderboard plus and minus 5 and their change






all_iGaming_Sites <- read.csv("~/Data/iGaming/all_iGaming_Sites.csv", stringsAsFactors = TRUE)


library(tldextract)
urllist <- as.character(all_iGaming_Sites$Website)
# get most recent TLD listings
tld <- getTLD() # optionally pass in a different URL than the default
manyhosts <- c(urllist)
newurls <- tldextract(urllist, tldnames=tld)
all_iGaming_Sites$SLD <- as.factor(newurls$domain)
all_iGaming_Sites$Domain <- as.factor(paste(newurls$domain, newurls$tld, sep="."))


igdf <- as.data.frame(all_iGaming_Sites$Domain)

write.csv(igdf,"~/Data/RandD/UK/igdf.csv")






# Do cleaning in preparation for the loop tings!

if (dir.exists(fullpath)) {
  cat("Getting list of files")
  
  filelist  <- list.files(path = fullpath, pattern = "^sl7kw", all.files = FALSE,
                          full.names = FALSE, recursive = FALSE,
                          ignore.case = FALSE, include.dirs = FALSE, no.. = FALSE)
  numfiles <- length(filelist)
  
} else {
  stop("No directory of that name run 'pullrank.R' to populate with ranking data\n\n", call.=FALSE)
}


#####################################

require(stringr)

# #Go to client directory
setwd(fullpath)
print(paste("switched to:", fullpath, sep = " " ))

#Check for logs dir...  and if it's not there, put it there.
if (!dir.exists(paste(fullpath,"logs", sep="/"))) {
  dir.create(paste(fullpath,"logs", sep="/"), showWarnings = TRUE, recursive = FALSE, mode = "0777")
  print(paste("creating dir:", fullpath, sep = " " ))
} else {
  print("Log directory exists ")
}



#Test for output switch to logging console output
if (opt$out == 1){
  print(paste("piping logs to: ",fullpath,"logs/aggregated.txt", sep = ""))
  #create the summarylog.txt
  writeLines(c(""), paste(fullpath,"logs/aggregated.txt", sep = ""))
  #redirect output to summarylog.txt
  sink(paste(fullpath,"logs/summarylog.txt", sep = ""), append = TRUE)
}

file_b7 <- NULL
file_i <- NULL

### Load the 3 monthly samples
foreach(i = 1:numfiles, .packages = c("dplyr", "RollingWindow")) %do% {
  file_i <- readRDS(paste(fullpath, filelist[i], sep = "/"))
  file_b7 <- rbind(file_i, file_b7) 
}

mdf7 <- file_b7


df7m <- inner_join(mdf7, HCL2, by = "Keyword")
df7m$Keyword <- as.factor(df7m$Keyword)
df7m$Category <- as.factor(df7m$Category)
df7m$Product <- as.factor(df7m$Product)
df7m$monSlice <- as.factor(df7m$monSlice)

#######summaries 

#Fix Dates

mdf <- df7m %>% dplyr::select(monSlice, Keyword, Domain, rsum, pmean, isum) %>%
  dplyr::group_by(monSlice, Domain, Keyword) %>%
  summarise(rsum = sum(rsum),
            isum = sum(isum),
            pmean = mean(pmean),
            n = n()) %>% top_n(10, -rsum)

##### figure out leaderboard plus and minus 5 and their change



df7m <- inner_join(mdf7, HCL2, by = "Keyword")
df7m$Keyword <- as.factor(df7m$Keyword)
df7m$Category <- as.factor(df7m$Category)
df7m$Product <- as.factor(df7m$Product)
df7m$monSlice <- as.factor(df7m$monSlice)
df7m$Domain <- as.factor(df7m$Domain)
df7m$SLD <- as.factor(df7m$SLD)


df7m <- ungroup(df7m)

latest   <- df7m %>% 
  dplyr::select(Keyword, Domain, rsum, pmean, isum, Product, NumberKeywords)  %>% 
  dplyr::group_by(Domain, Product) %>% 
  summarise(rsum = sum(rsum),
            isum = sum(isum),
            pmean = mean(pmean),
            n = n())

Out_latest_byProduct <- latest  %>% 
  dplyr::ungroup() %>%
  dplyr::select(Domain, rsum, pmean, isum, Product, n) %>% 
  dplyr::group_by(Product) %>% dplyr::arrange(desc(rsum)) %>%
  dplyr::mutate(id = row_number())

Out_latest <- Out_latest_byProduct   %>% 
  dplyr::ungroup() %>%
  dplyr::select(Domain, rsum, pmean, isum, Product, n) %>% 
  dplyr::arrange(desc(rsum)) %>%
  dplyr::mutate(id = row_number())

##### Biggest Movers

library(dplyr)


first <- Out_one %>% ungroup() %>% dplyr::select(Domain, Product,n, id) 
second <- Out_two %>% ungroup() %>% dplyr::select(Domain, Product,n, id)
third <- Out_three %>% ungroup() %>% dplyr::select(Domain, Product,n, id)
  
colnames(first) <- c("Domain", "Product","n_1" , "id_1")  
colnames(second) <- c("Domain", "Product", "n_2", "id_2")  
colnames(third) <- c("Domain", "Product", "n_3", "id_3")  

joined <- inner_join(second, third, by = c("Domain", "Product")) 

joined$id_diff <- -(joined$id_3 - joined$id_2 )
joined$n_diff <- joined$n_3 - joined$n_2


joined %>% dplyr::arrange(id_diff) %>% top_n(100, )
 


q <-  ggplot(df_cbr_1, aes(x = csum, y = pmean, size=nkw, color = Client, label=Keyword))  +
    geom_point(alpha = 1/3,  show.legend = FALSE ) +
    scale_color_manual(values=c(y="#286A99",n="#EE6B33"))+
    xlab("Clicks") +
    ylab("Avg Position") +
    scale_y_reverse()+
    scale_x_log10()+
    scale_size(range = c(2, 10)) +
    geom_text(size=2, alpha=1, color = "#40403E") 
q
  
  
q <-  ggplot(df_cbr_1, aes(x = csum, y = pmean, size=nkw, color = Client, label=Domain))  +
  geom_point(alpha = 1/3,  show.legend = FALSE ) +
  scale_color_manual(values=c(y="#286A99",n="#EE6B33"))+
  xlab("Clicks") +
  ylab("Avg Position") +
  scale_y_reverse()+
  scale_x_continuous()+
  scale_size(range = c(2, 10)) +
  geom_text(size=2, alpha=1, color = "#40403E") 
q

q <-  ggplot(df_cbr_1, aes(x = csum, y = pmean, size=nkw, color = Client, label=Domain))  +
  geom_point(alpha = 1/3,  show.legend = FALSE ) +
  scale_color_manual(values=c(y="#286A99",n="#EE6B33"))+
  xlab("Clicks") +
  ylab("Avg Position") +
  scale_y_reverse()+
  scale_x_continuous()+
  scale_size(range = c(2, 10)) +
  geom_text(size=2, alpha=1, color = "#40403E") 
q

