install.packages("http://cran.r-project.org/src/contrib/Archive/RCurl/RCurl_1.95-4.3.tar.gz", repos=NULL, type="source")
library(devtools)
library(SparkR)
Sys.setenv(SPARK_MEM="1g")
Sys.setenv(SPARK_HOME="/Users/paulreilly/spark-2.1.0-bin-hadoop2.7")
sc <- sparkR.init(master="local")




dfm <- inner_join(file_i, kwp, by = "Keyword" )
dfm$Keyword <- as.factor(dfm$Keyword)
dfm$Date <- as.Date(dfm$Date)

minDate <- min(dfm$Date)
maxDate <- max(dfm$Date)

dataPeriod <- maxDate - minDate

dfm$DayCount <- as.integer(dfm$Date - minDate) + 1
dfm$Month <- as.numeric(format(dfm$Date, "%m"))
dfm$Year <- as.numeric(format(dfm$Date, "%Y"))

dfm$Revenue <- ((dfm$Impressions / 30) * dfm$CTR)* dfm$Bid
dfm$Potential <- ((dfm$Impressions / 30) * 0.325)* dfm$Bid
dfm$Clicks <- ((dfm$Impressions / 30) * dfm$CTR)

# Sort Aggregations
DF_Group <- dfm %>% group_by(DayCount, Category, Country, Date, Domain, Keyword, Url, Month, Year) %>%
  arrange(DayCount) %>%
  summarise(pmean = mean(Position),
            rmean = mean(Revenue),
            cmean = mean(Clicks),
            bmean = mean(Bid),
            potmean = mean(Potential),
            imean = mean(Impressions))
print("finished initial group_by")

#30 day rolling mean

ma_7 <- 7
ma_30 <- 30
library(RcppRoll)


DF <- ungroup(DF_Group)

# Compute Moving Averages all wrong to date
by_site <- group_by(DF, Keyword, Domain, Url, Month, Category)
arrdf <- arrange(by_site, DayCount)

arrdf$pmeanSubbed = NaSub(arrdf$pmean, last_obs = TRUE, maxgap = 3)
arrdf$rmeanSubbed = NaSub(arrdf$rmean, last_obs = TRUE, maxgap = 3)
arrdf$cmeanSubbed = NaSub(arrdf$cmean, last_obs = TRUE, maxgap = 3)
arrdf$bmeanSubbed = NaSub(arrdf$bmean, last_obs = TRUE, maxgap = 3)
arrdf$imeanSubbed = NaSub(arrdf$imean, last_obs = TRUE, maxgap = 3)
arrdf$potmeanSubbed = NaSub(arrdf$potmean, last_obs = TRUE, maxgap = 3)

dtroll <- as.data.frame(ungroup(arrdf))


dtroll2 <- dtroll
dtroll2[is.na(dtroll2)] <- 0


kw_no_space <-str_replace_all(as.character(dtroll2[1,6]),"[^a-zA-Z0-9]", "_")


dtroll2 <-  ungroup(dtroll2)
#############################
smon <- dtroll2 %>% group_by(Month, Keyword, Domain, Category ) %>% arrange(Month, Keyword) %>%
  summarise(rsum = sum(rmeanSubbed),
            pmean = mean(pmeanSubbed),
            csum = sum(cmeanSubbed),
            bmean = mean(bmeanSubbed),
            NumberKeywords = n(),
            isum = sum(imeanSubbed),
            potmean = mean(potmeanSubbed))



print("Getting domain parts")
sdayfn <- as.character(paste(fullpath, "smon_", kw_no_space,opt$id,".RDS", sep = ""))


library(tldextract)
urllist <- as.character(smon$Domain)
# get most recent TLD listings
tld <- getTLD() # optionally pass in a different URL than the default
manyhosts <- c(urllist)
newurls <- tldextract(urllist, tldnames=tld)
smon$SLD <- as.factor(newurls$domain)
smon$Domain <- as.factor(paste(newurls$domain, newurls$tld, sep="."))


print("Saving smon ")
print("for client ID:")
print(opt$id)
print("in location:")
print(sdayfn)

saveRDS(smon, sdayfn)
#############################

#############################
dtroll2 <-  ungroup(dtroll2)
sdaykw <- dtroll2 %>% group_by(DayCount, Month,Category ,Keyword, Domain ) %>%
  arrange(DayCount, Month, Keyword) %>%
  summarise(rsum = sum(rmean),
            pmean = mean(pmean),
            csum = sum(cmean),
            bmean = mean(bmean),
            NumberKeywords = n(),
            isum = sum(imean),
            potmean = mean(potmean))

#Recreate Date Variable as.Date
sdaykw$Date <- as.Date(minDate + sdaykw$DayCount - 1)


urllist <- as.character(sdaykw$Domain)
# get most recent TLD listings
tld <- getTLD() # optionally pass in a different URL than the default
manyhosts <- c(urllist)
newurls <- tldextract(urllist, tldnames=tld)
sdaykw$SLD <- as.factor(newurls$domain)
sdaykw$Domain <- as.factor(paste(newurls$domain, newurls$tld, sep="."))


sdaykwfn <- as.character(paste(fullpath, "sdaykw_", kw_no_space,opt$id,".RDS", sep = ""))
print("Saving sdaykw")
print(opt$id)
saveRDS(sdaykw, sdaykwfn)
dtroll2 <-  ungroup(dtroll2)
#############################


sday2dom <- dtroll2 %>% group_by(DayCount, Month,Category ,Keyword, Domain ) %>%
  arrange(DayCount, Month, Keyword) %>%
  summarise(rsum = sum(rmean),
            pmean = mean(pmean),
            csum = sum(cmean),
            bmean = mean(bmean),
            NumberKeywords = n(),
            isum = sum(imean),
            potmean = mean(potmean))

sday2dom$Date <- as.Date(minDate + sday2dom$DayCount - 1)

sday2domfn <- as.character(paste(fullpath, "sday2dom_", kw_no_space,opt$id,".RDS", sep = ""))
print("Saving sday2dom")
print(opt$id)
print(sday2domfn)
saveRDS(sday2dom, sday2domfn)

print("Saving sday2dom ")
print("for client ID:")
print(opt$id)
print("in location:")
print(sday2domfn)

}



sink()

#
#
# #
# #
# #
#  sdaydomains <- rbind(readRDS("/home/paul/Data/RandD/AppData/2017-04-28-63/sday2dom_loans63.RDS"), readRDS("/home/paul/Data/RandD/AppData/2017-04-28-63/sday2dom_personal_loans63.RDS"))
#  sdaykw <- rbind(readRDS("/home/paul/Data/RandD/AppData/2017-04-28-63/sdaykw_loans63.RDS"), readRDS("/home/paul/Data/RandD/AppData/2017-04-28-63/sdaykw_personal_loans63.RDS"))
#  smon <- rbind(readRDS("/home/paul/Data/RandD/AppData/2017-04-28-63/smon_loans63.RDS"), readRDS("/home/paul/Data/RandD/AppData/2017-04-28-63/smon_personal_loans63.RDS"))
# #
# # #
#  sdaydomains$Keyword <- as.factor(sdaydomains$Keyword)
#  sdaydomains$Domain <- as.factor(sdaydomains$Domain)
#  sdaydomains$Category <- as.factor(sdaydomains$Category)
# #
# #
#  sdaykw$Keyword <- as.factor(sdaykw$Keyword)
#  sdaykw$Domain <- as.factor(sdaykw$Domain)
#  sdaykw$Category <- as.factor(sdaykw$Category)
# #
#   smon$Keyword <- as.factor(smon$Keyword)
#   smon$Domain <- as.factor(smon$Domain)
#   smon$Category <- as.factor(smon$Category)
#
#
#
#






# #
#
#   KWCluster_1<-
#
#     sdaykw %>%
#    group_by(Keyword) %>%
#    filter(pmean == 1, Month==4) %>% summarise(Revenue = sum(rsum), Impressions = sum(isum), Clicks = sum(csum)) %>% arrange(desc(Revenue))
#
#  top20_Rev <- smon %>%
#     group_by(Keyword, Domain, Category) %>%
#     filter(Category != "paydayloan", Month==2) %>%
#     summarise(Revenue = sum(rsum), Impressions = sum(isum), Clicks = sum(csum)) %>%
#     arrange(desc(Revenue)) %>%
#     top_n(10, Revenue)
#
#  uniquekw <- unique(top20_Rev$Keyword)
#
#  top20_Rev_Month <- smon %>%
#    group_by(Keyword, Domain, Category, Month) %>%
#    filter(Category != "paydayloan") %>%
#    summarise(Revenue = sum(rsum), Impressions = sum(isum), Clicks = sum(csum)) %>%
#    arrange(desc(Revenue)) %>%
#    top_n(10, Revenue)
#
#   levels(smon$Category)
# #
#   ggplot top20_Rev
#
#
# #saveRDS(kwptemp, "/home/paul/Data/RandD/AppData/kwpdata/1_kwp.RDS")
#
#
# #
# #
# # uniqueKWs <- as.factor(names(AtribLst(cluscards[cluscards==1], attrC="names", isNullC=NA)))
# #
# # subset <- unique(sdaykw[sdaykw$Keyword %in% uniqueKWs, ])
# #
# # cluster1 <- subset %>%
# #   group_by(Keyword) %>%
# #   filter(pmean <= 10) %>% summarise(Revenue = sum(rsum), Impressions = sum(isum), Clicks = sum(csum)) %>% arrange(desc(Revenue))
# #
# #



library(ggplot2)
library(grid)
library(RColorBrewer)
library(scales) 
library(foreach)
library(gridExtra)
library(zoo)
library(lubridate)
library(dplyr)
library(stringr)
library(extrafont)
library(dplyr)
library(stringr)
library(stats)
library(RcppRoll)

library(devtools)
install_github("shinyRGL", "trestletech")

#Remove Brand Affiliate Category

#Remove Brand Affiliate profile

#Remove Brands 

options(scipen=999)

font_import(pattern="PF*")
setwd("/home/ubuntu/Data/")


rawimport <- read.csv("~/Data/coral-2014-01-01-to-2016-05-03.csv", header=FALSE)



#Load keyword planner data 
KWP1 <- read.csv("~/iGB/K1.csv",header=TRUE, stringsAsFactors=FALSE, fileEncoding="latin1")
KWP2 <- read.csv("~/iGB/K2.csv",header=TRUE, stringsAsFactors=FALSE, fileEncoding="latin1")
KWP3 <- read.csv("~/iGB/K3.csv",header=TRUE, stringsAsFactors=FALSE, fileEncoding="latin1")
KWP4 <- read.csv("~/iGB/K4.csv",header=TRUE, stringsAsFactors=FALSE, fileEncoding="latin1")
KWP5 <- read.csv("~/iGB/K5.csv",header=TRUE, stringsAsFactors=FALSE, fileEncoding="latin1")
KWP <-  rbind(KWP1, KWP2, KWP3, KWP4, KWP5)
rm(KWP1)
rm(KWP2)
rm(KWP3)
rm(KWP4)
rm(KWP5)



raw <- rawimport
i = 809

names(raw) <- c("Date","Keyword", "Position", "Host")

#names(rawlatest) <- c("Date","Keyword", "Position", "Host")

df <- raw
ctrmodel <- read.csv("~/Intertops/ctrmodel.csv")




#append latest data
#df <- rbind(raw, rawlatest)


HCL <- read.csv("~/Data/HCL2.csv")

HCR <- filter(HCL, Brand == "N")


df$Date <- as.Date(df$Date, format = "%Y-%m-%d")



df$Keyword <- tolower(df$Keyword)
KWP$Keyword <- tolower(KWP$Keyword)
HCL$Keyword <- tolower(HCL$Keyword)
HCR$Keyword <- tolower(HCR$Keyword)


df$Keyword <- str_replace_all(df$Keyword, "[[:punct:]]", "")
KWP$Keyword <- str_replace_all(KWP$Keyword, "[[:punct:]]", "")
HCL$Keyword <- str_replace_all(HCL$Keyword, "[[:punct:]]", "")
HCR$Keyword <- str_replace_all(HCR$Keyword, "[[:punct:]]", "")

df <- unique(el)
#Fix catagorial variable for capitalisation


df$Keyword <- as.factor(df$Keyword)


#Convert Host to SLD
library(tldextract)
urllist <- as.character(el$Host)
# get most recent TLD listings
tld <- getTLD() # optionally pass in a different URL than the default
manyhosts <- c(urllist)
newurls <- tldextract(urllist, tldnames=tld)
df$SLD <- as.factor(newurls$domain)
df$Domain <- as.factor(paste(newurls$domain, newurls$tld, sep="."))



merged <- inner_join(x = KWP, y = HCL, by = "Keyword")
mergedR <- inner_join(x = KWP, y = HCR, by = "Keyword")

merged[is.na(merged)] <- 0
mergedR[is.na(mergedR)] <- 0

df_m <- inner_join(x = df, y = ctrmodel, by="Position")
df2 <- inner_join(x = df_m, y = merged, by="Keyword")
df2R <- inner_join(x = df_m, y = mergedR, by="Keyword")


df2$Rev <- ((df2$Avg..Monthly.Searches..exact.match.only. / 30) * df2$CTR)* df2$Suggested.bid 
df2R$Rev <- ((df2R$Avg..Monthly.Searches..exact.match.only. / 30) * df2R$CTR)* df2R$Suggested.bid 


#Sort out the Brand unto numeric for counting purposes.

df2$Brand <- as.character(df2$Brand)
df2$Br <- str_replace_all(df2$Brand, "Y", "1")
df2$Br <- str_replace_all(df2$Br, "N", "0")
df2$Br <- as.numeric(df2$Br)

summary(df2$Br)


df2R$Brand <- as.character(df2R$Brand)
df2R$Br <- str_replace_all(df2R$Brand, "Y", "1")
df2R$Br <- str_replace_all(df2R$Br, "N", "0")
df2R$Br <- as.numeric(df2R$Br)

summary(df2R$Br)

df1R <- df2R
df1 <- df2

#Drop Catagory Column
#df1 <- select(df2, -Category)



#Extract unique rows
df1 <- unique(df2)
df1$Keyword <- as.factor(df1$Keyword)
df1R$Keyword <- as.factor(df1R$Keyword)



#Remove Lotto from data set
df <- df1[df1$Product != "Lotto",]
dfR <- df1R[df1R$Product != "Lotto",]

maxDate <- max(df$Date)
minDate <- min(df$Date)


df$Day <- as.integer(df$Date - minDate) + 1
df$Month <- as.numeric(format(df$Date, "%m"))
df$Year <- as.numeric(format(df$Date, "%Y"))

saveRDS(df, "df.rds")

dfR$Day <- as.integer(dfR$Date - minDate) + 1
dfR$Month <- as.numeric(format(dfR$Date, "%m"))
dfR$Year <- as.numeric(format(dfR$Date, "%Y"))


#Calculate Elapsed Months Vector
elapsed_months <- function(end_date, start_date) {
  ed <- as.POSIXlt(end_date)
  sd <- as.POSIXlt(start_date)
  12 * (ed$year - sd$year) + (ed$mon - sd$mon)
}

sday3R$MonthCount <- elapsed_months(sday3R$Date, minDate)


sday3R$MonthCount <- as.factor(sday3R$MonthCount)

saveRDS(dfR, "dfR.rds")



sday <- df %>% group_by(Day, Keyword, Domain, Product) %>%
  summarise(rsum = sum(Rev), pmean = mean(Position), NumberKeywords = n(), bsum = sum(as.numeric(Br)))

sdayR <- dfR %>% group_by(Day, Keyword, Domain, Product) %>%
  summarise(rsum = sum(Rev), pmean = mean(Position), psd = sd(Position), NumberKeywords = n(), bsum = sum(as.numeric(Br)))

#Recreate Date Variable as.Date
sday$Date <- as.Date(minDate + sday$Day - 1)
sdayR$Date <- as.Date(minDate + sdayR$Day - 1)



sday2 <- sday %>% group_by(Day, Domain, Product) %>%
  summarise(rsum = sum(rsum), pmean = mean(pmean), NumberKeywords = n(), bsum = sum(as.numeric(bsum)))

sday2R <- sdayR %>% group_by(Day, Domain, Product) %>%
  summarise(rsum = sum(rsum), pmean = mean(pmean), NumberKeywords = n(), bsum = sum(as.numeric(bsum)))

#Recreate Date Variable as.Date
sday2$Date <- as.Date(minDate + sday2$Day - 1)
sday2R$Date <- as.Date(minDate + sday2R$Day - 1)





summary(sday)
summary(sday2)


saveRDS(sday, "sday.rds")
saveRDS(sday2, "sday2.rds")


summary(sdayR)
summary(sday2R)


saveRDS(sdayR, "sdayR.rds")
saveRDS(sday2R, "sday2R.rds")

sday2R <- readRDS("sday2R.rds")
sday2 <- readRDS("sday2.rds")

rm(raw)
rm(rawimport)
rm(rawlatest)
rm(df1)
rm(df2)
rm(df_m)
rm(merged)
rm(ctrmodel)
rm(HCL)
rm(KWP)
rm(newurls)
rm(rawimport)



ma_day <- 7
ms_sum <- 30




# Compute Moving Averages all wrong to date 
by_site <- group_by(sday2R, Product, Domain)
arrdf <- arrange(by_site, Product, Domain)

mutdf <- mutate(arrdf, ma_pmean = roll_meanr(x = pmean, ma_day, align = "right", fill = NA))
mutdf1 <- mutate(mutdf, ma_rsum = roll_meanr(x = rsum, ma_day, align = "right", fill = NA))
mutdf2 <- mutate(mutdf1, ma_NumberKeywords = roll_meanr(x = NumberKeywords, ma_day, align = "right", fill = NA))


mutdf3 <- mutate(mutdf2, ms_pmean = roll_sumr(x = pmean, ms_sum, align = "right", fill = NA))
mutdf4 <- mutate(mutdf3, ms_rsum = roll_sumr(x = rsum, ms_sum, align = "right", fill = NA))
mutdf5 <- mutate(mutdf4, ms_NumberKeywords = roll_sumr(x = NumberKeywords, ms_sum, align = "right", fill = NA))

#Assign to new DF
sday3R <- mutdf5
#Recreate Date Variable as.Date
sday3R$Date <- as.Date(minDate + sday3R$Day - 1)




#Remove Junk
rm(by_site)
rm(arrdf)
rm(mutdf)
rm(mutdf1)
rm(mutdf2)
rm(mutdf3)
rm(mutdf4)
rm(mutdf5)
#rm(df)  



sday3$Product <- as.factor(tolower(sday3$Product))
sday3R$Product <- as.factor(tolower(sday3R$Product))

maxDay <- max(sday3R$Day)



# Subset Starting Performance


#Convert Host to SLD
library(tldextract)
urllist <- as.character(sday3R$Domain)
# get most recent TLD listings
tld <- getTLD() # optionally pass in a different URL than the default
manyhosts <- c(urllist)
newurls <- tldextract(urllist, tldnames=tld)
sday3R$SLD <- as.factor(newurls$domain)


mxmsrs <- as.integer(max(sn$ms_rsum))
max_marsum <- as.integer(max(sday3R$ma_rsum)) 
max_msrsum <- as.integer(max(sday3R$ms_rsum)) 

min_marsum <- as.integer(min(sday3R$ma_rsum)) 
min_msrsum <- as.integer(min(sday3R$ms_rsum))

init_datum_casino <- sday3R %>% filter(Day==90) %>%  group_by(Product)















i = 120

foreach(i=120:maxDay) %do% {
  
  
  Quarter_datum <- sday3R %>% filter(Day == i-90 ) %>%  group_by(Product)
  
  #Convert Host to SLD
  library(tldextract)
  urllist <- as.character( Quarter_datum$Domain)
  # get most recent TLD listings
  tld <- getTLD() # optionally pass in a different URL than the default
  manyhosts <- c(urllist)
  newurls <- tldextract(urllist, tldnames=tld)
  Quarter_datum$SLD <- as.factor(newurls$domain)
  
  
  cbPalette <- c("#EE6B33", "#EE9D33", "#286A99", "#23A26F")
  
  date.x <- as.character(as.Date(minDate + i))
  text1 <- as.character(date.x)
  text2 <- as.character("7 Day Moving Average")
  title_text <- paste(text2, text1, sep=" - ") 
  sn <- sday3R %>% filter(Day == i, Product == "casino" ) %>%  group_by(Product) %>% arrange(desc(ma_rsum)) %>% head(20)
  sn[is.na(sn)] <- 0
  
  sn2 <- sday3R %>% filter(Day == i, Product == "betting" ) %>%  group_by(Product) %>% arrange(desc(ma_rsum)) %>% head(20)
  sn2[is.na(sn2)] <- 0
  
  sn$client <- ifelse(sn$Domain == "coral.co.uk", "y","n")
  sn2$client <- ifelse(sn2$Domain == "coral.co.uk", "y","n")
  
  
  q <-  ggplot(sn, aes(x = ma_rsum, y = ma_pmean, size=NumberKeywords, color = client, label=Domain))  + 
    geom_point(alpha = 1/3,  show.legend = FALSE ) +
    
    #  facet_wrap(~ Product) +
    scale_color_manual(values=c(y="#286A99",n="#EE6B33"))+
    ggtitle(title_text) +
    xlab("Traffic Value") +
    ylab("Avg Position") +
    scale_y_reverse(lim=c(21,1))+
    scale_x_continuous(limits= c(min_marsum, max_marsum))+
    #   scale_y_reverse()+
    scale_size(range = c(2, 10)) +
    geom_text(size=3, alpha=1, color = "#40403E") +
    msw
  
  q 
  
  
  q2 <-  ggplot(sn2, aes(x = ma_rsum, y = ma_pmean, size=NumberKeywords, color = client, label=Domain))  + 
    geom_point(alpha = 1/3,  show.legend = FALSE ) +
    
    #  facet_wrap(~ Product) +
    scale_color_manual(values=c(y="#286A99",n="#EE6B33"))+
    ggtitle(title_text) +
    xlab("Traffic Value") +
    ylab("Avg Position") +
    scale_y_reverse(lim=c(21,1))+
    scale_x_continuous(limits= c(min_marsum, max_marsum))+
    #   scale_y_reverse()+
    scale_size(range = c(2, 10)) +
    geom_text(size=3, alpha=1, color = "#40403E") +
    msw
  
  q2 
  
  
  
  p1 <- ggplot(sn, aes( x =  reorder(SLD, ma_rsum), y = ma_rsum, label=SLD, fill=client)) +
    geom_bar(stat = "identity", width=0.8, alpha = 1/3, show.legend = FALSE) + 
    ggtitle("Industry Top 10 by Traffic Value") +
    scale_fill_manual(values=c(y="#286A99",n="#EE6B33"))+
    xlab(" ") +
    ylab("Revenue 30 Days Rolling Sum") +
    scale_y_continuous(limits= c(min_marsum, max_marsum))+
    theme(axis.text.y  = element_text(angle=0), axis.text.x = element_text(angle = 0),
          strip.text.x = element_text(angle = 0)) + coord_flip() +
    msw
  
  p1
  
  
  
  
  
  #Prepare Data for Quartlery Diff Casino
  diffcalc <- left_join(sn,Quarter_datum, by = c("Domain","Product", "Date") )
  diffcalc[is.na(diffcalc)] <- 0
  diffcalc$ma_rsum_diff <- diffcalc$ma_rsum.x -diffcalc$ma_rsum.y
  
  
  
  urllist <- as.character(diffcalc$Domain)
  # get most recent TLD listings
  tld <- getTLD() # optionally pass in a different URL than the default
  manyhosts <- c(urllist)
  newurls <- tldextract(urllist, tldnames=tld)
  diffcalc$SLD <- as.factor(newurls$domain)
  
  diffcalc$color <- ifelse(  diffcalc$ma_rsum_diff < 0, "negative","positive")
  
  text2 <- as.character("Quarterly Difference (GBP)")
  
  
  QC <- ggplot(diffcalc, aes( x =  reorder(SLD, ma_rsum_diff), y = ma_rsum_diff  , label=SLD,  fill=client)) +
    geom_bar(stat = "identity", width=0.8, alpha = 1/3, show.legend = FALSE) + 
    scale_fill_manual(values=c(y="#286A99",n="#EE6B33"))+
    scale_y_continuous(limits= c(min_marsum, max_marsum))+
    theme(axis.text.y  = element_text(angle=0), axis.text.x = element_text(angle = 0),
          strip.text.x = element_text(angle = 0)) + 
    xlab(" ") +
    ylab("Revenue - YOY 90 Day Rolling Difference") +
    ggtitle(text2) + coord_flip() +
    msw
  
  QC
  
  
  
  
  
  #Prepare Data for Quartlery Diff Casino
  diffcalc <- left_join(sn2,Quarter_datum, by = c("Domain","Product", "Date") )
  diffcalc[is.na(diffcalc)] <- 0
  diffcalc$ma_rsum_diff <- diffcalc$ma_rsum.x -diffcalc$ma_rsum.y
  
  
  
  urllist <- as.character(diffcalc$Domain)
  # get most recent TLD listings
  tld <- getTLD() # optionally pass in a different URL than the default
  manyhosts <- c(urllist)
  newurls <- tldextract(urllist, tldnames=tld)
  diffcalc$SLD <- as.factor(newurls$domain)
  
  diffcalc$color <- ifelse(  diffcalc$ma_rsum_diff < 0, "negative","positive")
  
  text2 <- as.character("Quarterly Difference (GBP)")
  
  
  QS <- ggplot(diffcalc2, aes( x =  reorder(SLD, ma_rsum_diff), y = ma_rsum_diff , label=SLD,  fill=client)) +
    geom_bar(stat = "identity", width=0.8, alpha = 1/3, show.legend = FALSE) + 
    scale_fill_manual(values=c(y="#286A99",n="#EE6B33"))+
    scale_y_continuous(limits= c(min_marsum, max_marsum))+
    #    facet_wrap(~Product, scales = "free_x") + 
    theme(axis.text.y  = element_text(angle=0), axis.text.x = element_text(angle = 0),
          strip.text.x = element_text(angle = 0)) + 
    xlab(" ") +
    ylab("Revenue - YOY 90 Day Rolling Difference") +
    ggtitle(text2) + coord_flip() + 
    msw
  
  QS
  
  require("gridExtra")
  
  grid.arrange(q, QS, q2, QC, ncol=2)
  
  
  require(grid)
  require(gridExtra)
  
  grid.draw(plot) # interactive device
  
  plot <-  grid.arrange(q, QS, q2, QC, layout_matrix = rbind(c(1,2),c(3,4)))
  
  grid.draw(plot) # interactive device
  
  
  j <- i - 120
  ggsave(sprintf("igb1%03d.png",j), plot = plot, width=12.5, height=10, units="in", dpi = 300)
}



#Set Comparison Date 






# Most Recent Available Record 30 Day Moving Sum Rev



# Subset Two Years Later 30 Day Moving Sum Rev

# Subset Three Years Later 30 Day Moving Sum Rev


# Create Theme and Update Default

# Subset SLD Coral & Poker & Moving Average By unique(Keywords) 

# Subset Starting Performance
msw <- theme(
  line = element_line(colour = "#DCDAD4", size = 0.5, linetype = 1, lineend = "butt"), 
  rect = element_rect(fill = "#E7E7E2", colour = "#DCDAD4", size = 0.5, linetype = 1), 
  text = element_text(family = "PF DinText Pro Light", colour = "#40403E", size = 10, hjust = 0.5, vjust = 0.5, angle = 0, lineheight = 0.9), 
  axis.text = element_text(size = 10),
  strip.text = element_text(size = 10), 
  axis.line = element_blank(), 
  axis.text.x = element_text(vjust = 1), 
  axis.text.y = element_text(hjust = 1), 
  axis.title.x = element_text(), 
  axis.title.y = element_text(angle = 90), 
  
  legend.background = element_rect(colour = "#E7E7E2", fill = "#E7E7E2"), 
  legend.key = element_rect(fill = "#E7E7E2", colour = "#E7E7E2"), 
  legend.key.size = unit(.9, "lines"), 
  legend.key.height = NULL, 
  legend.key.width = NULL, 
  legend.text = element_text(size = 10), 
  legend.text.align = NULL, 
  legend.title = element_text(size = 10, hjust = 0), 
  legend.title.align = NULL, 
  legend.position = "bottom", 
  legend.direction = NULL, 
  legend.justification = "center", 
  legend.box = NULL, 
  
  panel.background = element_rect(fill = "#E7E7E2", colour = "#E7E7E2"), 
  panel.border = element_blank(), 
  panel.grid.major = element_line(colour = "#DCDAD4", size = 0.25),
  panel.grid.minor = element_line(colour = "#E7E7E2", size = 0.25), 
  panel.margin = unit(1, "lines"), 
  panel.margin.x = unit(1, "lines"), 
  panel.margin.y = unit(1, "lines"), 
  axis.ticks = element_line(colour = "#DCDAD4"), 
  strip.background = element_rect(fill = "#EE6B33", colour = "#EE6B33"), 
  strip.text.x = element_text(angle = 0, colour = "#FFFFFF"), 
  strip.text.y = element_text(angle = -90, colour = "#FFFFFF"), 
  plot.background = element_rect(colour = "#DCDAD4"), 
  plot.title = element_text(size = 14, color = "#40403E", hjust = .5))



library(ggplot2)
library(grid)
library(RColorBrewer)
library(scales) 
library(foreach)
library(gridExtra)
library(zoo)
library(dplyr)
library(stringr)
library(extrafont)
library(stats)

Nov <- read.csv("~/Data/RandD/UK/Nov.csv")
Dec <- read.csv("~/Data/RandD/UK/Dec.csv")
Jan <- read.csv("~/Data/RandD/UK/Jan.csv")
Feb <- read.csv("~/Data/RandD/UK/Feb.csv")
Mar <- read.csv("~/Data/RandD/UK/Mar.csv")

df <- rbind(Nov,Dec,Jan)
setwd("/home/paul/Data/RandD/UK")
library(tidyr)

melt_df <- melt(df0, id = "Keyword")
library(RColorBrewer)

ggplot(data_wide, aes(x = Day)) + 
  scale_color_brewer(palette = "paired")
geom_line(aes(y = `3d roulette`, colour="#8DD3C7")) +                 #
  geom_line(aes(y = `buzzluck casino`, colour = "#FFFFB3")) +           #
  geom_line(aes(y = `casinos slots`, colour = "#BEBADA")) +             #
  geom_line(aes(y = `crown gems slot game`, colour = "#FB8072")) +      #
  geom_line(aes(y = `crown gems slots`, colour = "")) +          #
  geom_line(aes(y = `crown jewels slot machine`, colour = 6)) + #
  geom_line(aes(y = `drake casino`, colour = 7)) +              #
  geom_line(aes(y = `3d roulette`, colour=1)) +                 #
  geom_line(aes(y = `buzzluck casino`, colour = 2)) +           #
  geom_line(aes(y = `casinos slots`, colour = 3)) +             #
  geom_line(aes(y = `crown gems slot game`, colour = 4)) +      #
  geom_line(aes(y = `crown gems slots`, colour = 5)) +          #
  ylab(label="Number of new members") + 
  xlab("Week")

dlong <- df0 %>% select(Day, Keyword, pmean)

data_wide <- spread(dlong, Keyword, pmean)


dftest <- data.frame(x = c("a", "b"), y = c(3, 4), z = c(5, 6))
dftest %>% spread(x, y) 
tidy <- data_wide %>% gather(Keyword, pmean, 7:64, na.rm = TRUE)

rm(df)

KWP <- readRDS("~/Data/RandD/UK/KWPS.rds")

df <- df %>% select(1,3,4,5)

names(df0) <- c("Day","Keyword", "Pmean", "CTRmean", "Date")

df$Keyword <- as.factor(tolower(str_replace_all(df$Keyword, "[[:punct:]]", "")))
HCL$Keyword <- as.factor(tolower(str_replace_all(HCL$Keyword, "[[:punct:]]", "")))
KWP$Keyword <- as.factor(tolower(str_replace_all(KWP$Keyword, "[[:punct:]]", "")))


df$Date <- as.Date(df$Date, format = "%Y-%m-%d")


ggplot(df, aes(df$Date)) +
  geom_histogram(stat="count")

options(scipen=999)

font_import(pattern="PF*")

df_c <- df_j[df_j$Product == "Casino", ]

df0j <- inner_join(df0, KWP, by = "Keyword") 
df <- droplevels(df)

df$Keyword <- as.factor(df$Keyword)



dbest <- df_c[df_c$Domain == "dbestcasino.com",]


df0 <- ungroup(df0)


mutdf <- df0 %>% group_by(Keyword, Day) %>%
  mutate(mapos7 = roll_meanr(x = pmean, 7, align = "right", fill = NA))



mutdf2 <- mutdf  %>% group_by(Keyword, Day) %>%
  mutate(mz = RollingZscore(Position, 7, expanding = FALSE, na_method = "none",pop = FALSE))


df0 <- ungroup(df0)
mutdf <- df0 %>% group_by(Keyword) %>%
  arrange(Keyword, Day) %>%
  mutate(maPos_7 = roll_meanr(x = pmean, 7 , align = "right", fill = NA))

mutdf2 <- mutdf %>% group_by(Keyword) %>%
  arrange(Keyword, Day) %>%
  mutate(maRev_7 = roll_meanr(x = rsum, 7 , align = "right", fill = NA))



df2 <- df[df$Product == "Casino",]

df0 <- dt %>% group_by(Day, as.factor(Keyword)) %>% 
  summarise(pmean = mean(Position), ctrmean = mean(CTR))



library(tldextract)
urllist <- as.character(df_c$Host)
# get most recent TLD listings
tld <- getTLD() # optionally pass in a different URL than the default
manyhosts <- c(urllist)
newurls <- tldextract(urllist, tldnames=tld)
df_c$Domain <- as.factor(paste(newurls$domain, newurls$tld, sep = "."))
df_c$TLD <- as.factor(newurls$tld)


dt <- inner_join(dbest, ctrmodel, by = "Position")

dt <- df0j
dt$Keyword <- as.factor(dt$Keyword)

dt$Revenue <- ((dt$Avg..Monthly.Searches..exact.match.only.) * dt$CTRmean) * dt$Suggested.bid
dt$Potential <- ((dt$Avg..Monthly.Searches..exact.match.only.) * 0.33) * dt$Suggested.bid


maxDay <- max(dt$Day)
maxPos <- max(df0$pmean)


dt <- inner_join(df2, ctrmodel, by = "Position")


df2$Domain <- as.factor(df2$Domain)

df0 <- dt %>% group_by(Day, Keyword) %>%
  summarise(pmean = mean(Pmean), rsum = sum(Revenue),
            potsum = sum(Potential))


#Recreate Date from Day and Datum
data_wide$Date <- as.Date(minDate + data_wide$Day - 1)

library(foreach)


df0 <- mutdf2


i <- 8

foreach(i=7:maxDay) %do% {
  
  date.x <- as.character(as.Date(minDate + i))
  
  text1 <- as.character(date.x)
  text2 <- as.character("7 Day Moving Average - DBestCasino")
  title_text <- paste(text2, text1, sep=" - ") 
  
  sn <- df0 %>% dplyr::filter(Day == i)
  
  maxrsum <- max(df0$rsum)
  
  
  plot <- ggplot(sn, aes(maPos_7, maRev_7, size = potsum,  label = Keyword, colour = potsum)) +
    geom_point(alpha = 1/2) +
    scale_size_continuous(range = c(2,6)) +
    coord_flip() +
    scale_fill_brewer() +
    scale_x_reverse(lim=c(maxPos,1))+
    scale_y_continuous(limits= c(0, maxrsum))+
    geom_text(size=3, alpha=1, color = "#40403E")+
    ggtitle(title_text) +
    xlab("Average Position") +
    ylab("Monthly Adwords Value from SEO (log)") +
    theme(axis.text.x = element_text(angle = 30, hjust = 1))
  plot
  j <- i - 8
  
  ggsave(sprintf("dbest%03d.png",j), plot = plot, width=12.5, height=10, units="in", dpi = 350)
  
}


require("gridExtra")

grid.arrange(q, QS, q2, QC, ncol=2)


require(grid)
require(gridExtra)

grid.draw(plot) # interactive device







ggplot(df, aes(TF, DA, colour = NICE)) +
  facet_wrap( ~ Niche) +
  geom_point() 

df$Keyword <- as.factor(tolower(df$Keyword))
jdf$Keyword <- as.factor(tolower(jdf$Keyword))


HCL$Keyword <- tolower(HCL$Keyword)
HCR$Keyword <- tolower(HCR$Keyword)

x1$Keyword <- str_replace_all(x1$Keyword, "\\[|\\]", "")

x1$Keyword <- as.factor(tolower(x1$Keyword))
df$Keyword <- str_replace_all(df$Keyword, "[[:punct:]]", "")
KWP$Keyword <- str_replace_all(KWP$Keyword, "[[:punct:]]", "")
HCL$Keyword <- str_replace_all(HCL$Keyword, "[[:punct:]]", "")
HCR$Keyword <- str_replace_all(HCR$Keyword, "[[:punct:]]", "")

df <- unique(el)
#Fix catagorial variable for capitalisation


kws <- data.frame(unique(df$Keyword))

df_j <- inner_join(df, HCL, by = "Keyword")

df_j$Keyword <- as.factor(df_j$Keyword)

df2[is.na(df2)] <- 0
mergedR[is.na(mergedR)] <- 0

df_m <- inner_join(x = j, y = ctrmodel, by="Position")
df0 <- inner_join(x = df0, y = KWP, by="Keyword")

df <- inner_join(df_m, df2, by= "Keyword")

df2$Keyword <- as.factor(df2$Keyword)
df2 <- droplevels(df2)

df_keywords <- data.frame(unique(df2$Keyword))

df$Revenue <- ((df$Avg..Monthly.Searches..exact.match.only.) * df$CTR) * df$Suggested.bid
df2$Potential <- ((df2$Avg..Monthly.Searches..exact.match.only.) * 0.33) * df2$Suggested.bid



cc_10 <- sn_top[sn_top$Category == "creditcard",]
ln_10 <- sn_top[sn_top$Category == "loans",]
pdl_10 <- sn_top[sn_top$Category == "paydayloan",]

#Bubbles
ggplot(cc_10, aes(pmean, rsum, size = NumberKeywords,  label = SLD, colour = Category)) +
  geom_point(alpha = 1/2) +
  coord_flip() +
  scale_y_log10(
    breaks = scales::trans_breaks("log10", function(x) 10^x),
    labels = scales::trans_format("log10", scales::math_format(10^.x))) +
  scale_x_reverse() +
  geom_text(size=2, alpha=1, color = "#40403E") +
  ggtitle("Relative Performance Summary - U.K. Top 20 - Credit Cards") +
  xlab("Average Position") +
  ylab("Monthly Adwords Value from SEO (log)")


ggplot(ln_10, aes(pmean, rsum, size = NumberKeywords,  label = SLD, colour = Category)) +
  geom_point(alpha = 1/2) +
  coord_flip() +
  scale_y_log10(
    breaks = scales::trans_breaks("log10", function(x) 10^x),
    labels = scales::trans_format("log10", scales::math_format(10^.x))) +
  scale_x_reverse() +
  geom_text(size=2, alpha=1, color = "#40403E") +
  ggtitle("Relative Performance Summary - U.K. Top 20 - Loans") +
  xlab("Average Position") +
  ylab("Monthly Adwords Value from SEO (log)")


ggplot(pdl_10, aes(pmean, rsum, size = NumberKeywords,  label = SLD, colour = Category)) +
  geom_point(alpha = 1/2) +
  coord_flip() +
  scale_y_log10(
    breaks = scales::trans_breaks("log10", function(x) 10^x),
    labels = scales::trans_format("log10", scales::math_format(10^.x))) +
  scale_x_reverse() +
  geom_text(size=2, alpha=1, color = "#40403E") +
  ggtitle("Relative Performance Summary - U.K. Top 20 - Payday Loans") +
  xlab("Average Position") +
  ylab("Monthly Adwords Value from SEO (log)")


# Bars


ggplot(pdl, aes(factor(SLD))) + geom_bar(stat = "identity")

# By default, uses stat="bin", which gives the count in each category
reorder_size <- function(x) {
  factor(x, levels = names(sort(table(x))))
}


ggplot(sn_top, aes(x = reorder(SLD, rsum), y = rsum, fill = Category, label = SLD)) +
  geom_bar(stat = "identity" ) +
  facet_wrap( ~ Category,  nrow = 2) + 
  coord_flip() +
  ggtitle("Relative Performance Summary - U.K. Top 10 by Product") +
  xlab("Domain") +
  ylab("Monthly Adwords Value from SEO (log)")  +
  theme(axis.text.x = element_text(angle = 30, hjust = 1))






sday <- df2 %>% group_by(Domain, Category, SLD) %>%
  summarise(rsum = sum(Revenue), rsdev = sd(Revenue), pmean = mean(Position), pstdev = sd(Position), NumberKeywords = n())



sday <- df2 %>% group_by(Category, SLD) %>%
  summarise(rsum = sum(Revenue), rsdev = sd(Revenue), pmean = mean(Position), pstdev = sd(Position), NumberKeywords = n())




sn_top <- sday %>%  group_by(Category) %>% arrange(desc(rsum)) %>% top_n(9, rsum)
sn_top10 <- sday %>%  group_by(Category) %>% arrange(desc(rsum)) %>% top_n(10, rsum)



avec <- sn_top[["SLD"]] 

sday2 <- sday %>% group_by(Day, Domain, Product) %>%
  summarise(rsum = sum(rsum), pmean = mean(pmean), NumberKeywords = n(), bsum = sum(as.numeric(bsum)))

sday2R <- sdayR %>% group_by(Day, Domain, Product) %>%
  summarise(rsum = sum(rsum), pmean = mean(pmean), NumberKeywords = n(), bsum = sum(as.numeric(bsum)))



top9 <- dplyr::filter(df2, df2[,"SLD"] %in% avec)


cc_9 <- top9[top9$Category == "creditcard",]
ln_9 <- top9[top9$Category == "loans",]
pdl_9 <- top9[top9$Category == "paydayloan",]




ggplot(cc_9, aes(Position, Revenue, size = Potential,  label = Keyword, colour = SLD, group = SLD)) +
  geom_point(alpha = 1/2) +
  facet_wrap(~ SLD) +
  coord_flip() +
  scale_y_log10()  + 
  scale_x_reverse() +
  ggtitle("Credit Cards - Top Brands - Keyword Level Report") +
  xlab("Average Position") +
  ylab("Monthly Adwords Value from SEO (log)") +
  theme(axis.text.x = element_text(angle = 30, hjust = 1)) 




ggsave("Loans-KW-top_n.pdf", plot = last_plot(), width=11, height=8.5)



ggplot(mutdf, aes(Position, Revenue, size = Potential,  label = Keyword, colour = SLD)) +
  geom_point(alpha = 1/2) +
  coord_flip() +
  scale_y_log10()  + 
  scale_x_reverse() +
  ggtitle("Loans - Top Brands - Keyword Level Report") +
  xlab("Average Position") +
  ylab("Monthly Adwords Value from SEO (log)") +
  theme(axis.text.x = element_text(angle = 30, hjust = 1))
ggsave("Loans-KW-top_n.pdf", plot = last_plot(), width=11, height=8.5)


ggplot(pdl_9, aes(Position, Revenue, size = Potential,  label = Keyword, colour = SLD)) +
  geom_point(alpha = 1/2) +
  facet_wrap(~ SLD) +
  coord_flip() +
  scale_y_log10()  + 
  scale_x_reverse() +
  ggtitle("Payday Loans - Top Brands - Keyword Level Report") +
  xlab("Average Position") +
  ylab("Monthly Adwords Value from SEO (log)") +
  theme(axis.text.x = element_text(angle = 30, hjust = 1))
ggsave("PayDayLoans-KW-top_n.pdf", plot = last_plot(), width=11, height=8.5)



top_df <- inner_join(top9, datagrabber, by = "SLD")


library(ggplot2)
library(grid)
library(RColorBrewer)
library(scales) 
library(foreach)
library(gridExtra)
library(zoo)
library(dplyr)
library(stringr)
library(extrafont)
library(stats)

Nov <- read.csv("~/Data/RandD/UK/Nov.csv")
Dec <- read.csv("~/Data/RandD/UK/Dec.csv")
Jan <- read.csv("~/Data/RandD/UK/Jan.csv")
Feb <- read.csv("~/Data/RandD/UK/Feb.csv")
Mar <- read.csv("~/Data/RandD/UK/Mar.csv")

df <- rbind(Nov,Dec,Jan)
setwd("/home/paul/Data/RandD/UK")
library(tidyr)

melt_df <- melt(df0, id = "Keyword")
library(RColorBrewer)

ggplot(data_wide, aes(x = Day)) + 
  scale_color_brewer(palette = "paired")
geom_line(aes(y = `3d roulette`, colour="#8DD3C7")) +                 #
  geom_line(aes(y = `buzzluck casino`, colour = "#FFFFB3")) +           #
  geom_line(aes(y = `casinos slots`, colour = "#BEBADA")) +             #
  geom_line(aes(y = `crown gems slot game`, colour = "#FB8072")) +      #
  geom_line(aes(y = `crown gems slots`, colour = "")) +          #
  geom_line(aes(y = `crown jewels slot machine`, colour = 6)) + #
  geom_line(aes(y = `drake casino`, colour = 7)) +              #
  geom_line(aes(y = `3d roulette`, colour=1)) +                 #
  geom_line(aes(y = `buzzluck casino`, colour = 2)) +           #
  geom_line(aes(y = `casinos slots`, colour = 3)) +             #
  geom_line(aes(y = `crown gems slot game`, colour = 4)) +      #
  geom_line(aes(y = `crown gems slots`, colour = 5)) +          #
  ylab(label="Number of new members") + 
  xlab("Week")

dlong <- df0 %>% select(Day, Keyword, pmean)

data_wide <- spread(dlong, Keyword, pmean)


dftest <- data.frame(x = c("a", "b"), y = c(3, 4), z = c(5, 6))
dftest %>% spread(x, y) 
tidy <- data_wide %>% gather(Keyword, pmean, 7:64, na.rm = TRUE)

rm(df)

KWP <- readRDS("~/Data/RandD/UK/KWPS.rds")

df <- df %>% select(1,3,4,5)

names(df0) <- c("Day","Keyword", "Pmean", "CTRmean", "Date")

df$Keyword <- as.factor(tolower(str_replace_all(df$Keyword, "[[:punct:]]", "")))
HCL$Keyword <- as.factor(tolower(str_replace_all(HCL$Keyword, "[[:punct:]]", "")))
KWP$Keyword <- as.factor(tolower(str_replace_all(KWP$Keyword, "[[:punct:]]", "")))


df$Date <- as.Date(df$Date, format = "%Y-%m-%d")


ggplot(df, aes(df$Date)) +
  geom_histogram(stat="count")

options(scipen=999)

font_import(pattern="PF*")

df_c <- df_j[df_j$Product == "Casino", ]

df0j <- inner_join(df0, KWP, by = "Keyword") 
df <- droplevels(df)

df$Keyword <- as.factor(df$Keyword)



dbest <- df_c[df_c$Domain == "dbestcasino.com",]


df0 <- ungroup(df0)


mutdf <- df0 %>% group_by(Keyword, Day) %>%
  mutate(mapos7 = roll_meanr(x = pmean, 7, align = "right", fill = NA))



mutdf2 <- mutdf  %>% group_by(Keyword, Day) %>%
  mutate(mz = RollingZscore(Position, 7, expanding = FALSE, na_method = "none",pop = FALSE))


df0 <- ungroup(df0)
mutdf <- df0 %>% group_by(Keyword) %>%
  arrange(Keyword, Day) %>%
  mutate(maPos_7 = roll_meanr(x = pmean, 7 , align = "right", fill = NA))

mutdf2 <- mutdf %>% group_by(Keyword) %>%
  arrange(Keyword, Day) %>%
  mutate(maRev_7 = roll_meanr(x = rsum, 7 , align = "right", fill = NA))



df2 <- df[df$Product == "Casino",]

df0 <- dt %>% group_by(Day, as.factor(Keyword)) %>% 
  summarise(pmean = mean(Position), ctrmean = mean(CTR))



library(tldextract)
urllist <- as.character(df_c$Host)
# get most recent TLD listings
tld <- getTLD() # optionally pass in a different URL than the default
manyhosts <- c(urllist)
newurls <- tldextract(urllist, tldnames=tld)
df_c$Domain <- as.factor(paste(newurls$domain, newurls$tld, sep = "."))
df_c$TLD <- as.factor(newurls$tld)


dt <- inner_join(dbest, ctrmodel, by = "Position")

dt <- df0j
dt$Keyword <- as.factor(dt$Keyword)

dt$Revenue <- ((dt$Avg..Monthly.Searches..exact.match.only.) * dt$CTRmean) * dt$Suggested.bid
dt$Potential <- ((dt$Avg..Monthly.Searches..exact.match.only.) * 0.33) * dt$Suggested.bid


maxDay <- max(dt$Day)
maxPos <- max(df0$pmean)


dt <- inner_join(df2, ctrmodel, by = "Position")


df2$Domain <- as.factor(df2$Domain)

df0 <- dt %>% group_by(Day, Keyword) %>%
  summarise(pmean = mean(Pmean), rsum = sum(Revenue),
            potsum = sum(Potential))


#Recreate Date from Day and Datum
data_wide$Date <- as.Date(minDate + data_wide$Day - 1)

library(foreach)


df0 <- mutdf2


i <- 8

foreach(i=7:maxDay) %do% {
  
  date.x <- as.character(as.Date(minDate + i))
  
  text1 <- as.character(date.x)
  text2 <- as.character("7 Day Moving Average - DBestCasino")
  title_text <- paste(text2, text1, sep=" - ") 
  
  sn <- df0 %>% dplyr::filter(Day == i)
  
  maxrsum <- max(df0$rsum)
  
  
  plot <- ggplot(sn, aes(maPos_7, maRev_7, size = potsum,  label = Keyword, colour = potsum)) +
    geom_point(alpha = 1/2) +
    scale_size_continuous(range = c(2,6)) +
    coord_flip() +
    scale_fill_brewer() +
    scale_x_reverse(lim=c(maxPos,1))+
    scale_y_continuous(limits= c(0, maxrsum))+
    geom_text(size=3, alpha=1, color = "#40403E")+
    ggtitle(title_text) +
    xlab("Average Position") +
    ylab("Monthly Adwords Value from SEO (log)") +
    theme(axis.text.x = element_text(angle = 30, hjust = 1))
  plot
  j <- i - 8
  
  ggsave(sprintf("dbest%03d.png",j), plot = plot, width=12.5, height=10, units="in", dpi = 350)
  
}


require("gridExtra")

grid.arrange(q, QS, q2, QC, ncol=2)


require(grid)
require(gridExtra)

grid.draw(plot) # interactive device







ggplot(df, aes(TF, DA, colour = NICE)) +
  facet_wrap( ~ Niche) +
  geom_point() 

df$Keyword <- as.factor(tolower(df$Keyword))
jdf$Keyword <- as.factor(tolower(jdf$Keyword))


HCL$Keyword <- tolower(HCL$Keyword)
HCR$Keyword <- tolower(HCR$Keyword)

x1$Keyword <- str_replace_all(x1$Keyword, "\\[|\\]", "")

x1$Keyword <- as.factor(tolower(x1$Keyword))
df$Keyword <- str_replace_all(df$Keyword, "[[:punct:]]", "")
KWP$Keyword <- str_replace_all(KWP$Keyword, "[[:punct:]]", "")
HCL$Keyword <- str_replace_all(HCL$Keyword, "[[:punct:]]", "")
HCR$Keyword <- str_replace_all(HCR$Keyword, "[[:punct:]]", "")

df <- unique(el)
#Fix catagorial variable for capitalisation


kws <- data.frame(unique(df$Keyword))

df_j <- inner_join(df, HCL, by = "Keyword")

df_j$Keyword <- as.factor(df_j$Keyword)

df2[is.na(df2)] <- 0
mergedR[is.na(mergedR)] <- 0

df_m <- inner_join(x = j, y = ctrmodel, by="Position")
df0 <- inner_join(x = df0, y = KWP, by="Keyword")

df <- inner_join(df_m, df2, by= "Keyword")

df2$Keyword <- as.factor(df2$Keyword)
df2 <- droplevels(df2)

df_keywords <- data.frame(unique(df2$Keyword))

df$Revenue <- ((df$Avg..Monthly.Searches..exact.match.only.) * df$CTR) * df$Suggested.bid
df2$Potential <- ((df2$Avg..Monthly.Searches..exact.match.only.) * 0.33) * df2$Suggested.bid



cc_10 <- sn_top[sn_top$Category == "creditcard",]
ln_10 <- sn_top[sn_top$Category == "loans",]
pdl_10 <- sn_top[sn_top$Category == "paydayloan",]

#Bubbles
ggplot(cc_10, aes(pmean, rsum, size = NumberKeywords,  label = SLD, colour = Category)) +
  geom_point(alpha = 1/2) +
  coord_flip() +
  scale_y_log10(
    breaks = scales::trans_breaks("log10", function(x) 10^x),
    labels = scales::trans_format("log10", scales::math_format(10^.x))) +
  scale_x_reverse() +
  geom_text(size=2, alpha=1, color = "#40403E") +
  ggtitle("Relative Performance Summary - U.K. Top 20 - Credit Cards") +
  xlab("Average Position") +
  ylab("Monthly Adwords Value from SEO (log)")


ggplot(ln_10, aes(pmean, rsum, size = NumberKeywords,  label = SLD, colour = Category)) +
  geom_point(alpha = 1/2) +
  coord_flip() +
  scale_y_log10(
    breaks = scales::trans_breaks("log10", function(x) 10^x),
    labels = scales::trans_format("log10", scales::math_format(10^.x))) +
  scale_x_reverse() +
  geom_text(size=2, alpha=1, color = "#40403E") +
  ggtitle("Relative Performance Summary - U.K. Top 20 - Loans") +
  xlab("Average Position") +
  ylab("Monthly Adwords Value from SEO (log)")


ggplot(pdl_10, aes(pmean, rsum, size = NumberKeywords,  label = SLD, colour = Category)) +
  geom_point(alpha = 1/2) +
  coord_flip() +
  scale_y_log10(
    breaks = scales::trans_breaks("log10", function(x) 10^x),
    labels = scales::trans_format("log10", scales::math_format(10^.x))) +
  scale_x_reverse() +
  geom_text(size=2, alpha=1, color = "#40403E") +
  ggtitle("Relative Performance Summary - U.K. Top 20 - Payday Loans") +
  xlab("Average Position") +
  ylab("Monthly Adwords Value from SEO (log)")


# Bars


ggplot(pdl, aes(factor(SLD))) + geom_bar(stat = "identity")

# By default, uses stat="bin", which gives the count in each category
reorder_size <- function(x) {
  factor(x, levels = names(sort(table(x))))
}


ggplot(sn_top, aes(x = reorder(SLD, rsum), y = rsum, fill = Category, label = SLD)) +
  geom_bar(stat = "identity" ) +
  facet_wrap( ~ Category,  nrow = 2) + 
  coord_flip() +
  ggtitle("Relative Performance Summary - U.K. Top 10 by Product") +
  xlab("Domain") +
  ylab("Monthly Adwords Value from SEO (log)")  +
  theme(axis.text.x = element_text(angle = 30, hjust = 1))






sday <- df2 %>% group_by(Domain, Category, SLD) %>%
  summarise(rsum = sum(Revenue), rsdev = sd(Revenue), pmean = mean(Position), pstdev = sd(Position), NumberKeywords = n())



sday <- df2 %>% group_by(Category, SLD) %>%
  summarise(rsum = sum(Revenue), rsdev = sd(Revenue), pmean = mean(Position), pstdev = sd(Position), NumberKeywords = n())




sn_top <- sday %>%  group_by(Category) %>% arrange(desc(rsum)) %>% top_n(9, rsum)
sn_top10 <- sday %>%  group_by(Category) %>% arrange(desc(rsum)) %>% top_n(10, rsum)



avec <- sn_top[["SLD"]] 

sday2 <- sday %>% group_by(Day, Domain, Product) %>%
  summarise(rsum = sum(rsum), pmean = mean(pmean), NumberKeywords = n(), bsum = sum(as.numeric(bsum)))

sday2R <- sdayR %>% group_by(Day, Domain, Product) %>%
  summarise(rsum = sum(rsum), pmean = mean(pmean), NumberKeywords = n(), bsum = sum(as.numeric(bsum)))



top9 <- dplyr::filter(df2, df2[,"SLD"] %in% avec)


cc_9 <- top9[top9$Category == "creditcard",]
ln_9 <- top9[top9$Category == "loans",]
pdl_9 <- top9[top9$Category == "paydayloan",]




ggplot(cc_9, aes(Position, Revenue, size = Potential,  label = Keyword, colour = SLD, group = SLD)) +
  geom_point(alpha = 1/2) +
  facet_wrap(~ SLD) +
  coord_flip() +
  scale_y_log10()  + 
  scale_x_reverse() +
  ggtitle("Credit Cards - Top Brands - Keyword Level Report") +
  xlab("Average Position") +
  ylab("Monthly Adwords Value from SEO (log)") +
  theme(axis.text.x = element_text(angle = 30, hjust = 1)) 




ggsave("Loans-KW-top_n.pdf", plot = last_plot(), width=11, height=8.5)



ggplot(mutdf, aes(Position, Revenue, size = Potential,  label = Keyword, colour = SLD)) +
  geom_point(alpha = 1/2) +
  coord_flip() +
  scale_y_log10()  + 
  scale_x_reverse() +
  ggtitle("Loans - Top Brands - Keyword Level Report") +
  xlab("Average Position") +
  ylab("Monthly Adwords Value from SEO (log)") +
  theme(axis.text.x = element_text(angle = 30, hjust = 1))
ggsave("Loans-KW-top_n.pdf", plot = last_plot(), width=11, height=8.5)


ggplot(pdl_9, aes(Position, Revenue, size = Potential,  label = Keyword, colour = SLD)) +
  geom_point(alpha = 1/2) +
  facet_wrap(~ SLD) +
  coord_flip() +
  scale_y_log10()  + 
  scale_x_reverse() +
  ggtitle("Payday Loans - Top Brands - Keyword Level Report") +
  xlab("Average Position") +
  ylab("Monthly Adwords Value from SEO (log)") +
  theme(axis.text.x = element_text(angle = 30, hjust = 1))
ggsave("PayDayLoans-KW-top_n.pdf", plot = last_plot(), width=11, height=8.5)



top_df <- inner_join(top9, datagrabber, by = "SLD")

