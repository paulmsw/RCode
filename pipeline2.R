options(stringsAsFactors=T)
source("~/Data/RandD/RCode/http-json.R")
ctrmodel <- read.csv("~/Data/sales/ctrmodel.csv")
KWP <- read.csv("~/Data/sales/CCPhrase.csv")
setwd("~/Data/RandD/RCode/")

ctrmod <- ctrmodel %>% select(2:3)
rm(ctrmodel)
df <- x0  %>% select(2,3,4,5)
names(df) <- c("Key","Position", "Url", "Domain")

#Provide CTR by joining look-up CTR model
df1 <- data.table(inner_join(ctrmod, df, by = "Position" ))
df2 <- df1[,Keyword:=lapply(Key, function(x) x[2])]
df3 <- df2[,Year:=lapply(Key, function(x) x[3])]
df4 <- df3[,Month:=lapply(Key, function(x) x[4])]
df5 <- df4[,Day:=lapply(Key, function(x) x[5])]
df6 <- df5[,Country:=lapply(Key, function(x) x[1])]
df6$Key  <- NULL
df7 <- as.Date(paste(df6$Year, df6$Month, df6$Day, sep = "-"), format = "%Y-%m-%d")
df7$Year <- NULL
df7$Month <- NULL
df7$Day <- NULL
#Phew... that was long winded, I hope someone shows me a more elagent way to do that!




