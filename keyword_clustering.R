# get the Hadley Wickham's vehicles data set
library(dplyr)
library(sparklyr)
spark_install(version = "2.0.1")

library(stringdist)
library(ggdendro)
library(ape)
library(ggplot2)
library(stringdist)
write.csv(kwp,"kwp_dl.csv")

loankws <- unique(as.character(kd[kd$Category == "loans",3]))
creditcardkws <- unique(as.character(kd[kd$Category == "creditcard",3]))

# call the stringdistmatrix function and request 20 groups


loans_dist_jw <- stringdistmatrix(loankws, loankws, method = "jw")
loans_dist_ja <- stringdistmatrix(loankws,loankws,method = "ja")
loans_dist_lv <- stringdistmatrix(loankws,loankws,method = "lv")
loans_dist_dl <- stringdistmatrix(loankws,loankws,method = "dl")

creditcards_dist_jw <- stringdistmatrix(creditcardkws,creditcardkws,method = "jw")
creditcards_dist_ja <- stringdistmatrix(creditcardkws,creditcardkws,method = "ja")
creditcards_dist_lv <- stringdistmatrix(creditcardkws,creditcardkws,method = "lv")
creditcards_dist_dl <- stringdistmatrix(creditcardkws,creditcardkws,method = "dl")



rownames(loans_dist_jw) <- loankws
rownames(loans_dist_ja) <- loankws
rownames(loans_dist_lv) <- loankws
rownames(loans_dist_dl) <- loankws

rownames(creditcards_dist_jw) <- creditcardkws
rownames(creditcards_dist_ja) <- creditcardkws
rownames(creditcards_dist_lv) <- creditcardkws
rownames(creditcards_dist_dl) <- creditcardkws


hc_loans_jw <- hclust(as.dist(loans_dist_jw))
hc_loans_ja <- hclust(as.dist(loans_dist_ja))
hc_loans_lv <- hclust(as.dist(loans_dist_lv))
hc_loans_dl <- hclust(as.dist(loans_dist_dl))

hc_creditcards_jw <- hclust(as.dist(creditcards_dist_jw))
hc_creditcards_ja <- hclust(as.dist(creditcards_dist_ja))
hc_creditcards_lv <- hclust(as.dist(creditcards_dist_lv))
hc_creditcards_dl <- hclust(as.dist(creditcards_dist_dl))


#Kmeans


# Cluster Plot against 1st 2 principal components

# vary parameters for most readable graph
library(cluster) 

loansfit1 <- kmeans(as.dist(loans_dist_jw), k)
loansfit2 <- kmeans(as.dist(loans_dist_ja), k)
loansfit3<- kmeans(as.dist(loans_dist_lv), k)
loansfit4<- kmeans(as.dist(loans_dist_dl), k)

ccfit1 <- kmeans(as.dist(creditcards_dist_jw), k)
ccfit2 <- kmeans(as.dist(creditcards_dist_ja), k)
ccfit3 <- kmeans(as.dist(creditcards_dist_lv), k)
ccfit4 <- kmeans(as.dist(creditcards_dist_dl), k)


library(class)
# kmeans clustering
cckfit1 <- kmeans(as.dist(creditcards_dist_jw), k)
cckfit2 <- kmeans(as.dist(creditcards_dist_ja), k)
cckfit3 <- kmeans(as.dist(creditcards_dist_lv), k)
cckfit4 <- kmeans(as.dist(creditcards_dist_dl), k)

lkfit1 <- kmeans(as.dist(loans_dist_jw), k)
lkfit2 <- kmeans(as.dist(loans_dist_ja), k)
lkfit3 <- kmeans(as.dist(loans_dist_lv), k)
lkfit4 <- kmeans(as.dist(loans_dist_dl), k)



creditcards_dist_jw_p0 <- stringdistmatrix(creditcardkws,creditcardkws,method = "jw", p = 0.0)
creditcards_dist_jw_p01 <- stringdistmatrix(creditcardkws,creditcardkws,method = "jw",  p = 0.1)
loans_dist_jw_p0 <- stringdistmatrix(loankws, loankws, method = "jw",  p = 0.0)
loans_dist_jw_p01 <- stringdistmatrix(loankws, loankws, method = "jw", p = 0.1)

cckfit1p0 <- kmeans(as.dist(creditcards_dist_jw_p0), 10)
cckfit1p01 <- kmeans(as.dist(creditcards_dist_jw_p01), 10)

lkfit1p0 <- kmeans(as.dist(loans_dist_jw_p0), 10)
lkfit1p01 <- kmeans(as.dist(loans_dist_jw_p01), 10)


clusplot(as.matrix(creditcards_dist_jw_p0), cckfit1p0$cluster)
clusplot(as.matrix(creditcards_dist_jw_p01), cckfit1p01$cluster)
clusplot(as.matrix(loans_dist_jw_p0), lkfit1p0$cluster)
clusplot(as.matrix(loans_dist_jw_p01), lkfit1p01$cluster)


#plot – need library cluster
library(cluster)
clusplot(as.matrix(creditcards_dist_jw), cckfit1$cluster)
clusplot(as.matrix(creditcards_dist_ja), cckfit2$cluster)
clusplot(as.matrix(creditcards_dist_lv), cckfit3$cluster)
clusplot(as.matrix(creditcards_dist_dl), cckfit4$cluster)

clusplot(as.matrix(loans_dist_jw), lkfit1$cluster)
clusplot(as.matrix(loans_dist_ja), lkfit2$cluster)
clusplot(as.matrix(loans_dist_lv), lkfit3$cluster)
clusplot(as.matrix(loans_dist_dl), lkfit4$cluster)


# DL = Damerau-Levenshtein distance
# LV = Levenshtein distance
# JA = Jaccard
# JW = Jaro-Winkler


clusplot(as.matrix(loans_dist_jw), lkfit1$cluster)

ccfit1jw <- kmeans(as.dist(creditcards_dist_jw), k)
ccfit2jw <- kmeans(as.dist(creditcards_dist_jw), k)

lfit1jw <- kmeans(as.dist(loans_dist_jw), k)
lfit2jw <- kmeans(as.dist(loans_dist_jw), k)





creditcards_dist_jw_p0 <- stringdistmatrix(creditcardkws,creditcardkws,method = "jw", p = 0.0)
creditcards_dist_jw_p01 <- stringdistmatrix(creditcardkws,creditcardkws,method = "jw",  p = 0.1)
loans_dist_jw_p0 <- stringdistmatrix(loankws, loankws, method = "jw",  p = 0.0)
loans_dist_jw_p01 <- stringdistmatrix(loankws, loankws, method = "jw", p = 0.1)

cckfit1p0 <- kmeans(as.dist(creditcards_dist_jw_p0), 10)
cckfit1p01 <- kmeans(as.dist(creditcards_dist_jw_p01), 10)

lkfit1p0 <- kmeans(as.dist(loans_dist_jw_p0), 10)
lkfit1p01 <- kmeans(as.dist(loans_dist_jw_p01), 10)


clusplot(as.matrix(creditcards_dist_jw_p0), cckfit1p0$cluster)
clusplot(as.matrix(creditcards_dist_jw_p01), cckfit1p01$cluster)
clusplot(as.matrix(loans_dist_jw_p0), lkfit1p0$cluster)
clusplot(as.matrix(loans_dist_jw_p01), lkfit1p01$cluster)

hc_loans_jw_p0 <- hclust(as.dist(loans_dist_jw_p0))
hc_loans_jw_p01 <- hclust(as.dist(loans_dist_jw_p01))
hc_loans_ja <- hclust(as.dist(loans_dist_ja))
hc_loans_lv <- hclust(as.dist(loans_dist_lv))
hc_loans_dl <- hclust(as.dist(loans_dist_dl))

hc_creditcards_jw_p0 <- hclust(as.dist(creditcards_dist_jw_p0))
hc_creditcards_jw_p01 <- hclust(as.dist(creditcards_dist_jw_p01))
hc_creditcards_ja <- hclust(as.dist(creditcards_dist_ja))
hc_creditcards_lv <- hclust(as.dist(creditcards_dist_lv))
hc_creditcards_dl <- hclust(as.dist(creditcards_dist_dl))


#Kmeans






dendr_ljw_p0    <- dendro_data(hc_loans_jw_p0, type="rectangle") # convert for ggplot
dendr_ljw_p01    <- dendro_data(hc_loans_jw_p01, type="rectangle") # convert for ggplot
dendr_lja    <- dendro_data(hc_loans_ja, type="rectangle") # convert for ggplot
dendr_llv    <- dendro_data(hc_loans_lv, type="rectangle") # convert for ggplot
dendr_ldl    <- dendro_data(hc_loans_dl, type="rectangle") # convert for ggplot

dendr_ccjw_p0    <- dendro_data(hc_creditcards_jw_p0, type="rectangle") # convert for ggplot
dendr_ccjw_p01    <- dendro_data(hc_creditcards_jw_p01, type="rectangle") # convert for ggplot
dendr_ccja    <- dendro_data(hc_creditcards_ja, type="rectangle") # convert for ggplot
dendr_cclv    <- dendro_data(hc_creditcards_lv, type="rectangle") # convert for ggplot
dendr_ccdl    <- dendro_data(hc_creditcards_dl, type="rectangle") # convert for ggplot



#install.packages('dendextend')
library(dendextend)

k=10
library(ggplot2)
library(ggdendro)

#hc_loans_jw
#hc_loans_ja

#HClust
clustljw_p0 = cutree(hc_loans_jw_p0, k=k)
clustljw_p01 = cutree(hc_loans_jw_p01, k=k)
clustlja = cutree(hc_loans_ja, k=k)
clustllv = cutree(hc_loans_lv, k=k)
clustldl = cutree(hc_loans_dl, k=k)

clustcjw_p0 = cutree(hc_creditcards_jw_p0, k=k)
clustcjw_p01 = cutree(hc_creditcards_jw_p01, k=k)
clustcja = cutree(hc_creditcards_ja, k=k)
clustclv = cutree(hc_creditcards_lv, k=k)
clustcdl = cutree(hc_creditcards_dl, k=k)



hc_loans_jw_p0 <- hclust(as.dist(loans_dist_jw_p0))
hc_loans_jw_p01 <- hclust(as.dist(loans_dist_jw_p01))
hc_loans_ja <- hclust(as.dist(loans_dist_ja))
hc_loans_lv <- hclust(as.dist(loans_dist_lv))
hc_loans_dl <- hclust(as.dist(loans_dist_dl))

hc_creditcards_jw_p0 <- hclust(as.dist(creditcards_dist_jw_p0))
hc_creditcards_jw_p01 <- hclust(as.dist(creditcards_dist_jw_p01))
hc_creditcards_ja <- hclust(as.dist(creditcards_dist_ja))
hc_creditcards_lv <- hclust(as.dist(creditcards_dist_lv))
hc_creditcards_dl <- hclust(as.dist(creditcards_dist_dl))

clust.ljw_p0.df <- data.frame(label=names(clustljw_p0), cluster=factor(clustljw_p0))
clust.ljw_p01.df <- data.frame(label=names(clustljw_p01), cluster=factor(clustljw_p01))
clust.lja.df <- data.frame(label=names(clustlja), cluster=factor(clustlja))
clust.llv.df <- data.frame(label=names(clustllv), cluster=factor(clustllv))
clust.ldl.df <- data.frame(label=names(clustldl), cluster=factor(clustldl))

clust.cjw_p0.df <- data.frame(label=names(clustcjw_p0), cluster=factor(clustcjw_p0))
clust.cjw_p01.df <- data.frame(label=names(clustcjw_p01), cluster=factor(clustcjw_p01))
clust.cja.df <- data.frame(label=names(clustcja), cluster=factor(clustcja))
clust.clv.df <- data.frame(label=names(clustclv), cluster=factor(clustclv))
clust.cdl.df <- data.frame(label=names(clustcdl), cluster=factor(clustcdl))


dendr_ljw_p0    <- dendro_data(hc_loans_jw_p0, type="rectangle") # convert for ggplot
dendr_ljw_p01    <- dendro_data(hc_loans_jw_p01, type="rectangle") # convert for ggplot
dendr_lja   <- dendro_data(hc_loans_ja, type="rectangle") # convert for ggplot
dendr_llv    <- dendro_data(hc_loans_lv, type="rectangle") # convert for ggplot
dendr_ldl   <- dendro_data(hc_loans_dl, type="rectangle") # convert for ggplot

dendr_cjw_p0   <- dendro_data(hc_creditcards_jw_p0, type="rectangle") # convert for ggplot
dendr_cjw_p01    <- dendro_data(hc_creditcards_jw_p01, type="rectangle") # convert for ggplot
dendr_cja   <- dendro_data(hc_creditcards_ja, type="rectangle") # convert for ggplot
dendr_clv    <- dendro_data(hc_creditcards_lv, type="rectangle") # convert for ggplot
dendr_cdl   <- dendro_data(hc_creditcards_dl, type="rectangle") # convert for ggplot


dendr_ljw_p0[["labels"]] <- merge(dendr_ljw_p0[["labels"]],clust.ljw_p0.df, by="label")
dendr_ljw_p01[["labels"]] <- merge(dendr_ljw_p01[["labels"]],clust.ljw_p01.df, by="label")
dendr_lja[["labels"]] <- merge(dendr_lja[["labels"]],clust.lja.df, by="label")
dendr_llv[["labels"]] <- merge(dendr_llv[["labels"]],clust.llv.df, by="label")
dendr_ldl[["labels"]] <- merge(dendr_ldl[["labels"]],clust.ldl.df, by="label")


dendr_cjw_p0[["labels"]] <- merge(dendr_cjw_p0[["labels"]],clust.cjw_p0.df , by="label")
dendr_cjw_p01[["labels"]] <- merge(dendr_cjw_p01[["labels"]],clust.cjw_p01.df , by="label")
dendr_cja[["labels"]] <- merge(dendr_cja[["labels"]],clust.cja.df, by="label")
dendr_clv[["labels"]] <- merge(dendr_clv[["labels"]],clust.clv.df, by="label")
dendr_cdl[["labels"]] <- merge(dendr_cdl[["labels"]],clust.cdl.df, by="label")


# plot the dendrogram; note use of color=cluster in geom_text(...)
ggplot() + 
  geom_segment(data=segment(dendr_ljw), aes(x=x, y=y, xend=xend, yend=yend)) + 
  geom_text(data=label(dendr_ljw), aes(x, y, label=label, hjust=0, color=cluster), 
            size=3) +
  coord_flip() + scale_y_reverse(expand=c(0.2, 0)) + 
  theme(axis.line.y=element_blank(),
        axis.ticks.y=element_blank(),
        axis.text.y=element_blank(),
        axis.title.y=element_blank(),
        panel.background=element_rect(fill="white"),
        panel.grid=element_blank())


# plot the dendrogram; note use of color=cluster in geom_text(...)
ggplot() + 
  geom_segment(data=segment(dendr_lja), aes(x=x, y=y, xend=xend, yend=yend)) + 
  geom_text(data=label(dendr_lja), aes(x, y, label=label, hjust=0, color=cluster), 
            size=3) +
  coord_flip() + scale_y_reverse(expand=c(0.2, 0)) + 
  theme(axis.line.y=element_blank(),
        axis.ticks.y=element_blank(),
        axis.text.y=element_blank(),
        axis.title.y=element_blank(),
        panel.background=element_rect(fill="white"),
        panel.grid=element_blank())



ggplot() + 
  geom_segment(data=segment(dendr_cjw_p01), aes(x=x, y=y, xend=xend, yend=yend)) + 
  geom_text(data=label(dendr_cjw_p01), aes(x, y, label=label, hjust=0, color=cluster), 
            size=3) +
  coord_flip() + scale_y_reverse(expand=c(0.2, 0)) + 
  theme(axis.line.y=element_blank(),
        axis.ticks.y=element_blank(),
        axis.text.y=element_blank(),
        axis.title.y=element_blank(),
        panel.background=element_rect(fill="white"),
        panel.grid=element_blank())


#KMeans

 
library("factoextra")
# K-means clustering
cc.res1 <- eclust(creditcards_dist_jw, "kmeans", k = 10, nstart = 25, graph = FALSE)
cc.res2 <- eclust(creditcards_dist_ja, "kmeans", k = 10, nstart = 25, graph = FALSE)
cc.res3 <- eclust(creditcards_dist_lv, "kmeans", k = 10, nstart = 25, graph = FALSE)
cc.res4 <- eclust(creditcards_dist_dl, "kmeans", k = 10, nstart = 25, graph = FALSE)

l.res1 <- eclust(loans_dist_jw, "kmeans", k = 10, nstart = 25, graph = FALSE)
l.res2 <- eclust(loans_dist_ja, "kmeans", k = 10, nstart = 25, graph = FALSE)
l.res3 <- eclust(loans_dist_lv, "kmeans", k = 10, nstart = 25, graph = FALSE)
l.res4 <- eclust(loans_dist_dl, "kmeans", k = 10, nstart = 25, graph = FALSE)
# k-means group number of each observation

fviz_cluster(cc.res1,  frame.type = "norm", frame.level = 0.68)
fviz_cluster(cc.res2,  frame.type = "norm", frame.level = 0.68)
fviz_cluster(cc.res3,  frame.type = "norm", frame.level = 0.68)
fviz_cluster(cc.res4,  frame.type = "norm", frame.level = 0.68)


fviz_cluster(l.res1,  frame.type = "norm", frame.level = 0.68)
fviz_cluster(l.res2,  frame.type = "norm", frame.level = 0.68)
fviz_cluster(l.res3,  frame.type = "norm", frame.level = 0.68)
fviz_cluster(l.res4,  frame.type = "norm", frame.level = 0.68)


fviz_dend(l.res1, rect = TRUE, show_labels = TRUE, cex = 0.5) 

clusplot(as.matrix(creditcards_dist_jw), cckfit1$cluster)
