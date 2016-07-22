setwd('/Users/aadil/desktop/Rworkspace')
# Set the system environment variables
Sys.setenv(SPARK_HOME = "/Users/aadil/spark-1.6.2-bin-hadoop2.6")
.libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))
#load the Sparkr library
library(SparkR)
library(RecordLinkage)
library(data.table)
library(stringdist)
# Create a spark context and a SQL context
sc <- sparkR.init(master = "local")
sqlContext <- sparkRSQL.init(sc)

#*********************************************** Dataset Preparation ********************************************************************************

dataset1<-read.csv(file.choose(), header=FALSE,colClasses="character",as.is=TRUE,na.strings=NULL,sep="\t")
#dataset1

dataset2<-read.csv(file.choose(), header=FALSE,colClasses="character",as.is =TRUE,na.strings=NULL,sep="\t")
dataset2

names(dataset1)<-c("FNAME","LNAME")
names(dataset2)<-c("FNAME","LNAME")

#dataset2

#names(datasetURL)<-c("tin","firstName","middleName","lastName","provider_id","providerfullname","addressline1","addressline2","city","state","zipcode")
#names(datasetNDB)<-c("prov_id","tax_id","lastname","firstname","midname","adr_id","addressline1","city","state","zipcode","canc_dt")


df1<-data.frame(dataset1$FNAME,dataset1$LNAME,fullname1=paste(dataset1$FNAME,dataset1$LNAME))

df2<-data.frame(dataset2$FNAME,dataset2$LNAME,fullname2=paste(dataset2$FNAME,dataset2$LNAME))

df1
df2



trim<- function (x) {
  x<- tolower(x)
  x<- gsub("^\\s+\\s+$","", x)
  x<-gsub("\\s+"," ",x)
  x<- gsub("[[:punct:]]", " ", x)
  x<-gsub("\\s+"," ",x)
  x<- gsub("street", "st", x)
  x<- gsub("drive", "dr", x)
  x<- gsub("suite","ste",x)
  x<- gsub("building","bldg",x)
  x<- gsub(" $","",x)
  
}



#df1$pfullName1<-trim((df1$fullname1))
df1$fullname1
df1$NamePhonetic<-phonetic(df1$fullname1,method=c("soundex"),useBytes=FALSE)


#df2$pfullName2<-trim(df2$fullname2)
df2$fullname2
df2$NamePhonetic<-phonetic(df2$fullname2,method=c("soundex"),useBytes=FALSE)
namematchdf<-merge(df1,df2,by="NamePhonetic",all=TRUE)
#namematchdf<-c(namematchdf)
#namematchdf<-as.character(namematchdf)
#Sys.setlocale('LC_ALL','C')
namematchdf <- data.frame(lapply(namematchdf, as.character), stringsAsFactors=FALSE)
#is.character(namematchdf$dataset1.FNAME)
namematchdf$namesimscore<-levenshteinSim(namematchdf$fullname1,namematchdf$fullname2)
write.table(namematchdf,file="test.txt",col.names=TRUE,quote=FALSE,sep="\t")
namematchdf<- createDataFrame(sqlContext, namematchdf)

#Query
perfectScore <- filter(namematchdf, namematchdf$namesimscore == 1)



head(arrange(namematchdf, desc(namematchdf$namesimscore)))

head(perfectScore)


sparkR.stop()
