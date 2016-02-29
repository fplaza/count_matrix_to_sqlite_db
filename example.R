library(RSQLite)
con=dbConnect(RSQLite::SQLite(),dbname="IGC.raw_profiles.new.db")
results=dbGetQuery( con,"select * from GENES_PROFILES where gene_id in('5600000','690000','9898')")
genes_ids=results[,1]
genes_names=results[,2]
genes_profiles=results[,-c(1,2)]
