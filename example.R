library(RSQLite)
con=dbConnect(RSQLite::SQLite(),dbname="IGC.raw_profiles.new.db")
results=dbGetQuery(con,"SELECT * FROM genes NATURAL JOIN genes_profiles WHERE gene_id IN('5600000','690000','9898')")
genes_ids=results[,"GENE_ID"]
genes_names=results[,2]
genes_profiles=results[,-c(1,2)]
