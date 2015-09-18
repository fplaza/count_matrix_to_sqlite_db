library(RSQLite)
con=dbConnect(RSQLite::SQLite(),dbname="count_matrix.db")
profiles=dbGetQuery( con,"select * from count_matrix where gene_id in('5600000','690000','9898')")
