library(RSQLite)

# genes ids and genes names are accepted but genes ids is much faster
# row.names can be either 'gene_id' or 'gene_name'
get_genes_profiles=function(con, genes)
{
  get_genes_profiles_tables=function()
  {
    req="SELECT name
    FROM sqlite_master
    WHERE type='table';"
    
    tables=dbGetQuery(con,req)
    genes_profiles_tables=tables$name[grep( '^genes_profiles', tables$name)]
    genes_profiles_tables
  }
  
  get_genes_profiles_by_id=function(genes_profiles_table)
  {
    req = paste("SELECT * FROM genes NATURAL JOIN", genes_profiles_table,
                "WHERE gene_id IN (", paste(genes, collapse=','), ") ",
                "ORDER BY gene_id")
    
    dbGetQuery(con,req)
  }
  
  get_genes_profiles_by_name=function(genes_profiles_table)
  {
    genes=sapply(genes, function(gene){return(paste0("'", gene, "'"))})
    
    req = paste("SELECT * FROM genes NATURAL JOIN", genes_profiles_table,
                "WHERE gene_name IN (", paste(genes, collapse=','), ") ",
                "ORDER BY gene_id"
    )
    
    
    dbGetQuery(con,req)
  }
  
  # Get the list of tables that store the genes profiles
  genes_profiles_tables=get_genes_profiles_tables()
  
  # Make sure that there is no duplicated genes
  genes=unique(genes)
  
  # Select appropriate function whether genes were passed as names or as ids
  if (typeof(genes) == "character")
  {
    get_genes_profiles_by_=get_genes_profiles_by_name
  }
  else if (typeof(genes) %in% c("integer", "double"))
  {
    get_genes_profiles_by_=get_genes_profiles_by_id
  }
  
  # Retrieve the genes profiles  in each table
  final_res=matrix(nrow=length(genes), ncol=0)
  for (genes_profiles_table in genes_profiles_tables)
  {
    res=get_genes_profiles_by_(genes_profiles_table)
    # Performs the concatenation
    final_res=cbind(final_res, as.matrix(res[,-(1:2)]))
  }
  
  # Set genes names as rownames
  rownames(final_res)=res[,2]
  
  final_res
}