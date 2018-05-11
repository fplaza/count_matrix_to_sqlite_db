library(RSQLite)

get_genes_profiles_tables=function(con)
{
    req="SELECT name
    FROM sqlite_master
    WHERE type='table';"

    tables=dbGetQuery(con,req)
    genes_profiles_tables=tables$name[grep( '^genes_profiles', tables$name)]
    genes_profiles_tables
}

get_num_genes=function(con)
{
    req = "SELECT COUNT(*) FROM genes"

    res = dbGetQuery(con,req)
    num_genes = res[1,1]
    num_genes
}

get_genes_names=function(con)
{
    req = "SELECT gene_name FROM genes"

    res = dbGetQuery(con,req)
    genes_names = res$gene_name
    genes_names
}

# genes ids and genes names are accepted but genes ids is much faster
# row.names can be either 'gene_id' or 'gene_name'
get_genes_profiles=function(con, genes)
{
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

    # Make sure that there is no duplicated genes
    genes=unique(genes)

    # Check SQLite connection
    if (class(con)[[1]] != "SQLiteConnection")
    {
        stop("con should be a SQLiteConnection")
    }

    # Select appropriate function whether genes were passed as names or as ids
    if (typeof(genes) == "character")
    {
        get_genes_profiles_by_=get_genes_profiles_by_name
    }
    else if (typeof(genes) %in% c("integer", "double"))
    {
        get_genes_profiles_by_=get_genes_profiles_by_id
    }
    else
    {
        stop("genes should be integers or strings")
    }

    # Get the list of tables that store the genes profiles
    genes_profiles_tables=get_genes_profiles_tables(con)

    # Retrieve the genes profiles  in each table
    final_res=matrix(nrow=length(genes), ncol=0)
    for (genes_profiles_table in genes_profiles_tables)
    {
        res=get_genes_profiles_by_(genes_profiles_table)

        # No genes found. Stop
        if (nrow(res) == 0)
        {
            return(NULL)
        }

        # Performs the concatenation
        final_res=cbind(final_res, as.matrix(res[,-(1:2)]))
    }

    # Set genes names as rownames
    rownames(final_res)=res[,2]

    final_res
}

get_samples_names=function(con)
{
    get_samples_names_=function(table_name)
    {
        req=paste0("PRAGMA table_info(", table_name,");")

        res=dbGetQuery(con,req)

        samples_names=res$name[-1]

        samples_names
    }

    # Check SQLite connection
    if (class(con)[[1]] != "SQLiteConnection")
    {
        stop("con should be a SQLiteConnection")
    }

    genes_profiles_tables=get_genes_profiles_tables(con)

    final_res=character()
    for (genes_profiles_table in genes_profiles_tables)
    {
        res=get_samples_names_(genes_profiles_table)

        # No genes found. Stop
        if (length(res) == 0)
        {
            return(NULL)
        }

        # Performs the concatenation
        final_res=c(final_res, res)
    }

    final_res
}

get_samples_profiles=function(con, samples)
{
    get_samples_names_=function(table_name)
    {
        req=paste0("PRAGMA table_info(", table_name,");")

        res=dbGetQuery(con,req)
        samples_names=res$name[-1]
        samples_names
    }

    get_samples_profiles_=function(samples, genes_profiles_table)
    {
        req = paste0("SELECT ", "`", paste(samples, collapse="`, `"), "` FROM genes NATURAL JOIN ", genes_profiles_table)
        dbGetQuery(con,req)
    }

    # Check SQLite connection
    if (class(con)[[1]] != "SQLiteConnection")
    {
        stop("con should be a SQLiteConnection")
    }

    if (typeof(samples) != "character")
    {
        stop("samples should be strings")
    }

    num_genes=get_num_genes(con)
    genes_profiles_tables=get_genes_profiles_tables(con)

    final_res=matrix(nrow=num_genes, ncol=0)
    for (genes_profiles_table in genes_profiles_tables)
    {
        samples_in_table=get_samples_names_(genes_profiles_table)
        samples_in_table=samples_in_table[samples_in_table %in% samples]

        if (length(samples_in_table > 0))
        {
            res=get_samples_profiles_(samples_in_table, genes_profiles_table)
            final_res=cbind(final_res, as.matrix(res))
        }
    }

    final_res
}

