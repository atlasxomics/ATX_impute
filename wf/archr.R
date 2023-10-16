library(ArchR)

args <- commandArgs(trailingOnly = TRUE)

run_id <- args[1]
positions_file <- args[2]
archrproject <- args[3]

proj <- loadArchRProject(archrproject)

col <- proj@cellColData@listData[["Clusters"]]
col2 <- proj@cellColData@rownames
unlist(col)
unlist(col2)
combine <- array(c(col2, col), dim = c(length(col), 2, 1))
write.csv(combine, "barcodes_clusters.csv", row.names=FALSE)