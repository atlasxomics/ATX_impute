library(ArchR)

args <- commandArgs(trailingOnly = TRUE)

run_id <- args[1]
positions_file <- args[2]
archrproject <- args[3]

# Filter for only the desired run
proj <- loadArchRProject(archrproject)
ids <- which(proj$Sample == run_id)
cells_names <- proj$cellNames[ids]
proj_filtered <- proj[cells_names, ]

df <- getCellColData(proj_filtered, select = "Clusters")
df <- data.frame(barcodes = rownames(df), df, row.names = NULL)
df$barcodes <- sub(".*#(.*?)-.*", "\\1", df$barcodes)

write.csv(df, "barcode_clusters.csv", row.names = FALSE)