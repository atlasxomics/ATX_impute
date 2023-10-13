library(ArchR)

args <- commandArgs(trailingOnly = TRUE)

run_id <- args[1]
positions_file <- args[2]
archrproject <- args[3]

proj <- loadArchRProject(archrproject)