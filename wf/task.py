from wf.impute import *

from latch.resources.tasks import medium_task
from latch.types.directory import LatchDir
from latch.types.file import LatchFile

import csv
import logging
import pandas as pd
import subprocess

from typing import List, Optional

logging.basicConfig(
    format="%(levelname)s - %(asctime)s - %(message)s",
    level=logging.INFO
)


@medium_task
def impute_task(
    run_id: str,
    missing_rows: Optional[List[int]],
    missing_columns: Optional[List[int]],
    fragments_file: LatchFile,
    positions_file: LatchFile,
    archrproject: LatchDir,
    output_directory: str
) -> LatchDir:

    _r_cmd = [
        "Rscript",
        "wf/archr.R",
        run_id,
        positions_file.local_path,
        archrproject.local_path
    ]
    subprocess.run(_r_cmd)

    bc_clusters = pd.read_csv("barcode_clusters.csv", header=None, skiprows=1)
    bc_clusters.columns = ['barcode', 'clusters']

    positions = pd.read_csv(
        positions_file.local_path,
        header=None,
        usecols=[0, 1, 2, 3]
    )
    positions.columns = ['barcode', 'on_off', 'row', 'col']

    cluster_positions = pd.merge(
        bc_clusters,
        positions,
        how='outer',
        on='barcode'
    )
    cluster_positions.to_csv('tissue_positions_list_clusters.csv')

    metrics_output = {}
    metrics_output["run_id"] = run_id

    deviations = int(1)
    missing_lanes["row"] = missing_rows
    missing_lanes["col"] = missing_columns
    degree = 1

    clusters = filter_sc('tissue_positions_list_clusters.csv')
    reduct_dict = combine_tables(clusters, deviations, degree)
    imputed = clean_fragments(fragments_file.local_path, reduct_dict)

    # sort and zip output
    out_table = f"{run_id}_fragments.tsv"
    imputed.to_csv(out_table, sep='\t', index=False, header=False)

    _sort_cmd = ["sort", "-k1,1V", "-k2,2n", out_table]
    subprocess.run(_sort_cmd, stdout=open(f"imputed_{out_table}", "w"))

    _zip_cmd = ["bgzip", f"imputed_{out_table}"]
    subprocess.run(_zip_cmd)

    # make summary csv
    fields = [
        'Run_Id',
        'Columns imputed',
        'Rows imputed',
        'Original fragments',
        'Final fragments',
        'pct_diff'
    ]

    summary_csv = f'{run_id}_impute_metrics.csv'
    with open(summary_csv, 'w') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(fields)
        writer.writerow(list(metrics_output.values()))

    # Move outputs to output directory
    subprocess.run(["mkdir", output_directory])
    subprocess.run(
        ["mv", f"imputed_{out_table}.tsv.gz", summary_csv, output_directory]
    )

    return LatchDir(
        f"/root/{output_directory}", f"latch:///impute/{output_directory}"
    )
