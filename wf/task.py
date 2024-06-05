from latch.resources.tasks import large_task
from latch.types.directory import LatchDir
from latch.types.file import LatchFile

import logging
import pandas as pd
import subprocess
import numpy as np

from typing import List, Optional, Union

logging.basicConfig(
    format="%(levelname)s - %(asctime)s - %(message)s",
    level=logging.INFO
)


@large_task
def impute_task(
    run_id: str,
    missing_rows: Optional[List[int]],
    missing_columns: Optional[List[int]],
    fragments_file: LatchFile,
    positions_file: LatchFile,
    cluster_specification: Union[LatchDir, LatchFile],
    output_directory: str
) -> LatchDir:

    if type(cluster_specification) is LatchDir:
        _r_cmd = [
            "Rscript",
            "wf/archr.R",
            run_id,
            positions_file.local_path,
            cluster_specification.local_path
        ]
        subprocess.run(_r_cmd)
    elif type(cluster_specification) is LatchFile:
        subprocess.run(
            ["mv", cluster_specification.local_path, "./barcode_clusters.csv"]
        )

    bc_clusters = pd.read_csv("barcode_clusters.csv", header=None, skiprows=1)
    bc_clusters.columns = ["barcode", "clusters"]

    positions = pd.read_csv(
        positions_file.local_path,
        header=None,
        usecols=[0, 1, 2, 3]
    )
    positions.columns = ["barcode", "on_off", "row", "col"]

    cluster_positions = pd.merge(
        positions,
        bc_clusters,
        how="outer",
        on="barcode"
    )
    not_in_archR = np.where(pd.isnull(cluster_positions))
    update = not_in_archR[0].tolist()
    for i in update:
        cluster_positions.iloc[i, 4] = "C0"
    cluster_positions.to_csv("tissue_positions_list_clusters.csv", index=False)

    _impute_cmd = [
        "python",
        "wf/impute.py",
        run_id,
        ",".join([str(i) for i in missing_rows]) if missing_rows else "",
        ",".join([str(i) for i in missing_columns]) if missing_columns else "",
        fragments_file.local_path,
        "tissue_positions_list_clusters.csv"
    ]

    subprocess.run(_impute_cmd)

    # Move outputs to output directory
    metrics = f"{run_id}_cleaning_metrics.csv"
    out_table = f"{run_id}_fragments.tsv"

    _sort_cmd = [
        "sort",
        "-k1,1V",
        "-k2,2n",
        out_table
    ]
    subprocess.run(_sort_cmd, stdout=open(f"imputed_{out_table}", "w"))

    _zip_cmd = [
        "bgzip",
        f"imputed_{out_table}"
    ]
    logging.info("Zipping fragments.tsv")
    subprocess.run(_zip_cmd)

    subprocess.run(["mkdir", output_directory])
    subprocess.run(
        ["mv", f"imputed_{out_table}.gz", metrics, output_directory]
    )

    return LatchDir(
        f"/root/{output_directory}", f"latch:///impute/{output_directory}"
    )


if __name__ == "__main__":

    impute_task(
        run_id="D01279",
        missing_rows=[1],
        missing_columns=[10, 34],
        fragments_file="latch://13502.account/atac_outs/D01270_NG02546/outs/D01270_NG02546_fragments.tsv.gz",
        positions_file="latch://13502.account/spatials/x50_all_tissue_positions_list.csv",
        cluster_specification="latch://13502.account/ArchRProjects/D01270/D01270_ArchRProject",
        output_directory="dev_test"
    )
