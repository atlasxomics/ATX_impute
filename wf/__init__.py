from wf.task import impute_task

from latch.resources.launch_plan import LaunchPlan
from latch.resources.workflow import workflow
from latch.types.directory import LatchDir
from latch.types.file import LatchFile
from latch.types.metadata import (
    LatchAuthor, LatchMetadata, LatchParameter, LatchRule
)

from typing import List, Optional, Union


metadata = LatchMetadata(
    display_name="ATX impute",
    author=LatchAuthor(
        name="AtlasXomics Inc.",
        email="joshuab@atlasxomics.com",
        github="https://github.com/atlasxomics",
    ),
    repository="https://github.com/atlasxomics/ATX_impute",
    license="MIT",
    parameters={
        "run_id": LatchParameter(
            display_name="run id",
            description="ATX Run ID with optional prefix, default to \
                        Dxxxxx_NGxxxxx format.",
            batch_table_column=True,
            placeholder="Dxxxxx_NGxxxxx",
            rules=[
                LatchRule(
                    regex="^[^/].*",
                    message="run id cannot start with a '/'"
                ),
                LatchRule(
                    regex="^\S+$",
                    message="run id cannot contain whitespace"
                )
            ]
        ),
        "missing_rows": LatchParameter(
            display_name="missing rows",
            description="List of rows to be imputed (1-50 or 1-96).",
            batch_table_column=True,
        ),
        "missing_columns": LatchParameter(
            display_name="missing columns",
            description="List of columns to be imputed (1-50 or 1-96).",
            batch_table_column=True,
        ),
        "fragments_file": LatchParameter(
            display_name="fragments file",
            description="fragments.tsv.gz file from an epigenomic \
                preprocessing and alignment workflow.",
            batch_table_column=True,
        ),
        "positions_file": LatchParameter(
            display_name="tissue positions file",
            description="tissue_positions_list.csv from spatial folder.",
            batch_table_column=True
        ),
        "cluster_specification": LatchParameter(
            display_name="cluster specification",
            description="ArchRProject or csv specifying the cluster of each \
                        tixel, with columns barcode|cluster_id.",
            batch_table_column=True,
        ),
        "output_directory": LatchParameter(
            display_name="output directory",
            batch_table_column=True,
            description="Name of Latch directory for merge fastq files; files \
                        will be saved to /impute/{output directory}.",
            rules=[
                LatchRule(
                    regex="^[^/].*",
                    message="output directory name cannot start with a '/'"
                ),
                LatchRule(
                    regex="^\S+$",
                    message="directory name cannot contain whitespace"
                )
            ]
        ),
    },
)


@workflow(metadata)
def impute_workflow(
    run_id: str,
    missing_rows: Optional[List[int]],
    missing_columns: Optional[List[int]],
    fragments_file: LatchFile,
    positions_file: LatchFile,
    cluster_specification: Union[LatchDir, LatchFile],
    output_directory: str
) -> LatchDir:

    return impute_task(
        run_id=run_id,
        missing_rows=missing_rows,
        missing_columns=missing_columns,
        fragments_file=fragments_file,
        positions_file=positions_file,
        cluster_specification=cluster_specification,
        output_directory=output_directory
        )


LaunchPlan(
    impute_workflow,
    "default",
    {
        "run_id": "default",
        "missing_rows": [11],
        "missing_columns": [24],
        "fragments_file": LatchFile(
            "latch:///chromap_outputs/demo/chromap_output/fragments.tsv.gz"
        ),
        "positions_file": LatchFile(
            "latch:///spatials/demo/spatial/tissue_positions_list.csv"
        ),
        "cluster_specification": LatchDir(
            "latch:///ArchRProjects/demo/demo_ArchRProject"
        ),
        "output_directory": "demo"
    }
)
