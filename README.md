# clean

<div align="center">
    <img src="images/data.png" alt="data" width="1000"/>
</div>

<br>
<br>

**impute** is a [latch.bio](https://latch.bio/) workflow for imputing missing row and column data
from spatial epigenomic experiments generated via [DBiT-seq](https://www.nature.com/articles/s41586-022-05094-1).  Provided outputs from an ArchRProject, a epigneomic fragment file, and a 'spatial' folder from [AtlasXBrowser](https://docs.atlasxomics.com/projects/AtlasXbrowser/en/latest/Overview.html), **impute** returns a fragments.tsv.gz with missing rows and columns filled in.

## Inputs
All input files for **impute** must be on the latch.bio [file system](https://wiki.latch.bio/wiki/data/overview).  

* run id: A string identifier for the experiment; AtlasXomics defaults to standard ATX run notation (ie. Dxxxxx_NGxxxxx.)

* missing rows: List of rows indices (1-based) to be imputed.

* missing columns: List of column indices (1-based) to be imputed.

* output directory: Name of the directory in the Latch file system that outputs will be saved under; outputs from **impute** are saved in subdirectories of the 'impute' directory (`/impute/[output directory]/`).

* positions file: A tissue_positions_list.csv containing tixel coordinates and on/off tissue designations for each tixel; this ile is output from the AtlasXBrowser app.

* fragments file: The fragments.tsv.gz file generated from a preprocessing and alignment pipeline; more information on the fragments file can be found [here](https://support.10xgenomics.com/single-cell-atac/software/pipelines/latest/output/fragments).

## Running the workflow

The **impute** workflow can be found in the [Workflows](https://wiki.latch.bio/workflows/overview) module in your latch.bio workspace. For access to an ATX-collaborator workspace, please contact your AtlasXomics Support Scientist or email support@atlasxomics.com.  See [here](https://wiki.latch.bio/workflows/overview) for general instructions for running workflows in latch.bio.

1. Navigate to the **impute** workflow in the Workflows module in your latch.bio workspace.  Ensure you are on the 'Parameters' tab of the workflow.

2. Add values to the input parameters fields, according to the descriptions provided above.

3. Click the 'Launch Workflow' button on the bottom-right of the parameters page.  This will automatically navigate you to the Executions tab of the workflow.

4. From the Executions tab, you can view the status of the launched workflow.  Once the workflow has completed running, the status will change to 'Succeeded'; if the workflow has the status 'Failed', please contact an AtlasXomics Support Scientist.  You can click on the workflow execution to view a more granular workflow status and see output logs.

5. Workflow outputs are loaded into the latch.bio [data module](https://wiki.latch.bio/wiki/data/overview) in the `impute` directory.

## Outputs

Outputs from **impute** are loaded into latch.bio [data module](https://wiki.latch.bio/wiki/data/overview) in the `impute` directory.

* imputed_[run_id]_fragments.tsv.gz: A fragments file with missing rows/columns imputed.

* [run_id]_imputation_metrics.csv: A comma-separated table containing the following summary statistics:
    * Columns imputed: Indices (1-based) of columns identified as outliers and downsampled, indexed from left to right.
    * Rows imputed: Indices (1-based) of rows identified as outliers and downsampled, indexed from left to right.
    * Original fragments: Total fragment count for the original fragment file.
    * Final fragments: Total fragment count for the imputed fragment file.
    * pct_diff: Percent of the original fragment count remaining in the cleaned fragment file (cleaned/original).

## Next Steps

Imputed fragment files can be used a input in downstream analysis (ArchR, Signac, Seuratm etc.).  Analysis can be performed locally or in a latch.bio [Pod](https://wiki.latch.bio/wiki/pods/overview).  For access to ATX-specific Pods, please contact your AtlasXomics Support Scientist.  

Further analysis can also be performed in latch.bio with the **optimize archr** (returns QC data and tests various input parameters on ArchR clustering), **create ArchRProject** (returns ArchRProject with peak and motif calling) and **atlasShiny** (returns inputs for the ATX ATAC-seq R Shiny App).  For access to these workflows, please contact your AtlasXomics Support Scientist.

## Support
Questions? Comments?  Contact support@atlasxomics.com or post in AtlasXomics [Discord](https://discord.com/channels/1004748539827597413/1005222888384770108).