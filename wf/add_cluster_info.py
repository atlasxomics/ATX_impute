import statistics
import pandas as pd

ee = pd.read_csv('/Users/joshuab/Desktop/clean_latch/tests/barcodes_clusters.csv', header=None, skiprows=1)
ee.columns = ['barcode', 'clusters']
dd = pd.read_csv('/Users/joshuab/Desktop/clean_latch/tests/D01310/tissue_positions_list.csv', header=None, usecols=[0,1,2,3])
dd.columns = ['barcode', 'on_off', 'row', 'col']
final = pd.merge(ee,dd, how='outer', on='barcode')
final.to_csv('tissue_positions_list_clusters.csv')