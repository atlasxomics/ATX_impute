import statistics
import pandas as pd
import numpy as np

ee = pd.read_csv('/Users/joshuab/Desktop/ATX_impute/tests/D01332/barcodes_clusters_1332.csv', header=None, skiprows=1)
ee.columns = ['barcode', 'clusters']
ee['barcode'] = ee['barcode'].str.split('#').str[1]
dd = pd.read_csv('/Users/joshuab/Desktop/ATX_impute/tests/D01332/tissue_positions_list.csv', header=None, usecols=[0,1,2,3])
dd.columns = ['barcode', 'on_off', 'row', 'col']
dd['barcode'] = dd.loc[:,'barcode'].apply(lambda x: x + "-1")
final = pd.merge(dd,ee, how='outer', on='barcode')
not_in_archR = np.where(pd.isnull(final))
update = not_in_archR[0].tolist()
for i in update:
  final.iloc[i, 4] = "C0"
final.to_csv('tissue_positions_list_clusters.csv')