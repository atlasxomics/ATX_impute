import csv
import logging
import math
import numpy as np
import pandas as pd
import statistics
import subprocess
import sys
import random

from typing import Dict, List

logging.basicConfig(
    format="%(levelname)s - %(asctime)s - %(message)s",
    level=logging.INFO
)

metrics_output = None
bad_elements = []
missing_lanes = {}
missing_tixel_neighbor = {}
number_of_channels = None
barcode_to_clusters = {}
clusters_to_barcode = {}
  
def average_duplicates(big_list: List[List[int]]) -> Dict[str, float]:
  """Combine row, col, diag reduction lists; if a barcode occurs in 
  more then one list, returns the average.
  """

  barcodes_match = {}
  final = {}
  holder = []
  for i in big_list:
    holder.extend(i)
    for x in holder:
      if x[0] not in barcodes_match.keys():
        barcodes_match[x[0]] = [x[1]]
      else:
        if x[1] not in barcodes_match[x[0]]:
          barcodes_match[x[0]].append(x[1])
  for i,j in barcodes_match.items():
    mean = statistics.mean(j)
    final[i] = mean

  return final

def filter_sc(position_path: str) -> pd.DataFrame:
  """ Reformat data, remove headers, apply custom column names for
  dataframes, add -1 to positions, remove off tixels.
  """
  global number_of_channels
  global barcode_to_clusters
  
  positions = pd.read_csv(position_path, header=None, usecols=[0,1,2,3,4])
  positions.columns = ['barcode', 'on_off', 'row', 'col', 'clusters']
  number_of_channels = math.sqrt(positions.shape[0])
  split_frame = positions[['barcode', 'on_off', 'clusters']]
  split_dict = split_frame.to_dict('split')['data']
  barcode_to_clusters = { bar:clu for (bar, on_off, clu) in split_dict}
  for i,x,j in split_dict:
    if x == 1:
      if j not in clusters_to_barcode.keys(): clusters_to_barcode[j] = [i]
      else: clusters_to_barcode[j].append(i)
  filtered = positions[positions['on_off'] == 1]

  return filtered

def get_neighbors(current_value: int, repeat: List[int]) -> List[int]:
  global bad_elements
  global number_of_channels
  
  all_neighbors = {}
  row = current_value[0]
  col = current_value[1]
  
  #right
  if col + 1 < number_of_channels and [row, col + 1] not in bad_elements:
    all_neighbors['r'] = [row, col + 1]
  #left
  if col - 1 >= 0 and [row, col - 1] not in bad_elements:
    all_neighbors['l'] = [row, col - 1]
  #down
  if row + 1 < number_of_channels and [row + 1, col] not in bad_elements:
    all_neighbors['d'] = [row + 1, col]
  #up
  if row - 1 >= 0 and [row - 1, col] not in bad_elements:
    all_neighbors['u'] = [row - 1, col]
  #leftUp
  if row - 1 >= 0 and col - 1 >= 0 and [row - 1, col - 1] not in bad_elements:
    all_neighbors['lu'] = [row - 1, col - 1]
  #leftDown
  if row + 1 < number_of_channels and col - 1 >= 0 and [row + 1, col - 1] not in bad_elements:
    all_neighbors['ld'] = [row + 1, col - 1]
  #rightUp
  if row - 1 >= 0 and col + 1 < number_of_channels and [row - 1, col + 1] not in bad_elements:
    all_neighbors['ru'] = [row - 1, col + 1]
  #rightDown
  if row + 1 < number_of_channels and col + 1 < number_of_channels and [row + 1, col + 1] not in bad_elements:
    all_neighbors['rd'] = [row + 1, col + 1]

  return all_neighbors

def multiple_degree(first_neighbors: List[int], degree: int, current: int) -> List[int]:
  current_neighbors = first_neighbors.copy()
  actual_degree = degree - 1
  for i in first_neighbors:
    for x in range(actual_degree):
      children = get_neighbors(i, current_neighbors)
      current_neighbors += children
  current_neighbors.remove(current)
  return current_neighbors

def neighbors_reductions(
    singlecell: pd.DataFrame,
    outliers: List[int],
    degree: int
  ) -> pd.DataFrame:
  """ Return table with barcode|barcode_index|adjust where "adjust"
  is the new value to reduce outlier lanes to; table to be used to
  reduce fragments.tsv  
  """
  global missing_lanes
  global missing_tixel_neighbor
  
  singlecell['adjust'] = 0
  for i in outliers:
    current_tixel = singlecell.iloc[i]
    row = current_tixel['row']
    col = current_tixel['col']
    barcode = current_tixel['barcode']
    neighbors = get_neighbors([row, col], [])
    # if degree > 1: neighbors += multiple_degree(neighbors, degree, i)        
    for pos,j in neighbors.items():
      try:
        current_neighbor = singlecell.loc[(singlecell['row'] == j[0]) & (singlecell['col'] == j[1])]
        current_barode = current_neighbor['barcode'].values[0]
        if barcode not in missing_tixel_neighbor.keys():
          missing_tixel_neighbor[barcode] = {}
          missing_tixel_neighbor[barcode][current_barode] = pos
        else: missing_tixel_neighbor[barcode][current_barode] = pos
        
      except Exception as e:
        pass


def imputate_lanes(
    singlecell: pd.DataFrame,
    degree: int
  ):
  """ Takes original data and applies the new values for (bad) tixels, then 
  updates the nmissing lanes within the data
  """
  global missing_lanes
  global bad_elements
  
  bad_elements = []
  all_elem_ids = {'row': [], 'col': []}
  for axis,lane in missing_lanes.items():
    for elem in lane:
      outlier_ids = np.where(singlecell[axis] == int(elem))
      all_elem_ids[axis] += outlier_ids[0].tolist()
    for bad_id in all_elem_ids[axis]:
      element = singlecell.iloc[bad_id]
      # final[element['barcode']] = 0
      row = element['row']
      col = element['col']
      bad_elements.append([row, col])
  
  for i,j in all_elem_ids.items():
    if len(j) > 0:
      neighbors_reductions(singlecell, j, degree)
  
    


def combine_tables(
    singlecell: pd.DataFrame,
    deviations: int=1,
    degree: int=1
  ) -> pd.DataFrame:
  global missing_lanes
  

  if (len(missing_lanes.values()) != 0):
    imputation_singlecell = singlecell.copy()
    imputate_lanes(imputation_singlecell, degree)


def update_fragments(
    fragments: pd.DataFrame
  ) -> pd.DataFrame:
  """Remove missing tixels from fragments and add them back
  """
  global missing_tixel_neighbor
  global clusters_to_barcode
  global barcode_to_clusters
  
  def max_cluster(list_clust: List[str]):
    counter = []
    max_val = 0
    final_clust = ''
    for i in list_clust:
      clust_appear = list_clust.count(i)
      tupe = (i, clust_appear)
      counter.append(tupe)
    
    counter.sort(key = lambda x: x[1], reverse=True)
    max_val = counter[0][1]
    count = 0
    for i in range(1,len(counter)):
      if i == max_val: count += 1
      else: break
    
    if count > 0:
      rand_num = random.randint(0,count)
      final_clust = counter[rand_num][0]
    else: final_clust = counter[0][0]
    
    return final_clust
    
  missing_barcodes = list(missing_tixel_neighbor.keys())
  remove_missing_barcodes = fragments[fragments['barcode'].isin(missing_barcodes) == False]
  final_frags = remove_missing_barcodes.copy()
  # return_value
  
  dict_data_clusters = {}
  for i,j in clusters_to_barcode.items():
    tixels_in_cluster = []
    dict_data_clusters[i] = {}
    for cluster_barcode in j:
      frag_count = remove_missing_barcodes[remove_missing_barcodes['barcode'] == cluster_barcode].shape[0]
      tixels_in_cluster.append(frag_count)
    try:
        dict_data_clusters[i]['avg_per_txl'] = math.ceil(statistics.mean(tixels_in_cluster))
        dict_data_clusters[i]['std'] = math.ceil(statistics.stdev(tixels_in_cluster))
    except:
        dict_data_clusters[i]['avg_per_txl'] = math.ceil(statistics.mean(tixels_in_cluster))
        dict_data_clusters[i]['std'] = math.ceil(statistics.mean(tixels_in_cluster) * .5)
    
  count = 0
  pre = pd.DataFrame()
  for m_tixel,j in missing_tixel_neighbor.items():
    count += 1
    print(count, pre.shape[0])
    define_cluster = []
    for barcode,direction in j.items():
      current_cluster = barcode_to_clusters[barcode]
      define_cluster.append(current_cluster)
    assigned_cluster = max_cluster(define_cluster)
    rand_plus_minu = random.choice([-1,1])
    clust_avg = dict_data_clusters[assigned_cluster]['avg_per_txl']
    clust_std = dict_data_clusters[assigned_cluster]['std']
    rand_std = random.randint(1, clust_std)
    given_frags = rand_std * rand_plus_minu + clust_avg
    current_cluster_frags = final_frags[final_frags['clusters'] == assigned_cluster]
    current_cluster_frags["barcode"] = [m_tixel for i in range(current_cluster_frags.shape[0])]
    downsampled = current_cluster_frags.sample(n=given_frags)
    pre = pd.concat([pre, downsampled])

  final_frags = pd.concat([final_frags, pre])
  final_frags = final_frags.drop('clusters', axis=1)
  return final_frags

def clean_fragments(
    fragments_path: str
  ) -> pd.DataFrame:
  """Reduce high tixels by randomly downsampling fragments.tsv
  according to reduction table.
  """
  global metrics_output
  global barcode_to_clusters
  
  logging.info("Loading fragments.tsv")
  fragments = pd.read_csv(
    fragments_path,
    sep='\t',
    header=None,
    comment='#'
  )
  fragments.columns = ['V1', 'V2', 'V3', 'barcode', 'V4']
  metrics_output['og'] = fragments.shape[0]
  # Add missing lanes if needed
  
  def add_clusters (v):
    all_barcode = v['barcode'].values
    all_clusters = [barcode_to_clusters[i] for i in all_barcode]
    return all_clusters
  
  logging.info("Splitting fragments.tsv")
  if (len(missing_lanes.values()) != 0):
    frag_cluster = fragments.assign(clusters = lambda x: add_clusters(x))
    fragments = None
    fragments = update_fragments(frag_cluster)
    
  return fragments

if __name__ == '__main__':
        
  metrics_output = {}
  run_id = sys.argv[1]
  metrics_output['run_id'] = run_id
  position_path = sys.argv[2]
  fragments_path = sys.argv[3]
  deviations = int(sys.argv[4])
  missing_rows = sys.argv[5].split(",")
  missing_cols = sys.argv[6].split(",")
  missing_lanes['row'] = missing_rows
  missing_lanes['col'] = missing_cols
  degree = 1

  singlecell = filter_sc(position_path)
  combine_tables(singlecell, deviations, degree)
  cleaned = clean_fragments(fragments_path)

  cleaned.to_csv(
    f'{run_id}_fragments.tsv',
    sep='\t',
    index=False,
    header=False
  )
  
  fields = [
    'Run_Id',
    'Columns downsampled',
    'Rows downsampled',
    'Diagonal downsampled',
    'Original fragments',
    'Final fragments',
    'pct_diff'
  ]
  filename = f'{run_id}_cleaning_metrics.csv'
  with open(filename, 'w') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(fields)
    writer.writerow(list(metrics_output.values()))
