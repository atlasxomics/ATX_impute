import csv
import logging
import math
import numpy as np
import pandas as pd
import statistics
import sys
import random
import multiprocessing
from typing import List, Dict

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

cpu_count = 2

def filter_sc(position_path: str) -> pd.DataFrame:
    """ Reformat data, remove headers, apply custom column names for
    dataframes, add -1 to positions, remove off tixels.
    """
    global number_of_channels
    global barcode_to_clusters
    global clusters_to_barcode

    positions = pd.read_csv(
        position_path, header=0, usecols=[0, 1, 2, 3, 4]
    )
    number_of_channels = math.sqrt(positions.shape[0])
    positions['barcode'] = (positions.loc[:, 'barcode']
                            .apply(lambda x: x + "-1"))
    positions['on_off'] = pd.to_numeric(positions['on_off'])
    positions['row'] = pd.to_numeric(positions['row'])
    positions['col'] = pd.to_numeric(positions['col'])
    split_frame = positions[['barcode', 'on_off', 'clusters']]
    split_dict = split_frame.to_dict('split')['data']
    barcode_to_clusters = {bar: clu for (bar, _, clu) in split_dict}
    for i, x, j in split_dict:
        if x == 1:
            if j not in clusters_to_barcode.keys():
                clusters_to_barcode[j] = [i]
            else:
                clusters_to_barcode[j].append(i)

    return positions


def get_neighbors(current_value: int, repeat: List[int]) -> List[int]:
    global bad_elements
    global number_of_channels

    all_neighbors = {}
    row = current_value[0]
    col = current_value[1]

    # right
    if col + 1 < number_of_channels and [row, col + 1] not in bad_elements:
        all_neighbors['r'] = [row, col + 1]
    # left
    if col - 1 >= 0 and [row, col - 1] not in bad_elements:
        all_neighbors['l'] = [row, col - 1]
    # down
    if row + 1 < number_of_channels and [row + 1, col] not in bad_elements:
        all_neighbors['d'] = [row + 1, col]
    # up
    if row - 1 >= 0 and [row - 1, col] not in bad_elements:
        all_neighbors['u'] = [row - 1, col]
    # leftUp
    if (row - 1 >= 0 and
            col - 1 >= 0 and
            [row - 1, col - 1] not in bad_elements):
        all_neighbors['lu'] = [row - 1, col - 1]
    # leftDown
    if (row + 1 < number_of_channels
            and col - 1 >= 0 and
            [row + 1, col - 1] not in bad_elements):
        all_neighbors['ld'] = [row + 1, col - 1]
    # rightUp
    if (row - 1 >= 0 and
            col + 1 < number_of_channels and
            [row - 1, col + 1] not in bad_elements):
        all_neighbors['ru'] = [row - 1, col + 1]
    # rightDown
    if (row + 1 < number_of_channels and
            col + 1 < number_of_channels and
            [row + 1, col + 1] not in bad_elements):
        all_neighbors['rd'] = [row + 1, col + 1]

    return all_neighbors


def multiple_degree(
    first_neighbors: List[int],
    degree: int,
    current: int
) -> List[int]:

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
    axis: str,
    degree: int
) -> pd.DataFrame:
    """ Return table with barcode|barcode_index|adjust where "adjust"
    is the new value to reduce outlier lanes to; table to be used to
    reduce fragments.tsv.
    """
    global missing_lanes
    global missing_tixel_neighbor

    singlecell['adjust'] = 0
    for i in outliers:
        current_tixel = singlecell.iloc[i]
        row = current_tixel['row']
        col = current_tixel['col']
        barcode = current_tixel['barcode']
        if barcode not in missing_tixel_neighbor.keys():
            missing_tixel_neighbor[barcode] = {}
        neighbors = get_neighbors([row, col], [])
        # if degree > 1: neighbors += multiple_degree(neighbors, degree, i)
        if len(neighbors) > 0:
            if current_tixel[axis] in missing_lanes[axis]:
                missing_lanes[axis].remove(current_tixel[axis])
        for pos, j in neighbors.items():
            try:
                current_neighbor = (
                    singlecell.loc[
                        (singlecell['row'] == j[0])
                        & (singlecell['col'] == j[1])
                    ]
                )
                current_barcode = current_neighbor['barcode'].values[0]
                missing_tixel_neighbor[barcode][current_barcode] = pos
            except Exception as e:
                logging.warn(f"{e}")
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
    for axis, lane in missing_lanes.items():
        for elem in lane:
            outlier_ids = np.where(singlecell[axis] == elem)
            all_elem_ids[axis] += outlier_ids[0].tolist()
        for bad_id in all_elem_ids[axis]:
            element = singlecell.iloc[bad_id]
            row = element['row']
            col = element['col']
            bad_elements.append([row, col])

    for i, j in all_elem_ids.items():
        if len(j) > 0:
            neighbors_reductions(singlecell, j, i, degree)


def combine_tables(
    singlecell: pd.DataFrame,
    degree: int = 1
  ) -> pd.DataFrame:
    global missing_lanes

    if len(missing_lanes['row'] + missing_lanes['col']) > 0:
        imputation_singlecell = singlecell.copy()
        imputate_lanes(imputation_singlecell, degree)

def max_cluster(list_clust: List[str]):
    counter = []
    max_val = 0
    final_clust = ''
    for i in list_clust:
        clust_appear = list_clust.count(i)
        tupe = (i, clust_appear)
        counter.append(tupe)

    counter.sort(key=lambda x: x[1], reverse=True)
    max_val = counter[0][1]
    count = 0
    for i in range(1, len(counter)):
        if i == max_val:
            count += 1
        else:
            break

    if count > 0:
        rand_num = random.randint(0, count)
        final_clust = counter[rand_num][0]
    else:
        final_clust = counter[0][0]

    return final_clust

def process_update(list_tup, final_frags, dict_data_clusters, queue, lock):
    global barcode_to_clusters
    
    alls = []
    for m_tixel, j in list_tup:
        define_cluster = []

        if len(j.keys()) > 0:
            for barcode, direction in j.items():
                current_cluster = barcode_to_clusters[barcode]
                define_cluster.append(current_cluster)

            assigned_cluster = max_cluster(define_cluster)
            rand_plus_minu = random.choice([-1, 1])
            clust_avg = dict_data_clusters[assigned_cluster]['avg_per_txl']
            clust_std = dict_data_clusters[assigned_cluster]['std']
            if clust_std < 1:
                clust_std = 1
            rand_std = random.randint(1, clust_std)
            given_frags = rand_std * rand_plus_minu + clust_avg
            current_cluster_frags = final_frags[
                final_frags['clusters'] == assigned_cluster
            ]
            current_cluster_frags["barcode"] = [
                m_tixel for i in range(current_cluster_frags.shape[0])
            ]
            if given_frags < 0:
                given_frags = 0
            downsampled = current_cluster_frags.sample(n=given_frags)
            alls.append(downsampled)
    with lock:
        queue.put(alls)
          
def split_dict(input_dict: Dict, num: int):
    items = list(input_dict.items())
    total_items = len(items)
    part_size = total_items // num
    remainder = total_items % num

    split_dicts = []
    start = 0

    for i in range(num):
        end = start + part_size + (1 if i < remainder else 0)
        split_dicts.append(dict(items[start:end]))
        start = end

    return split_dicts

def update_fragments(
    fragments: pd.DataFrame,
    stats_cluster: Dict[str, Dict[str, int]]
) -> pd.DataFrame:
    """Remove missing tixels from fragments and add them back
    """
    global missing_tixel_neighbor
    global cpu_count


    final_frags = fragments.copy()
    dict_data_clusters = stats_cluster

    pre = pd.DataFrame()
    split = split_dict(missing_tixel_neighbor, cpu_count)
    mult_process = []
    for frag_dict in split:
        new = [(i,j) for i,j in frag_dict.items()]
        mult_process.append(new)
    
    queue = multiprocessing.Queue()
    counter = multiprocessing.Value('i', -1 * cpu_count)  # Shared counter initialized to 0
    lock = multiprocessing.Lock()
    
    processes = []
    for missing_dict in mult_process:
        p = multiprocessing.Process(target=process_update, args=(missing_dict, final_frags, dict_data_clusters, queue, lock))
        processes.append(p)
        p.start()
        
    while counter.value != 0:
        result = queue.get()
        pre = pd.concat([pre, *result])
        with lock:
            counter.value += 1
        
    for p in processes:
        p.join()
        

    final_frags = pd.concat([final_frags, pre])
    final_frags = final_frags.drop('clusters', axis=1)
    missing_tixel_neighbor = {}
    return final_frags

def process_first_loop(chunk_barcode, queue, lock):
    global barcode_to_clusters
    
    clusters = []
    tixels = {}
    stats = {}
    for current_barcode in chunk_barcode:
        try:
            current_cluster = barcode_to_clusters[current_barcode]
            clusters.append(current_cluster)
        except Exception as e:
            current_cluster = 'C0'
            clusters.append('C0')
        try:
            if current_cluster not in tixels.keys():
                tixels[current_cluster] = {}
                stats[current_cluster] = {}
    
            if current_barcode not in tixels[current_cluster].keys():
                tixels[current_cluster][current_barcode] = 0
            tixels[current_cluster][current_barcode] += 1
        except Exception as e:
            print(e)
            pass
    with lock:
        queue.put((clusters, tixels, stats))
    
def process_second_loop(clust, counts):
    avg = 0
    std = 0
    try:
        avg = math.ceil(
                statistics.mean(counts)
            )
        std = math.ceil(
                statistics.stdev(counts)
            )
    except Exception as e:
        logging.warn(f"{e} cannot compute standard deviation")
        avg = math.ceil(
                statistics.mean(counts)
            )
        std = math.ceil(
                statistics.mean(counts) * .5
            )
    return((clust, avg, std))
      
def add_clusters(v):
    global cpu_count
    
    all_clusters = []
    tixels_in_cluster = {}
    stats_for_clusters = {}
    all_barcode = list(v['barcode'].values)
    
    chunksize = math.ceil(len(all_barcode)/cpu_count)
    split = [all_barcode[i:i+chunksize] for i in range(0, len(all_barcode), chunksize)]
    
    counter = multiprocessing.Value('i', -cpu_count)  # Shared counter initialized to 0
    lock = multiprocessing.Lock()
    queue = multiprocessing.Queue()
    
    processes = []
    for barcodes in split:
        p = multiprocessing.Process(target=process_first_loop, args=(barcodes, queue, lock))
        processes.append(p)
        p.start()
        
    while counter.value != 0 :
        result = queue.get()
        all_clusters += result[0]
        tixels_in_cluster.update(result[1])
        stats_for_clusters.update(result[2])
        with lock:
            counter.value += 1

    for p in processes:
        p.join()
        
    
    with multiprocessing.Pool() as pool:
        # prepare arguments
        items = [(i, list(j.values())) for i,j in tixels_in_cluster.items()]
        for result in pool.starmap(process_second_loop, items):
            clust = result[0]
            stats_for_clusters[clust]['avg_per_txl'] = result[1]
            stats_for_clusters[clust]['std'] = result[2]

    
    return list(all_clusters), stats_for_clusters


def clean_fragments(
    singlecell: pd.DataFrame,
    fragments_path: str
  ) -> pd.DataFrame:
    """Reduce high tixels by randomly downsampling fragments.tsv
    according to reduction table.
    """
    global metrics_output
    global barcode_to_clusters
    global missing_lanes
    global missing_tixel_neighbor

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
    logging.info("Splitting fragments.tsv")
    while len(missing_lanes['row'] + missing_lanes['col']) > 0:
        combine_tables(singlecell, degree)
        missing_barcodes = list(missing_tixel_neighbor.keys())
        remove_missing_barcodes = fragments[
            fragments['barcode'].isin(missing_barcodes) == False
        ]
        frag_cluster, stats_cluster = add_clusters(remove_missing_barcodes)
        remove_missing_barcodes['clusters'] = frag_cluster
        fragments = update_fragments(remove_missing_barcodes, stats_cluster)

    metrics_output['final'] = fragments.shape[0]
    metrics_output['pct'] = metrics_output['final'] / metrics_output['og']
    return fragments


if __name__ == '__main__':

    metrics_output = {}
    run_id = sys.argv[1]
    metrics_output['run_id'] = run_id

    missing_rows = [int(i) - 1 for i in sys.argv[2].split(",") if i != '']
    missing_cols = [int(i) - 1 for i in sys.argv[3].split(",") if i != '']
    missing_lanes['row'] = missing_rows
    missing_lanes['col'] = missing_cols

    metrics_output['col'] = ','.join([str(i) for i in missing_cols])
    metrics_output['row'] = ','.join([str(i) for i in missing_rows])

    fragments_path = sys.argv[4]
    position_path = sys.argv[5]

    degree = 1

    singlecell = filter_sc(position_path)
    cleaned = clean_fragments(singlecell, fragments_path)

    cleaned.to_csv(
        f'{run_id}_fragments.tsv',
        sep='\t',
        index=False,
        header=False
    )

    fields = [
        'Run_Id',
        'Columns imputed',
        'Rows imputed',
        'Original fragments',
        'Final fragments',
        'pct_diff'
    ]
    logging.info("Writing metrics")
    filename = f'{run_id}_cleaning_metrics.csv'
    with open(filename, 'w') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(fields)
        writer.writerow(list(metrics_output.values()))
