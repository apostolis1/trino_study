# Trino logs are saved in server's memory, meaning older query information is lost after running lots of queries
# Utility script to fetch the json results from the server and save them locally

import requests
import json
import os
import re
import numpy as np
import matplotlib.pyplot as plt 

MAPPING_PATH = "/home/apostolis/Projects/bigdata/scripts/charts/opt_optscrossjoin.json"
LOG_OUTPUT_FOLDER = "/home/apostolis/Projects/bigdata/scripts/charts/query_logs/dist_optscrossjoin"

API = "http://83.212.81.114:8080/ui/api/query/"



ui_tok = {"Trino-UI-Token": "UI_TOKEN_HERE"}


def convert_to_seconds(time_str):
    # Regular expression to capture the number and the unit
    if time_str == "0.00ns":
        return 0
    match = re.match(r"(\d+(\.\d+)?)([hms])", time_str.strip())
    
    if not match:
        raise ValueError(f"Invalid time format: {time_str}")
    
    value, _, unit = match.groups()
    value = float(value)
    
    # Convert based on the unit
    if unit == 'h':  # Convert hours to minutes
        return value * 60 * 60
    elif unit == 'm':  # Already in minutes
        return value * 60
    elif unit == 's':  # Convert seconds to minutes
        return value
    else:
        return 0
        raise ValueError(f"Unsupported time unit: {unit}")

def get_logs(map_path = MAPPING_PATH, log_folder = LOG_OUTPUT_FOLDER):
    raise KeyError()
    with open(map_path, "r") as f:
        queries = json.load(f)
    for query_name, query_ids in queries.items():
        for query_id in query_ids:
            path = os.path.join(log_folder, query_name+"_"+query_id+".json")
            data = requests.get(API + query_id, cookies=ui_tok)        
            with open(path, "w+") as f:
                json.dump(data.json(), f)

def get_stats_from_logs(map_path = MAPPING_PATH, log_folder = LOG_OUTPUT_FOLDER):
    with open(map_path, "r") as f:
        queries = json.load(f)
    results = {}
    for query_name, query_ids in queries.items():
        query_results = []
        for query_id in query_ids:
            path = os.path.join(log_folder, query_name+"_"+query_id+".json")
            with open(path, "r") as f:
                d = json.load(f)
                data = d["queryStats"]["physicalInputReadTime"]
                query_results.append(convert_to_seconds(data))
        results[query_name] = np.average(query_results)
        if query_name == "query37":
            print(query_results)
    # print(results)
    return results

def show_bar_graph():


    map1 = "/home/apostolis/Projects/bigdata/scripts/charts/naive.json"
    logs1 = "/home/apostolis/Projects/bigdata/scripts/charts/query_logs/naive"
    map2 = "/home/apostolis/Projects/bigdata/scripts/charts/opt_optsauto.json"
    logs2 = "/home/apostolis/Projects/bigdata/scripts/charts/query_logs/dist_optsauto"

    results_1 = get_stats_from_logs(map1, logs1)
    results_2 = get_stats_from_logs(map2, logs2)
    
    mykeys = results_1.keys()
    

    ylabel = "Execution Time (s)"
    symbolicxcoords = ",".join(i.replace("query", "Q") for i in mykeys)
    c1 = [f"({k.replace('query', 'Q')}, {round(v)})" for k,v in results_1.items() if k in mykeys]
    c2 = [f"({k.replace('query', 'Q')}, {round(v)})" for k,v in results_2.items() if k in mykeys]

    coords1 = " ".join(c1)
    coords2 = " ".join(c2)

    res = f"""
    \\begin{{tikzpicture}}
    \\begin{{axis}}[
        ybar,
        width=20cm,
        ymin = 0,
        bar width=16pt,
        height=10cm,
        enlarge x limits=0.08,
        legend style={{at={{(0.5,-0.15)}},
        anchor=north,legend columns=-1}},
        ylabel={{{ylabel}}},
        symbolic x coords={{{symbolicxcoords}}},
        xtick=data,
        nodes near coords,
        nodes near coords align={{vertical}},
        ]
    \\addplot coordinates {{{coords1}}};
    \\addplot coordinates {{{coords2}}};
    \\legend{{Naive Dist, Optimal Dist}}
    \\end{{axis}}
    \\end{{tikzpicture}}
        """

    print(res)
    return

    # set width of bar 
    barWidth = 0.25
    fig = plt.subplots(figsize =(12, 8)) 

    # set height of bar 
    RES_1 = list(results_1.values()) 
    RES_2 = list(results_2.values())  

    # Set position of bar on X axis 
    br1 = np.arange(len(RES_1)) 
    br2 = [x + barWidth for x in br1] 

    # Make the plot
    plt.bar(br1, RES_1, color ='r', width = barWidth, 
            edgecolor ='grey', label ='AUTO OPT') 
    plt.bar(br2, RES_2, color ='g', width = barWidth, 
            edgecolor ='grey', label ='CROSSJOIN ELIM') 

    # Adding Xticks 
    plt.xlabel('Queries', fontweight ='bold', fontsize = 15) 
    plt.ylabel('Execution Time (m)', fontweight ='bold', fontsize = 15) 
    plt.xticks([r + barWidth for r in range(len(RES_1))], 
            list(results_1.keys()))

    plt.legend()
    plt.show()



# get_logs()
# get_stats_from_logs()

show_bar_graph()