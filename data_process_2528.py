import ast
import json
import os
import pandas as pd
import csv
import datetime

def record_guardian_info(pathlog):

    f = open(pathlog, 'r', errors='ignore')
    # now we will open a file for writing
    data_file = open('data_file.csv', 'w')
     
    # create the csv writer object
    csv_writer = csv.writer(data_file)

    csv_writer.writerow(['date', 'line_open', 'groups_checkout', 'enter', 'exit'])

    for x in f:
        
        if ('metadata' in x) and ( 'predict_lane_num'not in x) and ('report15' not in x):
    
            data_row = [0 for _ in range(5)]
            res = json.loads(x)
            people_queue_time = res['ended_at']
            print(people_queue_time[:19])
            utc_date = datetime.datetime.strptime(str(people_queue_time[:19]), "%Y-%m-%dT%H:%M:%S")
            guardian_real_time = str(utc_date - datetime.timedelta(hours=7))
            data_row[0] = guardian_real_time[:-2] + '00'
            # print(res)
            if res['camera_id'] < 6:
                if 'queues' not in res['metadata']:
                    continue
                if res['metadata']['queues'][0]['queue_name'] != 'Lane SCO':
                    data_row[2] = res['metadata']['queues'][0]['avg_group_len']
                    if res['metadata']['queues'][0]['queue_status'] != 'off':
                        data_row[1] = 1
            elif res['camera_id'] > 6:
                data_row[3] = res['metadata']['enter']
                data_row[4] = res['metadata']['exit']
            
            csv_writer.writerow(data_row)
    data_file.close()
    data_file_location = os.path.join(os.path.dirname(__file__), 'data_file.csv')
    return data_file_location


  

def merge_data(data_file_location):
    df = pd.read_csv(data_file_location)
    df = df.groupby(['date']).sum()
    df = df.reset_index()
    df['date'] = pd.to_datetime(df['date'])
    d = {x: y for x , y in df.groupby(df['date'].dt.date)}
    for x, y in d.items():
        y.to_csv(f"{x}.csv", index=False)
    # del  delete redundant files
    os.remove(os.path.join(os.path.dirname(__file__), data_file_location))
    return
files = ['25--26.json','26--27.json', '27--28.json','28--29.json', '29--30.json','30--31.json', ]

def main():
    print('Process the guardian data info is start')
    # guardian log's location
    for file_name in files:
        print(f'log file : {file_name}')
        guardian_logs = os.path.join(os.path.dirname(__file__), file_name)
        data_file_location = record_guardian_info(guardian_logs)
        merge_data(data_file_location)
    


if __name__ == '__main__':
    main()

