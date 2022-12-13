import os
import pandas as pd
import xlsxwriter
import re
import datetime
import json
global time_hour_diff

def record_people_count_info(sorted_logs, pathlogs):
	# 2：processing
	workbook = xlsxwriter.Workbook('people_logs.xlsx')
	worksheet = workbook.add_worksheet()
	# add a header to the sheet
	worksheet.write('A1', 'date')
	worksheet.write('B1', 'hour')
	worksheet.write('C1', 'minutes')
	worksheet.write('D1', 'seconds')
	worksheet.write('E1', 'milsec')
	worksheet.write('F1', 'camera_id_const')
	worksheet.write('G1', 'camera_id')
	worksheet.write('H1', 'metadata')
	worksheet.write('I1', 'enter_const')
	worksheet.write('J1', 'enter')
	worksheet.write('K1', 'exit_const')
	worksheet.write('L1', 'exit')

	logstring = []
	keyword = 'metadata'
	# if there are additional info added to the metadata that is not wanted in the data processing, add it to the ignor list
	ignore_list_1 = 'report15'
	ignore_list_2 = 'mid3'
	row = 1
	for file in sorted_logs:
		if os.path.isfile(os.path.join(pathlogs, file)):
			f = open(os.path.join(pathlogs, file), 'r', errors='ignore')
			for x in f:
				if keyword in x:
					if ignore_list_1 in x or ignore_list_2 in x:
						continue
					x1 = json.loads(x)
					if 'enter' not in x1['metadata'].keys():
						continue
					people_count_id = x1['camera_id']
					people_count_info = x1['metadata']
					people_count_time = x1['ended_at']
					camera_ids = x1['camera_ids']
					num_cam = len(camera_ids)
					utc_date1 = datetime.datetime.strptime(str(people_count_time), "%Y-%m-%dT%H:%M:%S.%fZ")
					guardian_real_time = str(utc_date1 - datetime.timedelta(hours=time_hour_diff))
					guardian_real_time_1 = guardian_real_time.split('.')[0] + str(',623')
					#add_str = " [INFO ][39214][ThreadPoolExecutor-1_0][people_queuing_context:flow_generate_event_without_recording:469] camera 6 --[People Queuing] mid - metadata: {'queues':"
					guardian_reformat = guardian_real_time_1 + " camera_id:" + str(people_count_id) + ' metadata:' + str(people_count_info)

					y = guardian_reformat
					z = y.split(',')
					y = y.replace('[', '')
					y = y.replace(']', '')
					z = re.split(",| |:|'|[|]|--|{|}|[|]", y)

					while ("" in z):
						z.remove("")
					logstring.append(z)
					col = 0
					for entries in z:
						worksheet.write(row, col, entries)
						col += 1
					row += 1
			f.close
	workbook.close()
	people_logs_location = os.path.join(os.path.dirname(__file__), 'people_logs.xlsx')
	return people_logs_location, num_cam, camera_ids

def record_guardian_info(sorted_logs, pathlogs):
	# 2：processing
	workbook = xlsxwriter.Workbook('guardian_logs.xlsx')
	worksheet = workbook.add_worksheet()
	# add a header to the sheet
	worksheet.write('A1', 'date')
	worksheet.write('B1', 'hour')
	worksheet.write('C1', 'minutes')
	worksheet.write('D1', 'seconds')
	worksheet.write('E1', 'milsec')
	worksheet.write('F1', 'INFO')
	worksheet.write('G1', 'thread')
	worksheet.write('H1', 'flow')
	worksheet.write('I1', '359')
	worksheet.write('J1', 'camera')
	worksheet.write('K1', 'camera_num')
	worksheet.write('L1', 'People')
	worksheet.write('M1', 'Queuing')
	worksheet.write('N1', 'Mid')
	worksheet.write('O1', '-')
	worksheet.write('P1', 'metadata')
	worksheet.write('Q1', 'queues')
	worksheet.write('R1', 'queue_id_CONST')
	worksheet.write('S1', '1')
	worksheet.write('T1', 'queue_name_const')
	worksheet.write('U1', 'Lane_const')
	worksheet.write('V1', 'queue_name')
	worksheet.write('W1', 'queue_status_const')
	worksheet.write('X1', 'queue_status')
	worksheet.write('Y1', 'avg_group_const')
	worksheet.write('Z1', 'avg_group_len')
	worksheet.write('AA1', 'avg_queue_time_const')
	worksheet.write('AB1', 'avg_queue_time')
	worksheet.write('AC1', 'avg_wait_group_len_const')
	worksheet.write('AD1', 'avg_wait_group_len')
	worksheet.write('AE1', 'avg_wait_time_const')
	worksheet.write('AF1', 'avg_wait_time')
	worksheet.write('AG1', 'avg_checkout_group_len_const')
	worksheet.write('AH1', 'avg_checkout_group_len')
	worksheet.write('AI1', 'avg_checkout_time_const')
	worksheet.write('AJ1', 'avg_checkout_time')
	worksheet.write('AK1', 'checkout_group_count')
	worksheet.write('AL1', 'checkout_group')
	worksheet.write('AM1', 'checkout_multi_group_const')
	worksheet.write('AN1', 'checkout_multi_group')
	logstring = []
	keyword = 'metadata'
	# if there are additional info added to the metadata that is not wanted in the data processing, add it to the ignor list
	ignore_list = 'report15'
	row = 1

	num = 0
	for file in sorted_logs:
		if os.path.isfile(os.path.join(pathlogs, file)):
			f = open(os.path.join(pathlogs, file), 'r', errors='ignore')
			for x in f:
				if keyword in x:
					if ignore_list in x:
						continue
					x1 = json.loads(x)
					if 'enter' in x1['metadata'].keys() or 'time_index' in x1['metadata'].keys():
						continue
					people_queue_info = x1['metadata']['queues']
					people_queue_time = x1['ended_at']
					lane_num = len(x1['camera_ids'])
					people_queue_name = people_queue_info[0]["queue_name"]
					utc_date1 = datetime.datetime.strptime(str(people_queue_time), "%Y-%m-%dT%H:%M:%fZ")
					guardian_real_time = str(utc_date1 - datetime.timedelta(hours=time_hour_diff))
					guardian_real_time_1 = guardian_real_time.split('.')[0]+ str(',623')
					add_str = " [INFO ][39214][ThreadPoolExecutor-1_0][people_queuing_context:flow_generate_event_without_recording:469] camera 6 --[People Queuing] mid - metadata: {'queues':"
					guardian_reformat = guardian_real_time_1 + add_str + str(people_queue_info) + "]}, event-type: people_queuing:118"
					if guardian_reformat[0] == '.':
						y = guardian_reformat[15:]
					else:
						y = guardian_reformat
					z = y.split(',')
					y = y.replace('[', '')
					y = y.replace(']', '')
					z = re.split(",| |:|'|[|]|--|{|}|[|]", y)

					while ("" in z):
						z.remove("")
					logstring.append(z)
					col = 0
					for entries in z:
						worksheet.write(row, col, entries)
						col += 1
					row += 1
			f.close
	workbook.close()
	guardian_logs_location = os.path.join(os.path.dirname(__file__), 'guardian_logs.xlsx')
	return guardian_logs_location, lane_num

def time_convert(minutes):
	if minutes >= 60 * 24:
		minutes -= 60 * 24
	hours = int(minutes / 60)
	hour_minutes = minutes % 60
	str_hours = str(hours)
	str_minutes = str(hour_minutes)
	if hour_minutes < 10 and hour_minutes > 0:
		str_minutes = '0' + str_minutes
	if hours <= 9:
		str_hours = '0' + str_hours
	if hour_minutes == 0:
		str_minutes = '0' + str_minutes
	time = str_hours + ':' + str_minutes
	return time

def creat_standard_excel(guardian_logs, date, cam_num_guardian, cam_num_people):
	workbook = xlsxwriter.Workbook('{}.xlsx'.format(date))
	worksheet = workbook.add_worksheet()
	worksheet.write('B1', 'Date')
	worksheet.write('C1', 'Time')
	worksheet.write('J1', 'Queue open')
	worksheet.write('K1', 'AVG_NUM')
	worksheet.write('L1', 'lane1 status')
	worksheet.write('M1', 'lane1 avg group length')
	worksheet.write('N1', 'lane1 avg queue time')
	worksheet.write('O1', 'lane1 checkout number')
	worksheet.write('P1', 'lane2 status')
	worksheet.write('Q1', 'lane2 avg group length')
	worksheet.write('R1', 'lane2 avg queue time')
	worksheet.write('S1', 'lane2 checkout number')
	worksheet.write('T1', 'lane3 status')
	worksheet.write('U1', 'lane3 avg group length')
	worksheet.write('V1', 'lane3 avg queue time')
	worksheet.write('W1', 'lane3 checkout number')
	worksheet.write('X1', 'lane4 status')
	worksheet.write('Y1', 'lane4 avg group length')
	worksheet.write('Z1', 'lane4 avg queue time')
	worksheet.write('AA1', 'lane4 checkout number')
	worksheet.write('AB1', 'lane5 status')
	worksheet.write('AC1', 'lane5 avg group length')
	worksheet.write('AD1', 'lane5 avg queue time')
	worksheet.write('AE1', 'lane5 checkout number')
	worksheet.write('AF1', 'lane6 status')
	worksheet.write('AG1', 'lane6 avg group length')
	worksheet.write('AH1', 'lane6 avg queue time')
	worksheet.write('AI1', 'lane6 checkout number')
	worksheet.write('AJ1', 'lane7 status')
	worksheet.write('AK1', 'lane7 avg group length')
	worksheet.write('AL1', 'lane7 avg queue time')
	worksheet.write('AM1', 'lane7 checkout number')
	worksheet.write('AN1', 'SCO avg group length')
	worksheet.write('AO1', 'SCO avg queue time')
	worksheet.write('AP1', 'SCO checkout number')
	worksheet.write('AQ1', 'enter0')
	worksheet.write('AR1', 'exit0')
	worksheet.write('AS1', 'enter1')
	worksheet.write('AT1', 'exit1')
	worksheet.write('AU1', 'enter2')
	worksheet.write('AV1', 'exit2')
	minutes = 0
	top_minutes = 3
	l = 0
	row = 1
	i = 96*5
	l += 96*5
	while i > 0:
		worksheet.write_string(row, 1, date)
		time_str = time_convert(minutes)
		time_str_top = time_convert(top_minutes)
		time = time_str + '-' + time_str_top
		worksheet.write_string(row, 2, time)
		minutes += 3
		top_minutes += 3
		row += 1
		i -= 1

	workbook.close()
	# dirname = os.path.dirname(__file__)
	step2location = os.path.join(os.path.dirname(__file__), '{}.xlsx'.format(date))
	return step2location

def import_enter_leave_info(FinalReport, path40in, path40out,\
							path41in, path41out, path42in, path42out, date):
	df = pd.read_excel(FinalReport)
	return df

def findrow(date, guardian_date, hour, minutes):
	result = 0
	if date == guardian_date:
		result += max(hour * 4*5 + (minutes) / 3 - 1, 0)
		Flag = True

	elif int(guardian_date.split('-')[2]) - 1 == int(date.split('-')[2]) and hour == 0 and minutes == 0 :
		hour = 24
		result += max(hour * 4*5 + (minutes) / 3 - 1, 0)
		Flag = True
	else:
		Flag = False
	return result, Flag

def import_queuing_info(df, guardian_logs_location, people_logs_location, date, people_camera_ids):
	# read guardian data
	#df.reset_index(inplace=True)
	df2 = pd.read_excel(guardian_logs_location)
	df3 = pd.read_excel(people_logs_location)
	df.reset_index(drop=True)
	# now go through the raw data and transfer
	for index, row in df3.iterrows():
		# use function to get the row output
		row_out, Flag = findrow(date, row['date'], row['hour'], row['minutes'])
		if not Flag:
			continue
		if row_out == 0.0 or (row_out < 10 and (row['enter'] > 500 or row['exit'] > 500)):
			row['enter'] = 0
			row['exit'] = 0

		if len(people_camera_ids) == 3:
			if row["camera_id"] == people_camera_ids[0]:
				df.at[row_out, 'enter0'] = row['enter']
				df.at[row_out, 'exit0'] = row['exit']
			elif row["camera_id"] == people_camera_ids[1]:
				df.at[row_out, 'enter1'] = row['enter']
				df.at[row_out, 'exit1'] = row['exit']
			else:
				df.at[row_out, 'enter2'] = row['enter']
				df.at[row_out, 'exit2'] = row['exit']
		elif len(people_camera_ids) == 2:
			if row["camera_id"] == people_camera_ids[0]:
				df.at[row_out, 'enter0'] = row['enter']
				df.at[row_out, 'exit0'] = row['exit']
			elif row["camera_id"] == people_camera_ids[1]:
				df.at[row_out, 'enter1'] = row['enter']
				df.at[row_out, 'exit1'] = row['exit']

	for i in range(0, 3):
		index_1 = 0
		index_2 = 0
		for k in df["enter{}".format(i)].values:
			if pd.isna(k):
				df.at[index_1, "enter{}".format(i)] = 0
			index_1 += 1

		for j in df["exit{}".format(i)].values:
			if pd.isna(j):
				df.at[index_2, "exit{}".format(i)] = 0
			index_2 += 1
	num = 0


	for index, row in df2.iterrows():
		# use function to get the row output
		row_out, Flag = findrow(date, row['date'], row['hour'], row['minutes'])
		if not Flag:
			continue
		#if row['queue_status'] == 'on' and float(row['avg_group_len']) > 0.1 and float(row['avg_queue_time']) > 0:
		if row['queue_status'] == 'on':
			que_sta = 1
		else:
			que_sta = 0
		if row['queue_name'] == '1':
			df.at[row_out, 'lane1 status'] = que_sta
			df.at[row_out, 'lane1 avg group length'] = row['avg_group_len']
			df.at[row_out, 'lane1 avg queue time'] = row['avg_queue_time']
			df.at[row_out, 'lane1 checkout number'] = row['checkout_multi_group']
		elif row['queue_name'] == '2':
			df.at[row_out, 'lane2 status'] = que_sta
			df.at[row_out, 'lane2 avg group length'] = row['avg_group_len']
			df.at[row_out, 'lane2 avg queue time'] = row['avg_queue_time']
			df.at[row_out, 'lane2 checkout number'] = row['checkout_multi_group']
		elif row['queue_name'] == '3':
			df.at[row_out, 'lane3 status'] = que_sta
			df.at[row_out, 'lane3 avg group length'] = row['avg_group_len']
			df.at[row_out, 'lane3 avg queue time'] = row['avg_queue_time']
			df.at[row_out, 'lane3 checkout number'] = row['checkout_multi_group']
		elif row['queue_name'] == '4':
			df.at[row_out, 'lane4 status'] = que_sta
			df.at[row_out, 'lane4 avg group length'] = row['avg_group_len']
			df.at[row_out, 'lane4 avg queue time'] = row['avg_queue_time']
			df.at[row_out, 'lane4 checkout number'] = row['checkout_multi_group']
		elif row['queue_name'] == '5':
			df.at[row_out, 'lane5 status'] = que_sta
			df.at[row_out, 'lane5 avg group length'] = row['avg_group_len']
			df.at[row_out, 'lane5 avg queue time'] = row['avg_queue_time']
			df.at[row_out, 'lane5 checkout number'] = row['checkout_multi_group']
		elif row['queue_name'] == '6':
			df.at[row_out, 'lane6 status'] = que_sta
			df.at[row_out, 'lane6 avg group length'] = row['avg_group_len']
			df.at[row_out, 'lane6 avg queue time'] = row['avg_queue_time']
			df.at[row_out, 'lane6 checkout number'] = row['checkout_multi_group']
		elif row['queue_name'] == '7':
			df.at[row_out, 'lane7 status'] = que_sta
			df.at[row_out, 'lane7 avg group length'] = row['avg_group_len']
			df.at[row_out, 'lane7 avg queue time'] = row['avg_queue_time']
			df.at[row_out, 'lane7 checkout number'] = row['checkout_multi_group']
		elif row['queue_name'] == "SCO":
			df.at[row_out, 'SCO avg group length'] = row['avg_group_len']
			df.at[row_out, 'SCO avg queue time'] = row['avg_queue_time']
			df.at[row_out, 'SCO checkout number'] = row['checkout_multi_group']

	for i in range(1, 8):
		index_1 = 0
		index_2 = 0
		for k in df["lane{} avg group length".format(i)].values:
			if pd.isna(k):
				df.at[index_1, "lane{} avg group length".format(i)] = 0
			index_1 += 1

		for j in df["lane{} status".format(i)].values:
			if pd.isna(j):
				df.at[index_2, "lane{} status".format(i)] = 0
			index_2 += 1

	lane_open_lsit = [df["lane{} status".format(l)] for l in range(1, 8) if str(df["lane{} status".format(l)]) != 'nan']
	avg_num_lsit = [df["lane{} avg group length".format(l)] if str(df["lane{} avg group length".format(l)]) != 'nan' else 0.0 for l in range(1, 8) ]
	df['Queue open'] = sum(lane_open_lsit)
	df['AVG_NUM'] = sum(avg_num_lsit)
	df = df.drop(['Unnamed: 0'], axis=1)
	df = df.drop(['Unnamed: 3'], axis=1)
	df = df.drop(['Unnamed: 4'], axis=1)
	df = df.drop(['Unnamed: 5'], axis=1)
	df = df.drop(['Unnamed: 6'], axis=1)
	df = df.drop(['Unnamed: 7'], axis=1)
	df = df.drop(['Unnamed: 8'], axis=1)
	df.to_csv("{}.csv".format(date), index=False)
	# del  delete redundant files
	os.remove(os.path.join(os.path.dirname(__file__), '{}.xlsx'.format(date)))

def main():
	global time_hour_diff
	time_hour_diff = 5
	print('Process the guardian data info is start')
	# guardian log's location
	guardian_logs = os.path.join(os.path.dirname(__file__), 'surge_data_process', '2411')

	logs = os.listdir(guardian_logs)
	dates = ['2022-12-03','2022-12-04']
	# 1：Sort guardian log files by time

	# step 1: get the init guardian info data
	guardian_logs_location, num_cam_guardian = record_guardian_info(logs, guardian_logs)
	people_logs_location, num_cam_people_count, camera_ids = record_people_count_info(logs, guardian_logs)
	print('Process the guardian data info, step1 is over')
	for date in dates:
		print("Process {}'s guardian info".format(date))
		# step 2: create the standard excel.
		standard_excel_location = creat_standard_excel(guardian_logs, date, num_cam_guardian, num_cam_people_count)
		print('Process the guardian data info, step2 is over')

		# step 3: import the enter and leave message.
		Report_Info = pd.read_excel(standard_excel_location)
		print('Process the guardian data info, step3 is over')

		# step 4: import the queueing info.
		import_queuing_info(Report_Info, guardian_logs_location, people_logs_location, date, camera_ids)
		print('Process the guardian data info, step4 is over')

if __name__ == '__main__':
	main()
