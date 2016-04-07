import json
import os
import numpy as np 
import pandas as pd
import collections as col

import pymysql.cursors

from Stage2_DBprep import Stage2_1_TCHprep
from Stage2_1_FitaveScore import Lfit_Intercept


####################################################################
# variable type check
def check_args(*types):
	def real_decorator(func):
		def wrapper(*args, **kwargs):
			for val, typ in zip(args, types):
				assert isinstance(val, typ), "Value {} is not of expected type {}".format(val, typ)
			return func(*args, **kwargs)
		return wrapper
	return real_decorator

####################################################################

"""
####################################################################
"""

# select out tags that: total_call >= 10 and total_call != tagNcall
# update DB2.tagcallhistory, add MaxScore, Score_exp, time_pin
# check if there is "jump"; mark low-tier if yes
def Stage2_1_Step2(connection, MySQL_DBkey1, MySQL_DBkey2):

	####################################################################
	# select out tags that: total_call >= 10 and total_call != tagNcall
	lowTier_candidate = [] # store key of candidates
	lowTier_tag_set = set([]) # store key of confirmed low-tier tags
	# command
	db2_tagUnique = MySQL_DBkey2['db'] + '.tag_unique'
	comd_loeTier_select = "\
select tagText from "+db2_tagUnique+"\n\
WHERE total_call >= 10 and total_call != tagNcall;\n"
	# execute command
	try:
		with connection.cursor() as cursor:
			cursor.execute(comd_loeTier_select)
			result = cursor.fetchall()
			# result is a list of dicts: {u'tagText': u'100yearsold'}
			for item in result:
				lowTier_candidate.append(item['tagText'])
				print item['tagText']
	finally:
		pass	

	####################################################################
	# update DB2.tagcallhistory, add MaxScore, Score_exp, time_pin
	for tagText in lowTier_candidate:
		# using pre-defined functions...
		Stage2_1_TCHprep(connection, tagText, MySQL_DBkey1, MySQL_DBkey2)

	####################################################################
	# check if there is "jump"; mark low-tier if yes
	# criteria: score >= 4 or == 9, with tagNcall = 1 and TCH_ID > tagNcall, prev score < 2
	
	# universal data obj; key = tagText
	lowTier_Data = col.defaultdict(np.array) # stores 3 columns of data
	lowTier_pin = col.defaultdict(list) # stores pins for ave calculation
	# loop through all tagcallhistory tables
	for tagText in lowTier_candidate:
		# need to extract 3 columns: TCH_ID (int), tagScore(float), tagNcall(int)
		temp_data = [[],[],[]]
		# command
		db2_tagcallhistory = MySQL_DBkey2['db'] + '.tagcallhistory_' + tagText
		comd_selectData = "\
SELECT TCH_ID, tagScore, tagNCall \n\
FROM "+db2_tagcallhistory +"\n\
ORDER BY callTime ASC;\n"
		# execute command
		try:
			with connection.cursor() as cursor:
				cursor.execute(comd_selectData)
				result = cursor.fetchall()
				# result is a list of dicts: {u'tagText': u'100yearsold'}
				for item in result:
					temp_data[0].append(item['TCH_ID'])
					temp_data[1].append(item['tagScore'])
					temp_data[2].append(item['tagNCall'])
		finally:
			pass
		# load into lowTier_Data; 1 for score
		lowTier_Data[tagText] = np.array(temp_data)
		lowTier_pin[tagText] = [0] # include the first one...
	
	# end of loading all candidate data into lowTier_Data
	# check criteria
	for key in lowTier_Data:
		# extract all the pins, not just the first
		for pin in range(len(lowTier_Data[key][1])):
			# 1st, val >= 4.0
			if (lowTier_Data[key][1,pin] >= 4.0) and (lowTier_Data[key][2,pin] == 1) and (lowTier_Data[key][0,pin] > lowTier_Data[key][2,pin]+5) and (lowTier_Data[key][1,pin-3] < 2.0):
				print "found low-tier tag: ", key, "position: ", pin
				lowTier_tag_set.add(key)
				lowTier_pin[key].append(pin)
		# if there is pin, add the last index
		if len(lowTier_pin[key])> 0:
			lowTier_pin[key].append(len(lowTier_Data[key][1])-1)

	# end of finding lowTier_tags
	####################################################################
	# calculate ave_Score; get keys of lowTier Tag
	lowTier_tag_aveScore = col.defaultdict(dict)
	for key in lowTier_tag_set:
		# temp holder
		ave_Score_list = []
		for i in range(len(lowTier_pin[key])-1):
			# data set
			start_pin = lowTier_pin[key][i]
			end_pin = lowTier_pin[key][i+1]
			Yo = lowTier_Data[key][1,start_pin:end_pin]
			Xo = lowTier_Data[key][2,start_pin:end_pin]
			# convert to 1/x and linear fit for intercept, which is ave_score
			# pass variable (key, i) for graph naming
			print "tag: ", key
			print "data range: {}-{}".format(start_pin, end_pin)
			if (end_pin - start_pin > 30):
				ave_Score_list.append(Lfit_Intercept(tagText=key, index=i, Xo=Xo[15:], Yo=Yo[15:]))
		# calculate Ave of ave_ScoreS
		if len(ave_Score_list) > 0:
			ave_Score_list = np.array(ave_Score_list)
			lowTier_tag_aveScore[key] = {}
			lowTier_tag_aveScore[key]['ave_score'] = np.average(ave_Score_list)
			lowTier_tag_aveScore[key]['ave_score_std'] = np.std(ave_Score_list)

	# end of calculating ave_score
	####################################################################
	# upload lowTier_tag marker, ave_score, ave_score_std
	for tag in lowTier_tag_aveScore:
		# command
		db2_tagunique = MySQL_DBkey2['db'] + '.tag_unique'
		comd_updateTU_ave = "\
UPDATE "+db2_tagunique+"\n\
SET\n\
tier_type = 'lowTier',\n\
ave_score = "+str(lowTier_tag_aveScore[tag]['ave_score'])+",\n\
ave_score_std = "+str(lowTier_tag_aveScore[tag]['ave_score_std'])+"\n\
WHERE tagText = '"+tag+"';\n"

		# execute commands
		try:
			with connection.cursor() as cursor:
				cursor.execute(comd_updateTU_ave)
			# commit commands
			print "update :", tag
			connection.commit()
		finally:
			pass

	####################################################################
	# return 

"""
####################################################################
"""






"""
####################################################################
# test code
"""

if __name__ == "__main__":

	MySQL_DBkey1 = {'host':'localhost', 'user':'sa', 'password':'fanyu01', 'db':'ultra_v3_feb15v2','charset':'utf8mb4'}
	MySQL_DBkey2 = {'host':'localhost', 'user':'sa', 'password':'fanyu01', 'db':'stage2_test','charset':'utf8mb4'}

	####################################################################

	# Connect to the database
	connection = pymysql.connect(host=MySQL_DBkey2['host'],
								 user=MySQL_DBkey2['user'],
								 password=MySQL_DBkey2['password'],
								 db=MySQL_DBkey2['db'],
								 charset=MySQL_DBkey2['charset'],
								 cursorclass=pymysql.cursors.DictCursor)

	####################################################################

	# Stage2_1_Step2(connection, MySQL_DBkey1, MySQL_DBkey2)



