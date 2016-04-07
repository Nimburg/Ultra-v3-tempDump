

	####################################################################
	# Part 2_1
	# try classify score(time) using sciphy.signal local maximum
	# get Ave_Score of all classes
	# establish first method of classification: curve type and Ave_Score; 2D classification
	# 
	# Note that: those low-tier tags are easy to get separated by fitting y = ave_y + (ave_y - shock_y)/N
	# 
	# (Note: correct formula: Y_observe = Beta/(X + Alpha) + Y_ave; Beta took the shock, Alpha as the shift
	# on X-axis)
	# 
	# using 1/N as linear fit
	# Note that: this fit is also marginally useful for representing mid and high tier tags in ave_y vs shock_y system; 
	# though not accurate
	# 


"""
####################################################################
"""

import json
import os
import numpy as np 
import pandas as pd
import collections as col

import pymysql.cursors

from Stage2_DBprep import Stage2_DB_tableMove, Stage2_totalN_recal, Stage2_1_TUprep
from Stage2_1_General import Stage2_1_Step2

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
# tagcallhistry talbe transfer threshold at score >= 5
# update DB.tables
# recalculate total_call
def Stage2_Main_Part1(MySQL_DBkey1, MySQL_DBkey2):

	####################################################################

	# Connect to the database
	connection = pymysql.connect(host=MySQL_DBkey2['host'],
								 user=MySQL_DBkey2['user'],
								 password=MySQL_DBkey2['password'],
								 db=MySQL_DBkey2['db'],
								 charset=MySQL_DBkey2['charset'],
								 cursorclass=pymysql.cursors.DictCursor)
	
	####################################################################
	# Part 1
	# moving necessary tables to DB: Stage2_test
	table_shift_list1 = ['User_Unique', 'Tweet_Stack', 'tag_Unique']
	for item in table_shift_list1:
		Stage2_DB_tableMove(tableName=item, connection=connection, 
			MySQL_DBkey1=MySQL_DBkey1, MySQL_DBkey2=MySQL_DBkey2)

	# recalculate Total_Call for tags
	# select and transfer TagCallHistory tables
	Stage2_totalN_recal(connection=connection, MySQL_DBkey1=MySQL_DBkey1, MySQL_DBkey2=MySQL_DBkey2)

	####################################################################
	# clear file objs
	connection.close()

"""
####################################################################
"""

# update DB to increase flags and type-columns
# select out low-tier tags
# calculate MaxScore and Score_exp

def Stage2_Main_Part2(MySQL_DBkey1, MySQL_DBkey2):

	####################################################################

	# Connect to the database
	connection = pymysql.connect(host=MySQL_DBkey2['host'],
								 user=MySQL_DBkey2['user'],
								 password=MySQL_DBkey2['password'],
								 db=MySQL_DBkey2['db'],
								 charset=MySQL_DBkey2['charset'],
								 cursorclass=pymysql.cursors.DictCursor)

	####################################################################
	# Step1
	# update DB2.tag_unique, add tier_type, cluster_mark, ave_score, ave_score_std
	Stage2_1_TUprep(connection = connection, MySQL_DBkey1 = MySQL_DBkey1, MySQL_DBkey2 = MySQL_DBkey2)

	####################################################################
	# Step2
	# select out tags that: total_call >= 10 and total_call != tagNcall
	# update DB2.tagcallhistory, add MaxScore, Score_exp, time_pin
	# check if there is "jump"; mark low-tier if yes
	# perform fitting to extract ave_score and ave_score_std
	lowTier_tag_list = Stage2_1_Step2(connection = connection, MySQL_DBkey1 = MySQL_DBkey1, MySQL_DBkey2 = MySQL_DBkey2)

	####################################################################
	# Step4
	# select out tags that: total_call == tagNcall and total_call >= 10
	# update DB2.tagcallhistory, add MaxScore, Score_exp, time_pin

	####################################################################
	# clear file objs
	connection.close()

"""
####################################################################
"""

def Stage2_Gross(MySQL_DBkey1, MySQL_DBkey2):

	#MySQL_DBkey1 = {'host':'localhost', 'user':'sa', 'password':'fanyu01', 'db':'ultra_v3_feb15v2','charset':'utf8mb4'}
	#MySQL_DBkey2 = {'host':'localhost', 'user':'sa', 'password':'fanyu01', 'db':'stage2_test2','charset':'utf8mb4'}

	####################################################################
	# tagcallhistry talbe transfer threshold at score >= 5
	# update DB.tables
	# recalculate total_call
	Stage2_Main_Part1(MySQL_DBkey1 = MySQL_DBkey1, MySQL_DBkey2 = MySQL_DBkey2)

	####################################################################
	# update DB to increase flags and type-columns
	# select out low-tier tags
	# calculate MaxScore and Score_exp
	Stage2_Main_Part2(MySQL_DBkey1 = MySQL_DBkey1, MySQL_DBkey2 = MySQL_DBkey2)



"""
####################################################################
# test code for Stage1 main
"""

if __name__ == "__main__":

	MySQL_DBkey1 = {'host':'localhost', 'user':'', 'password':'', 'db':'','charset':'utf8mb4'}
	MySQL_DBkey2 = {'host':'localhost', 'user':'', 'password':'', 'db':'','charset':'utf8mb4'}

	####################################################################
	# tagcallhistry talbe transfer threshold at score >= 5
	# update DB.tables
	# recalculate total_call
	Stage2_Main_Part1(MySQL_DBkey1 = MySQL_DBkey1, MySQL_DBkey2 = MySQL_DBkey2)

	####################################################################
	# update DB to increase flags and type-columns
	# select out low-tier tags
	# calculate MaxScore and Score_exp
	Stage2_Main_Part2(MySQL_DBkey1 = MySQL_DBkey1, MySQL_DBkey2 = MySQL_DBkey2)


