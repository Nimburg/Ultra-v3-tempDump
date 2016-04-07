
	# 
	# ###################################################################
	# 
	# 
	# Note that: from current rolling ave_score, one could recalculate:
	# Score_N = 0.5*Max_Score + 0.5*Score_N-1 = 0.5*Max_Score + 0.5^2*MS_N-1 + 0.5^3*MS_N-2; 
	# thus this recal will be much more locally sensitive. One could use this for cov() matrix of mid and high tier tags
	# then again k-means 
	# 
	# Note that: both Score_ave and Score_exp for k-means are at end time; Both are actually Delta(Score_i, Score_j)
	# for Score_exp, the Delta(i,j) is Average over each Timespan Partition's Delta_t(i,j)
	# 
	# Note: Score_exp recalculation
	# at N, Score_ave_N = 1/N*(Score_ave_N-1 * N-1 + MaxScore_N)
	# => MaxScore_N = N*Score_ave_N - (N-1)*Score_ave_N-1
	# First calculate column(MaxScore), then calculate column(Score_exp)
	# 
	# Note that: Score_ave and Score_exp are NOT entirely independent
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

from Stage3_1_DBprep import Stage3_Part1_DBprep
from Stage3_General import Stage3_Part2_Step1Score_trump, Stage3_Part2_Step1Score_hillary



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
# create tables for DB_Stage3
# single out high and mid tier tags

def Stage3_Main_Part1(DB_Stage1_T, DB_Stage1_H, DB_Stage2_T, DB_Stage2_H, DB_Stage3):

	####################################################################

	# Connect to the database
	connection = pymysql.connect(host=DB_Stage3['host'],
								 user=DB_Stage3['user'],
								 password=DB_Stage3['password'],
								 db=DB_Stage3['db'],
								 charset=DB_Stage3['charset'],
								 cursorclass=pymysql.cursors.DictCursor)
	
	####################################################################

	# create tables for DB_Stage3
	Stage3_Part1_DBprep(connection=connection, MySQL_DBkey=DB_Stage3)
	connection.close()


"""
####################################################################
"""

# calculate 1-step score of low tier tags relative to high and mid tier tags
# create k-means fit with scores-2D and scores+Ncall-4D
# k-means of: 1-step-2D, 1-step+scores-4D

def Stage3_Main_Part2(DB_Stage3):

	# Connect to the database
	connection = pymysql.connect(host=DB_Stage3['host'],
								 user=DB_Stage3['user'],
								 password=DB_Stage3['password'],
								 db=DB_Stage3['db'],
								 charset=DB_Stage3['charset'],
								 cursorclass=pymysql.cursors.DictCursor)

	####################################################################
	# calculate 1-step score of low tier tags relative to high and mid tier tags

	Stage3_Part2_Step1Score_trump(connection=connection)

	Stage3_Part2_Step1Score_hillary(connection=connection)

	####################################################################
	# create k-means fit with scores-2D and scores+Ncall-4D


	connection.close()
	####################################################################

"""
####################################################################
"""

def Stage3_Gross(DB_Stage3):
	
	#DB_Stage3 = {'host':'localhost', 'user':'sa', 'password':'fanyu01', 'db':'ultra_stage3','charset':'utf8mb4'}


	####################################################################
	# create tables for DB_Stage3
	# single out high and mid tier tags

	Stage3_Main_Part1(DB_Stage1_T=DB_Stage1_T, DB_Stage1_H=DB_Stage1_H, 
		DB_Stage2_T=DB_Stage2_T, DB_Stage2_H=DB_Stage2_H, DB_Stage3=DB_Stage3)


	####################################################################
	# calculate 1-step score of low tier tags relative to high and mid tier tags
	# create k-means fit with scores-2D and scores+Ncall-4D
	# k-means of: 1-step-2D, 1-step+scores-4D, max(1-step, scores)-2D

	Stage3_Main_Part2(DB_Stage3=DB_Stage3)


"""
####################################################################
# test code for Stage1 main
"""

if __name__ == "__main__":

	DB_Stage1_T = {'host':'localhost', 'user':'sa', 'password':'fanyu01', 'db':'ultra_v3_feb15v2','charset':'utf8mb4'}
	DB_Stage1_H = {'host':'localhost', 'user':'sa', 'password':'fanyu01', 'db':'ultra_v3_feb15vh','charset':'utf8mb4'}
	DB_Stage2_T = {'host':'localhost', 'user':'sa', 'password':'fanyu01', 'db':'ultra_stage2_t','charset':'utf8mb4'}
	DB_Stage2_H = {'host':'localhost', 'user':'sa', 'password':'fanyu01', 'db':'ultra_stage2_h','charset':'utf8mb4'}
	DB_Stage3 = {'host':'localhost', 'user':'sa', 'password':'fanyu01', 'db':'ultra_stage3','charset':'utf8mb4'}


	####################################################################
	# create tables for DB_Stage3
	# single out high and mid tier tags

	Stage3_Main_Part1(DB_Stage1_T=DB_Stage1_T, DB_Stage1_H=DB_Stage1_H, 
		DB_Stage2_T=DB_Stage2_T, DB_Stage2_H=DB_Stage2_H, DB_Stage3=DB_Stage3)


	####################################################################
	# calculate 1-step score of low tier tags relative to high and mid tier tags
	# create k-means fit with scores-2D and scores+Ncall-4D
	# k-means of: 1-step-2D, 1-step+scores-4D, max(1-step, scores)-2D

	Stage3_Main_Part2(DB_Stage3=DB_Stage3)








