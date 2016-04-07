import json
import os
import numpy as np 
import pandas as pd
import collections as col

import pymysql.cursors

from Stage1_JSON import Stage1_Json_TimeUserTag, Stage1_Json_ExtraInfor
from Stage1_Universal import UTO_update
from Stage1_MySQL import Stage1_MySQL_TweetOBJ

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
# check each line from readline(), check length, start/end character
@check_args(str, int)
def Json_format_check(input_str, index_line):
	# check tweet_line, which should at least be "{}"
	fflag_line = True
	if len(input_str) <= 2:
		fflag_line = False
	# check if tweet_line is complete
	fflag_json = False
	if fflag_line and input_str[0:1] == '{':
		if input_str[-2:-1] == '}' or input_str[-1:] == '}': # last line has no '\n'
			fflag_json = True
	else:
		print "Line: {}, Incomplete or Empty Line".format(index_line)
	return fflag_line, fflag_json

"""
####################################################################
"""

def Stage1_Main(file_name, keyword, MySQL_DBkey):

	####################################################################
	# read tweets.txt file data
	InputfileDir = os.path.dirname(os.path.realpath('__file__'))
	print InputfileDir
	data_file_name =  '../Data/' + file_name
	Inputfilename = os.path.join(InputfileDir, data_file_name) # ../ get back to upper level
	Inputfilename = os.path.abspath(os.path.realpath(Inputfilename))
	print Inputfilename
	file_input = open(Inputfilename,'r')

	####################################################################

	# Connect to the database
	connection = pymysql.connect(host=MySQL_DBkey['host'],
								 user=MySQL_DBkey['user'],
								 password=MySQL_DBkey['password'],
								 db=MySQL_DBkey['db'],
								 charset=MySQL_DBkey['charset'],
								 cursorclass=pymysql.cursors.DictCursor)
	
	####################################################################
	
	# main data structre, in memory
	# keyword_tags, related tags, user_key, user_upper, 
	# key = different class
	Tag_Related_OBJ = col.defaultdict(col.Counter)
	# key = tag
	Tag_Related_OBJ['tag_keyword'] = col.Counter() # val = N_call
	Tag_Related_OBJ['tag_relevant'] = col.Counter() # val = score
	Tag_Related_OBJ['tag_relevant_N'] = col.Counter() # val = N_call
	Tag_Related_OBJ['user'] = col.Counter() # key = id_str, val = score
	Tag_Related_OBJ['user_N'] = col.Counter() # key = id_str, val = N_act

	# main data structure, for each tweet
	Universal_Tweet_OBJ = col.defaultdict(set)
	# initialize
	Universal_Tweet_OBJ['tweet_time'] = set([])
	Universal_Tweet_OBJ['tweet_id'] = set([])
	Universal_Tweet_OBJ['user_id'] = set([])
	Universal_Tweet_OBJ['user_name'] = set([])
	Universal_Tweet_OBJ['user_followers'] = set([])
	Universal_Tweet_OBJ['user_friends'] = set([])
	Universal_Tweet_OBJ['user_favourites'] = set([])
	Universal_Tweet_OBJ['user_born'] = set([])
	Universal_Tweet_OBJ['Tag_Keyword'] = set([])
	Universal_Tweet_OBJ['Tag_Relevent'] = set([])
	Universal_Tweet_OBJ['Tag_due_user'] = set([])
	Universal_Tweet_OBJ['text'] = set([])
	Universal_Tweet_OBJ['reply_to_userID'] = set([])
	Universal_Tweet_OBJ['mentioned_userID'] = set([])

	####################################################################
	# main logic structure, controled by readline() returns, exist at end of file
	flag_fileEnd = False
	count_emptyline = 0
	count_line = 0
	# this is the logic loop for EACH tweet
	while (flag_fileEnd == False):
		count_line += 1 
		tweet_line = file_input.readline()
		
		# json format check and file end check and data type check
		flag_line = False
		flag_json = False
		try:
			flag_line, flag_json = Json_format_check(tweet_line, count_line)
		except AssertionError:
			print "Line: {}, Json_format_check() data type check failed".format(index_line)
			pass 
		else:
			pass
		# count adjacent empty lines
		if flag_line == True:
			count_emptyline = 0
		else:
			count_emptyline += 1
		# flag end of file
		if count_emptyline > 4:
			flag_fileEnd = True

		####################################################################
		# Stage1, json load, extract time and tag information
		# data type already checked
		flag_TimeUser = False # has valid tweet time and user infor
		flag_Tag_Detail = False # if true, then need to extract more information

		# json load and initial processing
		if flag_json == True:
			flag_TimeUser, flag_Tag_Detail, Universal_Tweet_OBJ = Stage1_Json_TimeUserTag(input_str = tweet_line, 
				index_line = count_line, keyword = keyword, Tag_Related_OBJ = Tag_Related_OBJ)

		####################################################################
		# update UTO, this will include a lot more low-tier users and tags
		if flag_TimeUser == True:
			Tag_Related_OBJ = UTO_update(Tag_Related_OBJ = Tag_Related_OBJ, Universal_Tweet_OBJ = Universal_Tweet_OBJ)

		####################################################################
		# if need to update Tag_Related_OBJ and extract more information
		if flag_TimeUser == True and flag_Tag_Detail == True: 
			# second need to extract more information INTO Universal_Tweet_OBJ
			Universal_Tweet_OBJ = Stage1_Json_ExtraInfor(
				input_str = tweet_line, index_line = count_line, Universal_Tweet_OBJ = Universal_Tweet_OBJ)

		####################################################################
		# upload to MySQL DB
		# Threshold: high-tier tag: >= 5; high-tier user: >= 1; drop tag < 1; drop user < 0.1
		if flag_TimeUser == True:
			try:
				Stage1_MySQL_TweetOBJ(connection = connection, MySQL_DBkey = MySQL_DBkey,
				Tag_Related_OBJ = Tag_Related_OBJ, Universal_Tweet_OBJ = Universal_Tweet_OBJ, flag_Detail = flag_Tag_Detail)
			finally:
				pass

	####################################################################
	# clear file objs
	connection.close()
	file_input.close()

"""
####################################################################
"""

#def Stage1_Gross(file_name, keyword, MySQL_DBkey):

	#file_name = 'US_tweets_Feb15_PT.txt'
	#keyword = 'hillary'
	#MySQL_DBkey = {'host':'localhost', 'user':'sa', 'password':'fanyu01', 'db':'ultra_v3_feb15vh','charset':'utf8mb4'}

	#Stage1_Main(file_name = file_name, keyword = keyword, MySQL_DBkey = MySQL_DBkey)


"""
####################################################################
# test code for Stage1 main
"""

if __name__ == "__main__":

	file_name = 'US_tweets_Feb15_PT.txt'
	keyword = 'hillary'
	MySQL_DBkey = {'host':'localhost', 'user':'', 'password':'', 'db':'','charset':'utf8mb4'}

	Stage1_Main(file_name = file_name, keyword = keyword, MySQL_DBkey = MySQL_DBkey)

# 130K tweets in 2461 seconds
# Tracking tags:  732
# Tracking users:  341
