import json
import os
import numpy as np 
import pandas as pd
import collections as col

import pymysql.cursors

"""
####################################################################
"""

def Stage2_DB_tableMove(tableName, connection, MySQL_DBkey1, MySQL_DBkey2):

	# command
	db2_table = MySQL_DBkey2['db'] + '.' + tableName
	db1_table = MySQL_DBkey1['db'] + '.' + tableName
	comd_tableMove = "\
CREATE TABLE "+db2_table+" LIKE "+db1_table+";\n\
INSERT INTO "+db2_table+" SELECT * FROM "+db1_table+";\n"
	
	# execute commands
	try:
		with connection.cursor() as cursor:
			cursor.execute(comd_tableMove)
		# commit commands
		print "Shift table: ", db1_table, " to : ", db2_table
		connection.commit()
	finally:
		pass

"""
####################################################################
"""

def Stage2_totalN_recal(connection, MySQL_DBkey1, MySQL_DBkey2):

	###############################################
	# get the list of tag_text
	# command
	db2_tagUnique = MySQL_DBkey2['db'] + '.tag_unique'
	comd_getTagText = "\
SELECT tagText FROM " + db2_tagUnique + ";\n"

	# execute command
	try:
		with connection.cursor() as cursor:
			cursor.execute(comd_getTagText)
			result = cursor.fetchall()
			# result is a list of dicts: {u'tagText': u'100yearsold'}
			full_tag_list = []
			for item in result:
				full_tag_list.append(item)
				# print item['tagText']
	finally:
		pass

	###############################################
	# get cout(*) of each tagcallhistory table, from db1, 
	for entry in full_tag_list:
		# command: cout(*)
		db1_TCH = MySQL_DBkey1['db']+'.'+'tagcallhistory_'+entry['tagText']
		TCH_name = 'tagcallhistory_'+entry['tagText']
		comd_count = "\
SELECT IF( (SELECT count(*) FROM information_schema.tables\n\
WHERE table_schema = '"+MySQL_DBkey1['db']+"' AND table_name = '"+TCH_name+"'), \n\
(SELECT count(*) FROM "+db1_TCH+"), \n\
0);\n"
		try:
			with connection.cursor() as cursor:					
				# load into curosr
				cursor.execute(comd_count)
				# execute command
				result = cursor.fetchall()
				# result is a single-entry list of a single-key dict, with command as key....
			for key in result[0]:
				entry['totalN'] = result[0][key]
			print entry['tagText'], entry['totalN']
		finally:
			pass

	###############################################
	# command: insert new column into db2.tag_unique

	comd_insertColumn = "\
ALTER TABLE "+db2_tagUnique+"\n\
ADD COLUMN total_call int not null;\n"

	# execute commands
	try:
		with connection.cursor() as cursor:	
			cursor.execute(comd_insertColumn)
		# commit commands
		connection.commit()
	finally:
		pass

	###############################################
	# command: update db2.tag_unique using full_tag_list
	try:
		with connection.cursor() as cursor:	
			for entry in full_tag_list:
				# command
				comd_updateTotalN = "\
UPDATE "+db2_tagUnique+"\n\
SET total_call = "+str(entry['totalN'])+"\n\
WHERE tagText = '"+entry['tagText']+"';\n"
				# load to cursor
				cursor.execute(comd_updateTotalN)
		# commit commands
		connection.commit()
	finally:
		pass

	###############################################
	# select tags with totalN > 5, using full_tag_list
	# command: transfer tagcallhistory tables of selected tags to db2
	try:
		with connection.cursor() as cursor:	
			for entry in full_tag_list:
				if entry['totalN'] >= 5:
					db2_table = MySQL_DBkey2['db'] + '.tagcallhistory_' + entry['tagText']
					db1_table = MySQL_DBkey1['db'] + '.tagcallhistory_' + entry['tagText']
					# command table shift
					comd_tableMove = "\
		CREATE TABLE "+db2_table+" LIKE "+db1_table+";\n\
		INSERT INTO "+db2_table+" SELECT * FROM "+db1_table+";\n"
					# load into cursor
					cursor.execute(comd_tableMove)

		# commit commands
		print "Shift selected tagcallhistory tables"
		connection.commit()
	finally:
		pass

"""
####################################################################
"""
# update DB2.tag_unique, add tier_type, cluster_mark, ave_score, ave_score_std
def Stage2_1_TUprep(connection, MySQL_DBkey1, MySQL_DBkey2):
	
	###############################################
	# add columns to tag_unique
	# tier_type, cluster_mark, ave_score, ave_score_std
	db2_tagUnique = MySQL_DBkey2['db'] + '.tag_unique'
	comd_add_columns = "\
ALTER TABLE "+db2_tagUnique+"\n\
ADD COLUMN tier_type varchar(64) default 'unclear',\n\
ADD COLUMN cluster_mark int,\n\
ADD COLUMN ave_score float,\n\
ADD COLUMN ave_score_std float;\n"

	# execute commands
	try:
		with connection.cursor() as cursor:
			cursor.execute(comd_add_columns)
		# commit commands
		print "update :", db2_tagUnique
		connection.commit()
	finally:
		pass

"""
####################################################################
"""

# update DB2.tagcallhistory, add MaxScore, Score_exp, time_pin
# this function will be called when one starts to calculate tier-level or score_exp
def Stage2_1_TCHprep(connection, tagText, MySQL_DBkey1, MySQL_DBkey2):
	# MaxScore, Score_exp, time_pin
	db2_tagcallhistory = MySQL_DBkey2['db'] + '.tagcallhistory_' + tagText
	comd_add_columns = "\
ALTER TABLE "+db2_tagcallhistory+"\n\
ADD COLUMN MaxScore float,\n\
ADD COLUMN Score_exp float,\n\
ADD COLUMN time_pin int;\n"

	# execute commands
	try:
		with connection.cursor() as cursor:
			cursor.execute(comd_add_columns)
		# commit commands
		print "update :", db2_tagcallhistory
		connection.commit()
	finally:
		pass

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


