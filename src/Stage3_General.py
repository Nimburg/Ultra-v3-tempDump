import json
import os
import numpy as np 
import pandas as pd
import collections as col

import pymysql.cursors

"""
####################################################################
"""

def Stage3_Part2_Step1Score_trump(connection):

	#######################################################################
	# create lowtier tag dict
	lowtier_tag_stepScore = {}
	# command
	comd_get_lowtierTag = "\
select tagText\n\
from ultra_stage3.tag_compare_lowtier as lowtier;\n"
	# execute command
	try:
		with connection.cursor() as cursor:
			cursor.execute(comd_get_lowtierTag)
			result = cursor.fetchall()
			# result is a list of dicts: {u'tagText': u'100yearsold'}
			for item in result:
				lowtier_tag_stepScore[str(item['tagText'])] = 0
	finally:
		print "extracted low-tier tags"
		pass	

	#######################################################################
	# create hmtier tag dict
	hmtier_tag_score = {}
	# command
	comd_get_hmtierTag = "\
select tagText, tagScore_t\n\
from ultra_stage3.tag_compare_hmtier as hmtier\n\
where hmtier.tagScore_t > 5;"
	# execute command
	try:
		with connection.cursor() as cursor:
			cursor.execute(comd_get_hmtierTag)
			result = cursor.fetchall()
			# result is a list of dicts: {u'tagText': u'100yearsold'}
			for item in result:
				hmtier_tag_score[str(item['tagText'])] = item['tagScore_t']
	finally:
		print "extracted hm-tier tags"
		pass

	#######################################################################
	# for each hmtier tag
	for hmtag in hmtier_tag_score:

		##########################################
		# get list of tweetID
		tweetID_list = []
		# command
		comd_tweetIDlist = "select tweetID from ultra_v3_feb15v2.tagcallhistory_" +hmtag+";"
		# execute command
		try:
			with connection.cursor() as cursor:
				cursor.execute(comd_tweetIDlist)
				result = cursor.fetchall()
				# result is a list of dicts: {u'tagText': u'100yearsold'}
				for item in result:
					tweetID_list.append(item['tweetID'])
		finally:
			print "extracted list of hm-tier tag tweetID for: ", hmtag
			pass

		##########################################
		# extract taglist for each tweetID
		for tweetID in tweetID_list:
			# get list of tweetID
			tweetID_lowtier_tag_list = []
			# command
			pin = ''
			comd_taglist_check = "\
SELECT IF( \n\
(SELECT count(*) FROM information_schema.tables\n\
WHERE table_schema = 'ultra_v3_feb15v2' AND table_name = 'taglist_"+str(tweetID)+"'),\n\
1, 0);"
			# execute command
			try:
				with connection.cursor() as cursor:
					cursor.execute(comd_taglist_check)
					result = cursor.fetchall()
					# result is a list of dicts: {u'tagText': u'100yearsold'}
					pin = result[0][comd_taglist_check[7:-1]]
			finally:
				pass

			if pin == 1:
				comd_tweet_lowtierTag = "\
select tagText \n\
from ultra_v3_feb15v2.taglist_"+str(tweetID)+";"
				# execute command
				try:
					with connection.cursor() as cursor:
						cursor.execute(comd_tweet_lowtierTag)
						result = cursor.fetchall()
						# result is a list of dicts: {u'tagText': u'100yearsold'}
						for item in result:
							tweetID_lowtier_tag_list.append(str(item['tagText']))
				finally:
					pass

			##########################################
			# if tweetID_lowtier_tag_list is not empty, 
			# compare hmtier_tag_score and lowtier_tag_stepScore, update lowtier_tag_stepScore
			if len(tweetID_lowtier_tag_list) > 0:
				for lowtier_tag in tweetID_lowtier_tag_list:
					try:
						if hmtier_tag_score[hmtag]-1 > lowtier_tag_stepScore[lowtier_tag]:
							lowtier_tag_stepScore[lowtier_tag] = hmtier_tag_score[hmtag]-1
					except KeyError:
						pass

	# end of going through all hmtier tags
	#######################################################################
	# update lowtier_tag_stepScore into ultra_stage3.tag_compare_all step1_score_t
	for lowtier_tag in lowtier_tag_stepScore:
		# command
		comd_update_stepscore_t = "\
UPDATE ultra_stage3.tag_compare_all\n\
SET\n\
step1_score_t = "+str(lowtier_tag_stepScore[lowtier_tag])+"\n\
WHERE tagText = '"+lowtier_tag+"';\n"
		# execute commands
		try:
			with connection.cursor() as cursor:
				cursor.execute(comd_update_stepscore_t)
			# commit commands
			print "update step1_score_t of :", lowtier_tag
			connection.commit()
		finally:
			pass	


"""
####################################################################
"""

def Stage3_Part2_Step1Score_hillary(connection):

	#######################################################################
	# create lowtier tag dict
	lowtier_tag_stepScore = {}
	# command
	comd_get_lowtierTag = "\
select tagText\n\
from ultra_stage3.tag_compare_lowtier as lowtier;\n"
	# execute command
	try:
		with connection.cursor() as cursor:
			cursor.execute(comd_get_lowtierTag)
			result = cursor.fetchall()
			# result is a list of dicts: {u'tagText': u'100yearsold'}
			for item in result:
				lowtier_tag_stepScore[item['tagText']] = 0
	finally:
		print "extracted low-tier tags"
		pass	

	#######################################################################
	# create hmtier tag dict
	hmtier_tag_score = {}
	# command
	comd_get_hmtierTag = "\
select tagText, tagScore_h\n\
from ultra_stage3.tag_compare_hmtier as hmtier\n\
where hmtier.tagScore_h > 5;"
	# execute command
	try:
		with connection.cursor() as cursor:
			cursor.execute(comd_get_hmtierTag)
			result = cursor.fetchall()
			# result is a list of dicts: {u'tagText': u'100yearsold'}
			for item in result:
				hmtier_tag_score[item['tagText']] = item['tagScore_h']
	finally:
		print "extracted hm-tier tags"
		pass

	#######################################################################
	# for each hmtier tag
	for hmtag in hmtier_tag_score:

		##########################################
		# get list of tweetID
		tweetID_list = []
		# command
		comd_tweetIDlist = "select tweetID from ultra_v3_feb15vh.tagcallhistory_" +hmtag+";"
		# execute command
		try:
			with connection.cursor() as cursor:
				cursor.execute(comd_tweetIDlist)
				result = cursor.fetchall()
				# result is a list of dicts: {u'tagText': u'100yearsold'}
				for item in result:
					tweetID_list.append(item['tweetID'])
		finally:
			print "extracted list of hm-tier tag tweetID for: ", hmtag
			pass

		##########################################
		# extract taglist for each tweetID
		for tweetID in tweetID_list:
			# get list of tweetID
			tweetID_lowtier_tag_list = []
			# command
			pin = ''
			comd_taglist_check = "\
SELECT IF( \n\
(SELECT count(*) FROM information_schema.tables\n\
WHERE table_schema = 'ultra_v3_feb15vh' AND table_name = 'taglist_"+str(tweetID)+"'),\n\
1, 0);"
			# execute command
			try:
				with connection.cursor() as cursor:
					cursor.execute(comd_taglist_check)
					result = cursor.fetchall()
					# result is a list of dicts: {u'tagText': u'100yearsold'}
					#print result
					pin = result[0][comd_taglist_check[7:-1]]
					#print pin
			finally:
				pass

			if pin == 1:
				comd_tweet_lowtierTag = "\
select tagText \n\
from ultra_v3_feb15vh.taglist_"+str(tweetID)+";"
				# execute command
				try:
					with connection.cursor() as cursor:
						cursor.execute(comd_tweet_lowtierTag)
						result = cursor.fetchall()
						# result is a list of dicts: {u'tagText': u'100yearsold'}
						for item in result:
							if item['tagText'] != 'unexpected_data_missing':
								tweetID_lowtier_tag_list.append(item['tagText'])
				finally:
					pass

			##########################################
			# if tweetID_lowtier_tag_list is not empty, 
			# compare hmtier_tag_score and lowtier_tag_stepScore, update lowtier_tag_stepScore
			if len(tweetID_lowtier_tag_list) > 0:
				for lowtier_tag in tweetID_lowtier_tag_list:
					try:
						if hmtier_tag_score[hmtag]-1 > lowtier_tag_stepScore[lowtier_tag]:
							lowtier_tag_stepScore[lowtier_tag] = hmtier_tag_score[hmtag]-1
					except KeyError:
						pass

	# end of going through all hmtier tags
	#######################################################################
	# update lowtier_tag_stepScore into ultra_stage3.tag_compare_all step1_score_t
	for lowtier_tag in lowtier_tag_stepScore:
		# command
		comd_update_stepscore_t = "\
UPDATE ultra_stage3.tag_compare_all\n\
SET\n\
step1_score_h = "+str(lowtier_tag_stepScore[lowtier_tag])+"\n\
WHERE tagText = '"+lowtier_tag+"';\n"
		# execute commands
		try:
			with connection.cursor() as cursor:
				cursor.execute(comd_update_stepscore_t)
			# commit commands
			print "update step1_score_h of :", lowtier_tag
			connection.commit()
		finally:
			pass



















