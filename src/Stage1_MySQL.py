import json
import os
import numpy as np 
import pandas as pd
import collections as col

import pymysql.cursors




"""
####################################################################
"""
# variable type check
def check_args(*types):
	def real_decorator(func):
		def wrapper(*args, **kwargs):
			for val, typ in zip(args, types):
				assert isinstance(val, typ), "Value {} is not of expected type {}".format(val, typ)
			return func(*args, **kwargs)
		return wrapper
	return real_decorator

"""
####################################################################
"""

@check_args(unicode, unicode, int, int, int, pd.tslib.Timestamp)
def DTC_User_Essen(user_id, user_name, user_followers, user_friends, user_favourites, user_born):
	# check user_id
	try:
		flag_idstr = user_id.isdigit()
	except ValueError:
		pass
	except KeyError:
		pass
	except AttributeError:
		pass
	except TypeError:
		pass
	else:
		return flag_idstr

@check_args(unicode, pd.tslib.Timestamp)
def DTC_Tweet_Essen(tweet_id, tweet_time):
	# check tweet_id
	try:
		flag_idstr = tweet_id.isdigit()
	except ValueError:
		pass
	except KeyError:
		pass
	except AttributeError:
		pass
	except TypeError:
		pass
	else:
		return flag_idstr

@check_args(float, int)
def DTC_score(Score, N_call):
	return True

@check_args(str, float, int)
def DTC_TagUnique(key, tag_score, tag_N_call):
	return True


"""
####################################################################
"""

def Stage1_MySQL_TweetOBJ(connection, MySQL_DBkey, Tag_Related_OBJ, Universal_Tweet_OBJ, flag_Detail):

	####################################################################
	# extract essential user data and data-type check
	user_id = next(iter(Universal_Tweet_OBJ['user_id']))
	user_name = next(iter(Universal_Tweet_OBJ['user_name']))
	user_followers = next(iter(Universal_Tweet_OBJ['user_followers']))
	user_friends = next(iter(Universal_Tweet_OBJ['user_friends']))
	user_favourites = next(iter(Universal_Tweet_OBJ['user_favourites']))
	user_born = next(iter(Universal_Tweet_OBJ['user_born']))
	# data type check
	flag_user_essen = False
	try:
		flag_user_essen = DTC_User_Essen(user_id, user_name, user_followers, user_friends, user_favourites, user_born)
	except AssertionError:
		print "DTC_User_Essen failed"
		print type(user_id)
		pass 
	else:
		pass

	####################################################################
	# extract essebtial tweet data and data-type check
	tweet_id = next(iter(Universal_Tweet_OBJ['tweet_id']))
	tweet_time = next(iter(Universal_Tweet_OBJ['tweet_time']))
	reply_to_userID = unicode(next(iter(Universal_Tweet_OBJ['reply_to_userID'])))
	
	if flag_Detail == True:
		tweet_Text = next(iter(Universal_Tweet_OBJ['text']))
	
	# data type check
	flag_tweet_essen = False
	try:
		flag_tweet_essen = DTC_Tweet_Essen(tweet_id, tweet_time)
	except AssertionError:
		print "DTC_Tweet_Essen failed"
		print type(tweet_id), type(tweet_time), type(reply_to_userID)
		pass 
	else:
		pass

	####################################################################
	# flag for Tweet_Stack, User_Unique, UserAction_"userID"
	flag_TS_UU_UA = False
	if flag_user_essen == True and flag_tweet_essen == True:
		# if user_id in the bank, 
		if user_id in Tag_Related_OBJ['user'] and user_id in Tag_Related_OBJ['user_N']:
			# extract user score data
			user_score = Tag_Related_OBJ['user'][user_id]
			user_N_actions = Tag_Related_OBJ['user_N'][user_id]
			# data-type check
			try:
				flag_TS_UU_UA = DTC_score(Score = user_score, N_call = user_N_actions)
			except AssertionError:
				print "DTC_score failed"
				pass 
			else:
				pass

	####################################################################
	# table Tweet_Stack, User_Unique, UserAction_userID
	if flag_TS_UU_UA == True:
		# command for Tweet_Stack
		comd_TweetStack = "\
INSERT INTO Tweet_Stack (tweetID, tweetTime, userID, reply_user_id)\n\
VALUES ("+tweet_id+", '"+str(tweet_time)+"', "+user_id+", "+reply_to_userID+")\n\
ON DUPLICATE KEY UPDATE userID = " + user_id + ";\n"
		#print comd_TweetStack

		# command for User_Unique
		comd_UserUnique = "\
INSERT INTO User_Unique (userID, userName, user_follower, user_friend, user_favorites, user_score, user_N_actions, user_creation)\n\
VALUES ("+user_id+", '"+user_name+"', "+str(user_followers)+", "+str(user_friends)+", "+str(user_favourites)+", "+str(user_score)+", "+str(user_N_actions)+", '"+str(user_born)+"')\n\
ON DUPLICATE KEY UPDATE user_score = " + str(user_score) + ", user_N_actions = " + str(user_N_actions) + ";\n"
		#print comd_UserUnique

		# command for UserAction_"userID"
		UA_tablename = "UserAction_" + user_id
		comd_UserAction = "\
CREATE TABLE IF NOT EXISTS " + UA_tablename +"\n\
(\n\
	UA_ID int AUTO_INCREMENT NOT NULL,\n\
	PRIMARY KEY (UA_ID),\n\
	actTime timestamp NOT NULL,\n\
    userID bigint NOT NULL,\n\
    tweetID bigint NOT NULL,\n\
	user_score float NOT NULL,\n\
    user_N_actions int NOT NULL, \n\
    reply_user_id bigint\n\
);\n\
INSERT INTO "+UA_tablename+" (actTime, userID, tweetID, user_score, user_N_actions, reply_user_id)\n\
VALUES ('"+str(tweet_time)+"', "+user_id+", "+tweet_id+", "+str(user_score)+", "+str(user_N_actions)+", "+reply_to_userID+ ")\n\
ON DUPLICATE KEY UPDATE actTime = '"+str(tweet_time)+ "';\n"
		#print comd_UserAction

		# execute commands
		try:
			with connection.cursor() as cursor:
				cursor.execute(comd_TweetStack)
				cursor.execute(comd_UserUnique)
				cursor.execute(comd_UserAction)
			# commit commands
			print "commit commands TweetStack UserUnique UserAction"
			connection.commit()
		finally:
			pass

	# end of table Tweet_Stack, User_Unique, UserAction_userID
	####################################################################

	####################################################################
	# upload Tag_Unique, TagCallHistory and (if) TagList
	if flag_TS_UU_UA == True:
		
		# combined tag_list
		all_tag_set = set([])
		for item in Universal_Tweet_OBJ['Tag_Keyword']:
			all_tag_set.add(item)
		
		for item in Universal_Tweet_OBJ['Tag_Relevent']:
			all_tag_set.add(item)
		
		for item in Universal_Tweet_OBJ['Tag_due_user']:
			all_tag_set.add(item)

		# check against Tag_Related_OBJ
		for key in all_tag_set:
			# within loop parameters
			# flag whether upload this tag or not
			flag_tag_load = False
			tag_score = 0.0
			tag_N_call = 0

			# check if is keyword tag
			if key in Tag_Related_OBJ['tag_keyword']:
				flag_tag_load = True
				tag_score = 10.0
				tag_N_call = Tag_Related_OBJ['tag_keyword'][key]

			# check if score >= 1.0
			if key in Tag_Related_OBJ['tag_relevant'] and key in Tag_Related_OBJ['tag_relevant_N']:
				flag_tag_load = True
				tag_score = Tag_Related_OBJ['tag_relevant'][key]
				tag_N_call = Tag_Related_OBJ['tag_relevant_N'][key]

			# data-type check
			flag_tag_DTC = False
			try:
				flag_tag_DTC = DTC_TagUnique(key, tag_score, tag_N_call)		
			except AssertionError:
				print "DTC_TagUnique failed"
				print type(key), type(tag_score), type(tag_N_call)
				pass 
			else:
				pass
			
			####################################################################
			# general case
			if flag_tag_load == True and flag_tag_DTC == True:
				
				# command for Tag_Unique
				comd_TagUnique = "\
INSERT INTO Tag_Unique (tagText, tagScore, tagNcall)\n\
VALUES ('"+key+"', "+str(tag_score)+", "+str(tag_N_call)+")\n\
ON DUPLICATE KEY UPDATE tagScore = "+str(tag_score)+", tagNcall = "+str(tag_N_call)+";\n"
				#print comd_TagUnique
				
				# command for TagCallHistory
				TCH_tablename = "TagCallHistory_" + key
				comd_TagCallHistory= "\
CREATE TABLE IF NOT EXISTS " + TCH_tablename +"\n\
(\n\
	TCH_ID int AUTO_INCREMENT NOT NULL,\n\
	PRIMARY KEY (TCH_ID),\n\
	callTime timestamp NOT NULL,\n\
    tagText varchar(64) NOT NULL,\n\
	userID bigint NOT NULL,\n\
    tweetID bigint NOT NULL,\n\
 	tagScore float NOT NULL,\n\
    tagNcall int NOT NULL\n\
);\n\
INSERT INTO "+TCH_tablename+" (callTime, tagText, userID, tweetID, tagScore, tagNcall)\n\
VALUES ('"+str(tweet_time)+"', '"+key+"', "+user_id+", "+tweet_id+", "+str(tag_score)+", "+str(tag_N_call)+")\n\
ON DUPLICATE KEY UPDATE callTime = '"+str(tweet_time)+ "';\n"
				#print comd_TagCallHistory

				# execute commands
				try:
					with connection.cursor() as cursor:
						cursor.execute(comd_TagUnique)
						cursor.execute(comd_TagCallHistory)
					# commit commands
					print "commit commands TagUnique TagCallHistory"
					connection.commit()
				finally:
					pass

			####################################################################
			# if there is more than 1 tag
			if flag_tag_load == True and flag_tag_DTC == True and len(all_tag_set) >= 2:
				# command for TagList_tweetID
				TL_tablename = "TagList_" + tweet_id
				comd_TagList= "\
CREATE TABLE IF NOT EXISTS " + TL_tablename +"\n\
(\n\
	TL_ID int AUTO_INCREMENT NOT NULL,\n\
	PRIMARY KEY (TL_ID),\n\
    tagText varchar(64) NOT NULL\n\
);\n\
INSERT INTO "+TL_tablename+" (tagText)\n\
VALUES ('"+key+"')\n\
ON DUPLICATE KEY UPDATE tagText = '"+key+ "';\n"
				#print comd_TagList

				# execute commands
				try:
					with connection.cursor() as cursor:
						cursor.execute(comd_TagList)
					# commit commands
					print "commit commands TagList"
					connection.commit()
				finally:
					pass

	# end of tag upload
	####################################################################

	####################################################################
	# upload MenUserList
	if flag_TS_UU_UA == True and flag_Detail == True and len(Universal_Tweet_OBJ['mentioned_userID']) > 0:
		# MenUserList
		for userID in Universal_Tweet_OBJ['mentioned_userID']:
			if userID.isdigit() and userID in Tag_Related_OBJ['user'] and userID in Tag_Related_OBJ['user_N']:

				# command for MenUserList_tweetID
				MUL_tablename = "MenUserList_" + tweet_id
				comd_MenUserList= "\
CREATE TABLE IF NOT EXISTS " + MUL_tablename +"\n\
(\n\
	MUL_ID int AUTO_INCREMENT NOT NULL,\n\
	PRIMARY KEY (MUL_ID),\n\
    userID bigint NOT NULL\n\
);\n\
INSERT INTO "+MUL_tablename+" (userID)\n\
VALUES ("+userID+")\n\
ON DUPLICATE KEY UPDATE userID = "+userID+ ";\n"

				# execute commands
				try:
					with connection.cursor() as cursor:
						cursor.execute(comd_MenUserList)
					# commit commands
					print "commit commands MenUserList"
					connection.commit()
				finally:
					pass

	# end of MenUserList upload
	####################################################################

"""
####################################################################
"""

