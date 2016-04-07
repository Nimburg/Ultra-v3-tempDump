import json
import os
import numpy as np 
import pandas as pd
import collections as col

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

########################################################################
@check_args(pd.tslib.Timestamp, tuple)
def time_tuple_check(input1, input2):
	return True
@check_args(str, str)
def tuple_check(input1, input2):
	return True


"""
########################################################################
"""

def Stage1_Json_TimeUserTag(input_str, index_line, keyword, Tag_Related_OBJ):

	# to be returned data structure
	# using set, thus eliminating repeating #s within a tweet
	RetD_TimeUserTag = col.defaultdict(set)
	# initialize
	RetD_TimeUserTag['tweet_time'] = set([])
	RetD_TimeUserTag['tweet_id'] = set([])
	RetD_TimeUserTag['user_id'] = set([])
	RetD_TimeUserTag['user_name'] = set([])
	RetD_TimeUserTag['user_followers'] = set([])
	RetD_TimeUserTag['user_friends'] = set([])
	RetD_TimeUserTag['user_favourites'] = set([])
	RetD_TimeUserTag['user_born'] = set([])
	RetD_TimeUserTag['Tag_Keyword'] = set([])
	RetD_TimeUserTag['Tag_Relevent'] = set([])
	RetD_TimeUserTag['Tag_due_user'] = set([])
	RetD_TimeUserTag['text'] = set([])
	RetD_TimeUserTag['reply_to_userID'] = set([])
	RetD_TimeUserTag['mentioned_userID'] = set([])

	#############################################################
	# json load, extract tweet time
	flag_tweetUpdate = True # flag for tweet time and user information
	try:
		# load json
		tweet_json = json.loads(input_str)
	except ValueError: 
		print "Line: {}, json loads Error".format(index_line)
		flag_tweetUpdate = False
	else:	
		# extract date-time from mainbody
		try:
			time_str = tweet_json['created_at']
			tweet_id = tweet_json['id_str']
		except ValueError:
			flag_tweetUpdate = False
			pass
		except KeyError:
			flag_tweetUpdate = False
			pass
		else:
			# convert to pandas timestamp
			try:
				time_dt = pd.to_datetime(time_str)
				if pd.isnull(time_dt):
					flag_tweetUpdate = False
					print "Line: {}, date-time is NaT".format(index_line)
			except ValueError:
				flag_tweetUpdate = False
				print "Line: {}, date-time convertion failed".format(index_line)
				pass
			else:
				# upload to RetD_TimeUserTag
				if flag_tweetUpdate == True:
					RetD_TimeUserTag['tweet_time'] = set([time_dt])
					RetD_TimeUserTag['tweet_id'] = set([tweet_id])

	#############################################################
	# extract user information sub-json
	if flag_tweetUpdate == True:
		try:
			user_json = tweet_json['user']
		except ValueError:
			flag_tweetUpdate = False
			pass
		except KeyError:
			flag_tweetUpdate = False
			pass
		else:
			# extract user statistics
			try: 
				user_id = user_json['id_str']
				user_name = user_json['screen_name']
				user_followers = user_json['followers_count']
				user_friends = user_json['friends_count']
				user_favourites = user_json['favourites_count']
				user_born = user_json['created_at']
				user_born = pd.to_datetime(user_born)
				if pd.isnull(user_born):
					flag_tweetUpdate = False
					print "Line: {}, date-time is NaT".format(index_line)
			except ValueError:
				flag_tweetUpdate = False
				pass
			except KeyError:
				flag_tweetUpdate = False
				pass
			else:
				if flag_tweetUpdate == True:
					RetD_TimeUserTag['user_id'] = set([user_id])
					RetD_TimeUserTag['user_name'] = set([user_name])
					RetD_TimeUserTag['user_followers'] = set([user_followers])
					RetD_TimeUserTag['user_friends'] = set([user_friends])
					RetD_TimeUserTag['user_favourites'] = set([user_favourites])
					RetD_TimeUserTag['user_born'] = set([user_born])
					# extract reply_to_user information
					try:
						reply_userID_str = tweet_json['in_reply_to_user_id_str']
						# if userID == null, raise error; if not full digit str, raise false
						flag_idstr = reply_userID_str.isdigit()
					except ValueError:
						pass
					except KeyError:
						pass
					except AttributeError:
						pass
					except TypeError:
						pass
					else:
						if flag_idstr == True:
							RetD_TimeUserTag['reply_to_userID'].add(reply_userID_str)
					# if EE failed, set default value
					if len(RetD_TimeUserTag['reply_to_userID']) == 0:
						RetD_TimeUserTag['reply_to_userID'].add("-1")	

	#############################################################
	# extract tags from entities
	flag_tag = False # flag for tags of tweet
	if flag_tweetUpdate == True:
		# extract tags from entities
		tag_list = set([]) # eliminate repeating tags
		try:
			entities_json = tweet_json['entities']
			Hashtags_json = entities_json['hashtags']
			flag_tag = True
		except ValueError:
			pass
		except KeyError:
			pass
		except TypeError:
			pass
		else:		
			for entry in Hashtags_json:
				try:
					# THIS IS VERY VERY VERY IMPORTANT !!!!!
					tag_list.add(str(entry['text']).lower()) # THIS IS VERY VERY VERY IMPORTANT !!!!!
					# THIS IS VERY VERY VERY IMPORTANT !!!!!
					# MySQL cant distinguish upper and lower cases when str is used as name for table
					# which will result in confusion in data analysis
				except ValueError:
					pass
				except KeyError:
					pass
				except TypeError:
					pass
			# check if there is anything in tag_list
			if len(tag_list) == 0:
				flag_tag = False

	#############################################################
	# check if has tag with keyword 'trump'
	flag_tag_keyword = False # flag for tags related to key word trump
	if flag_tweetUpdate == True and flag_tag == True:
		# check keyword_tag
		for item in tag_list:
			if keyword in item.lower():
				print "\n\nLine: {}, Key Word Tag found: {}".format(index_line, item)
				print "Tracking tags: ", len(Tag_Related_OBJ['tag_relevant'])
				print "Tracking users: ", len(Tag_Related_OBJ['user'])
				flag_tag_keyword = True
				# add to relevent tag set
				RetD_TimeUserTag['Tag_Keyword'].add(item)
		# remove this tag from tag_list
		if flag_tag_keyword == True:
			for item in RetD_TimeUserTag['Tag_Keyword']:
				tag_list.discard(item)
		# if there is keyword tag, all else added to relevent set()
		if flag_tag_keyword == True:
			for item in tag_list: 
				RetD_TimeUserTag['Tag_Relevent'].add(item)

	#############################################################
	# check if has tag in the bank of "related" tags
	flag_tag_related = False
	# has time and user_infor, has tag, but not keyword tag
	if flag_tweetUpdate == True and flag_tag == True and flag_tag_keyword == False:
		# check if there is upper-level tags
		for item in tag_list:
			if item in Tag_Related_OBJ['tag_relevant'] and Tag_Related_OBJ['tag_relevant'][item] >= 5.0:
				flag_tag_related = True
				break
		# load related tags if yes
		if flag_tag_related == True:
			for item in tag_list:
				RetD_TimeUserTag['Tag_Relevent'].add(item)

	#############################################################
	# check if this tweet is issued by recorded user
	flag_recorded_user = False
	# has tag, but not keyword or related tag, check if is recorded user
	if flag_tweetUpdate == True and flag_tag == True and flag_tag_keyword == False and flag_tag_related == False:
		if user_id in Tag_Related_OBJ['user'] and Tag_Related_OBJ['user'][user_id] >= 1.0:
			flag_recorded_user = True
		# load ALL tags as due_to_user
		for item in tag_list: 
			RetD_TimeUserTag['Tag_due_user'].add(item)

	####################################################
	# return 
	if flag_tag_related == True or flag_recorded_user == True:
		flag_tag_keyword = True # as General flag indicating the need for more information

	# if there is no tag at all, then ignore the tweet
	if flag_tag == False:
		flag_tweetUpdate = False

	return flag_tweetUpdate, flag_tag_keyword, RetD_TimeUserTag

####################################################

"""
########################################################################
"""

def Stage1_Json_ExtraInfor(input_str, index_line, Universal_Tweet_OBJ):

	#############################################################
	# json load, extract tweet time
	flag_jsonload = True # flag for tweet time and user information
	try:
		# load json
		tweet_json = json.loads(input_str)
	except ValueError: 
		print "Line: {}, json loads Error".format(index_line)
		flag_jsonload = False
	else:
		pass

	####################################################
	# extract text
	if flag_jsonload == True:
		# extract date-time from mainbody
		try:
			text_str = tweet_json['text']
		except ValueError:
			pass
		except KeyError:
			pass
		else:
			Universal_Tweet_OBJ['text'].add(text_str)

	####################################################
	# extract mentioned_userID
	if flag_jsonload == True:
		# extract entities and user_mentions	
		try:
			entities_json = tweet_json['entities']
			usermentions_json = entities_json['user_mentions']
		except ValueError:
			pass
		except KeyError:
			pass
		except TypeError:
			pass
		else:
			for entry in usermentions_json:
				try:
					Universal_Tweet_OBJ['mentioned_userID'].add(entry['id_str'])
				except ValueError:
					pass
				except KeyError:
					pass
				except TypeError:
					pass

	####################################################

	return Universal_Tweet_OBJ