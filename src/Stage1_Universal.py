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



########################################################################
# NOTE: this version of UTO_update enhanced the sensitivity of score to sudden upward change
# for BOTH tag and user
# For tag, threshold is 9.0 vs 2.0 and 5.0 vs 2.0
# For tag, threshold is 9.0 vs 1.2 and 5.0 vs 1.2
# After threshold, need to set N_call = 1
def UTO_update(Tag_Related_OBJ, Universal_Tweet_OBJ):

	########################################################################
	# 1st, if there is keyword_tag
	# update tag, then update user
	flag_key = False
	if len(Universal_Tweet_OBJ['Tag_Keyword']) > 0:
		flag_key = True

		# add keyword_tag into Tag_Related_OBJ
		for item in Universal_Tweet_OBJ['Tag_Keyword']:
			if item in Tag_Related_OBJ['tag_keyword']:
				Tag_Related_OBJ['tag_keyword'][item] += 1
			else:
				Tag_Related_OBJ['tag_keyword'][item] = 1

		# add relvent_tag into Tag_Related_OBJ
		# since there is the keyword_tag, Max_score = 9
		for item in Universal_Tweet_OBJ['Tag_Relevent']:
			if item in Tag_Related_OBJ['tag_relevant']:
				# update score; as New Average
				New_Tag_Score = 1.0*(9.0 + Tag_Related_OBJ['tag_relevant'][item]*Tag_Related_OBJ['tag_relevant_N'][item])/(Tag_Related_OBJ['tag_relevant_N'][item]+1)
				if Tag_Related_OBJ['tag_relevant'][item] < 2.0:
					Tag_Related_OBJ['tag_relevant'][item] = 9.0
					Tag_Related_OBJ['tag_relevant_N'][item] = 1
				else:
					Tag_Related_OBJ['tag_relevant'][item] = New_Tag_Score
					Tag_Related_OBJ['tag_relevant_N'][item] += 1
			else:
				Tag_Related_OBJ['tag_relevant'][item] = 9.0
				Tag_Related_OBJ['tag_relevant_N'][item] = 1

		# update user_infor
		for element in Universal_Tweet_OBJ['user_id']:
			user_id_str = element
		if user_id_str in Tag_Related_OBJ['user'] and user_id_str in Tag_Related_OBJ['user_N']:
			# since there is the keyword_tag, score to 10
			New_User_Score = 1.0*(9.0 + Tag_Related_OBJ['user'][item]*Tag_Related_OBJ['user_N'][item])/(Tag_Related_OBJ['user_N'][item]+1)
			if Tag_Related_OBJ['user'][item] < 1.2:
				Tag_Related_OBJ['user'][user_id_str] = 9.0
				Tag_Related_OBJ['user_N'][user_id_str] = 1
			else:
				Tag_Related_OBJ['user'][user_id_str] = New_User_Score
				Tag_Related_OBJ['user_N'][user_id_str] += 1
		else:
			Tag_Related_OBJ['user'][user_id_str] = 9.0
			Tag_Related_OBJ['user_N'][user_id_str] = 1			

	########################################################################
	# 2nd, if not, whether there is upper_tag
	flag_relevent = False
	if flag_key == False  and len(Universal_Tweet_OBJ['Tag_Relevent']) > 0: 
		flag_relevent = True

		# add relvent_tag into Tag_Related_OBJ
		# find out max_score
		Max_Score = 0.0
		for item in Universal_Tweet_OBJ['Tag_Relevent']:
			if item in Tag_Related_OBJ['tag_relevant'] and Max_Score < Tag_Related_OBJ['tag_relevant'][item]:
				Max_Score = Tag_Related_OBJ['tag_relevant'][item]

		# load relevant_tags
		for item in Universal_Tweet_OBJ['Tag_Relevent']:
			if item in Tag_Related_OBJ['tag_relevant']:
				# update score; as New Average
				New_Tag_Score = 1.0*(Max_Score - 1 + Tag_Related_OBJ['tag_relevant'][item]*Tag_Related_OBJ['tag_relevant_N'][item])/(Tag_Related_OBJ['tag_relevant_N'][item]+1)
				if Max_Score > 5.0 and Tag_Related_OBJ['tag_relevant'][item] < 2.0:
					Tag_Related_OBJ['tag_relevant'][item] = Max_Score -1
					Tag_Related_OBJ['tag_relevant_N'][item] = 1
				else:
					Tag_Related_OBJ['tag_relevant'][item] = New_Tag_Score
					Tag_Related_OBJ['tag_relevant_N'][item] += 1
			else:
				Tag_Related_OBJ['tag_relevant'][item] = Max_Score - 1
				Tag_Related_OBJ['tag_relevant_N'][item] = 1		

		# update user_infor
		for element in Universal_Tweet_OBJ['user_id']:
			user_id_str = element
		if user_id_str in Tag_Related_OBJ['user'] and user_id_str in Tag_Related_OBJ['user_N']:
			# compare user_score with current Max_Score
			New_User_Score = 1.0*(Max_Score - 1 + Tag_Related_OBJ['user'][item]*Tag_Related_OBJ['user_N'][item])/(Tag_Related_OBJ['user_N'][item]+1)
			if  Tag_Related_OBJ['user'][item] < 1.2 and Max_Score > 5.0:
				Tag_Related_OBJ['user'][item] = Max_Score -1
				Tag_Related_OBJ['user_N'][user_id_str] = 1
			else:
				Tag_Related_OBJ['user'][user_id_str] = New_User_Score
				Tag_Related_OBJ['user_N'][user_id_str] += 1
		else:
			Tag_Related_OBJ['user'][user_id_str] = Max_Score - 1
			Tag_Related_OBJ['user_N'][user_id_str] = 1

	########################################################################
	# 3rd, if no keyword nor relevant tags, update Tag_Related_OBJ according to Tag_due_user
	# Thus, upload ALL remaining tags into the Bank
	if flag_key == False and flag_relevent == False and len(Universal_Tweet_OBJ['Tag_due_user']) > 0:
		# add Tag_due_user into Tag_Related_OBJ
		# find out max_score
		Max_Score = 0.0
		for item in Universal_Tweet_OBJ['Tag_due_user']:
			if item in Tag_Related_OBJ['tag_relevant'] and Max_Score < Tag_Related_OBJ['tag_relevant'][item]:
				Max_Score = Tag_Related_OBJ['tag_relevant'][item]

		# load relevant_tags
		for item in Universal_Tweet_OBJ['Tag_due_user']:
			if item in Tag_Related_OBJ['tag_relevant']:
				# update score; as New Average
				New_Tag_Score = 1.0*(Max_Score - 1 + Tag_Related_OBJ['tag_relevant'][item]*Tag_Related_OBJ['tag_relevant_N'][item])/(Tag_Related_OBJ['tag_relevant_N'][item]+1)
				Tag_Related_OBJ['tag_relevant'][item] = New_Tag_Score
				# update N_call
				Tag_Related_OBJ['tag_relevant_N'][item] += 1
			else:
				Tag_Related_OBJ['tag_relevant'][item] = Max_Score - 1
				Tag_Related_OBJ['tag_relevant_N'][item] = 1		

		# update user_infor
		for element in Universal_Tweet_OBJ['user_id']:
			user_id_str = element
		if user_id_str in Tag_Related_OBJ['user'] and user_id_str in Tag_Related_OBJ['user_N']:
			# compare user_score with current Max_Score
			Tag_Related_OBJ['user_N'][user_id_str] += 1
			New_User_Score = 1.0*(Max_Score - 1 + Tag_Related_OBJ['user'][item]*Tag_Related_OBJ['user_N'][item])/(Tag_Related_OBJ['user_N'][item]+1)		
			Tag_Related_OBJ['user'][user_id_str] = New_User_Score
		else:
			Tag_Related_OBJ['user'][user_id_str] = Max_Score - 1
			Tag_Related_OBJ['user_N'][user_id_str] = 1

	########################################################################
	# clean the bank according to ~25% threshold of ~score >= 1
	
	# clear tags
	clear_tag = set([])
	# search for tags
	for key in Tag_Related_OBJ['tag_relevant']:
		if Tag_Related_OBJ['tag_relevant'][key] < 1.0:
			clear_tag.add(key)
			#print "delete tag: ", key, Tag_Related_OBJ['tag_relevant'][key]
	# clear targets
	for key in clear_tag:
		del Tag_Related_OBJ['tag_relevant'][key]
		del Tag_Related_OBJ['tag_relevant_N'][key]

	# clear users
	clear_user = set([])
	# search for tags
	for key in Tag_Related_OBJ['user']:
		if Tag_Related_OBJ['user'][key] < 0.1:
			clear_user.add(key)
			#print "delete user: ", key, Tag_Related_OBJ['user'][key]
	# clear targets
	for key in clear_user:
		del Tag_Related_OBJ['user'][key]
		del Tag_Related_OBJ['user_N'][key]

	########################################################################
	# return

	return Tag_Related_OBJ


########################################################################

