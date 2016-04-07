import json
import os
import numpy as np 
import pandas as pd
import collections as col

import pymysql.cursors


"""
####################################################################
"""

def Stage3_Part1_DBprep(connection, MySQL_DBkey):

	################################################################
	#comd1
	comd_TUtrans_t = "\
DROP TABLE IF EXISTS tag_unique_t;\n\
CREATE TABLE IF NOT EXISTs tag_unique_t LIKE ultra_stage2_t.tag_unique;\n\
INSERT INTO tag_unique_t\n\
select * from ultra_stage2_t.tag_unique;\n"

	# execute commands
	try:
		with connection.cursor() as cursor:
			cursor.execute(comd_TUtrans_t)
		# commit commands
		connection.commit()
		print "comd_TUtrans_t"
	finally:
		pass

	################################################################
	#comd2
	comd_TUtrans_h = "\
DROP TABLE IF EXISTS tag_unique_h;\n\
CREATE TABLE IF NOT EXISTs tag_unique_h LIKE ultra_stage2_h.tag_unique;\n\
INSERT INTO tag_unique_h\n\
select * from ultra_stage2_h.tag_unique;\n"

	# execute commands
	try:
		with connection.cursor() as cursor:
			cursor.execute(comd_TUtrans_h)
		# commit commands
		connection.commit()
		print "comd_TUtrans_h"
	finally:
		pass

	################################################################
	#comd3
	comd_tb_mode = "\
DROP TABLE IF EXISTS tag_combine_mode;\n\
CREATE TABLE IF NOT EXISTS tag_combine_mode\n\
(\n\
	tagText varchar(64) primary key NOT NULL,\n\
	tagScore_T float NOT NULL,\n\
	tagScore_H float NOT NULL,\n\
    tagNcall_T int NOT NULL,\n\
    tagNcall_H int NOT NULL,\n\
    total_call_T int not null,\n\
	total_call_H int not null, \n\
    tier_type_T varchar(64),\n\
    tier_type_H varchar(64)\n\
);\n"

	# execute commands
	try:
		with connection.cursor() as cursor:
			cursor.execute(comd_tb_mode)
		# commit commands
		connection.commit()
		print "comd_tb_mode"
	finally:
		pass

	################################################################
	#comd4
	comd_tb_all = "\
DROP TABLE IF EXISTS tag_compare_all;\n\
CREATE TABLE IF NOT EXISTs tag_compare_all LIKE tag_combine_mode;\n\
INSERT INTO tag_compare_all\n\
select tag_unique_t.tagText as tagText, tag_unique_t.tagScore as tagScore_t, tag_unique_h.tagScore as tagScore_h, tag_unique_t.tagNcall as tagNcall_t, tag_unique_h.tagNcall as tagNcall_h, tag_unique_t.total_Call as total_Call_t, tag_unique_h.total_Call as total_Call_h, tag_unique_t.tier_type as tier_type_t, tag_unique_h.tier_type as tier_type_h\n\
from tag_unique_t, tag_unique_h\n\
where tag_unique_t.tagText = tag_unique_h.tagText;\n"

	# execute commands
	try:
		with connection.cursor() as cursor:
			cursor.execute(comd_tb_all)
		# commit commands
		connection.commit()
		print "comd_tb_all"
	finally:
		pass

	################################################################
	#comd5
	comd_tb_ncnt = "\
DROP TABLE IF EXISTS tag_compare_NcNt;\n\
CREATE TABLE IF NOT EXISTs tag_compare_NcNt LIKE tag_compare_all;\n\
INSERT INTO tag_compare_NcNt\n\
select *\n\
from tag_compare_all\n\
where tagNcall_t = total_call_t and tagNcall_h = total_call_h;\n"

	# execute commands
	try:
		with connection.cursor() as cursor:
			cursor.execute(comd_tb_ncnt)
		# commit commands
		connection.commit()
		print "comd_tb_ncnt"
	finally:
		pass

	################################################################
	#comd6
	comd_tb_highmidtier = "\
DROP TABLE IF EXISTS tag_compare_hmtier;\n\
CREATE TABLE IF NOT EXISTs tag_compare_hmtier LIKE tag_compare_all;\n\
INSERT INTO tag_compare_hmtier\n\
SELECT *\n\
FROM tag_compare_all\n\
WHERE tagScore_t >=5 or tagScore_h >=5;\n"

	# execute commands
	try:
		with connection.cursor() as cursor:
			cursor.execute(comd_tb_highmidtier)
		# commit commands
		connection.commit()
		print "comd_tb_highmidtier"
	finally:
		pass

	################################################################
	#comd7
	comd_tb_lowtier = "\
DROP TABLE IF EXISTS tag_compare_lowtier;\n\
CREATE TABLE IF NOT EXISTs tag_compare_lowtier LIKE tag_compare_all;\n\
INSERT INTO tag_compare_lowtier\n\
SELECT *\n\
FROM tag_compare_all\n\
WHERE tagScore_t < 5 and tagScore_h < 5;\n"

	# execute commands
	try:
		with connection.cursor() as cursor:
			cursor.execute(comd_tb_lowtier)
		# commit commands
		connection.commit()
		print "comd_tb_lowtier"
	finally:
		pass

	################################################################
	#comd8
	comd_insertColumn1 = "\
ALTER TABLE tag_compare_all\n\
ADD COLUMN step1_score_t float not null default 0;\n"

	comd_insertColumn2 = "\
ALTER TABLE tag_compare_all\n\
ADD COLUMN step1_score_h float not null default 0;\n"

	comd_insertColumn3 = "\
ALTER TABLE tag_compare_all\n\
ADD COLUMN max_score_t float not null default 0;\n"

	comd_insertColumn4 = "\
ALTER TABLE tag_compare_all\n\
ADD COLUMN max_score_h float not null default 0;\n"

	# execute commands
	try:
		with connection.cursor() as cursor:	
			cursor.execute(comd_insertColumn1)
			cursor.execute(comd_insertColumn2)
			cursor.execute(comd_insertColumn3)
			cursor.execute(comd_insertColumn4)
		# commit commands
		connection.commit()
	finally:
		pass



"""
####################################################################
"""




