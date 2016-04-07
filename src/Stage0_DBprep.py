import pymysql.cursors

def Stage0_Main(MySQL_DBkey):

	# Connect to the database
	connection = pymysql.connect(host=MySQL_DBkey['host'],
								 user=MySQL_DBkey['user'],
								 password=MySQL_DBkey['password'],
								 db=MySQL_DBkey['db'],
								 charset=MySQL_DBkey['charset'],
								 cursorclass=pymysql.cursors.DictCursor)

	try:
		with connection.cursor() as cursor:
			# create table Tweet_Stack
			sql = "\
USE " + MySQL_DBkey['db'] + ";\
DROP TABLE IF EXISTS Tweet_Stack;\
CREATE TABLE IF NOT EXISTS Tweet_Stack\
(\
	tweetID BIGINT PRIMARY KEY NOT NULL,\
	tweetTime TIMESTAMP NOT NULL, \
	tweetText TEXT,\
	userID BIGINT NOT NULL,\
	reply_user_id BIGINT\
)"
			cursor.execute(sql)

			# create User_Unique 
			sql = "\
USE " + MySQL_DBkey['db'] + ";\
DROP TABLE IF EXISTS User_Unique;\
CREATE TABLE IF NOT EXISTS User_Unique\
(\
	userID BIGINT PRIMARY KEY NOT NULL,\
	userName text,\
	user_follower int,\
    user_friend int,\
    user_favorites int,\
    user_score float NOT NULL,\
    user_N_actions int NOT NULL,\
    user_creation TIMESTAMP NOT NULL\
)"
			cursor.execute(sql)

			# create Tag_Unique 
			sql = "\
USE " + MySQL_DBkey['db'] + ";\
DROP TABLE IF EXISTS Tag_Unique;\
CREATE TABLE IF NOT EXISTS Tag_Unique\
(\
	tagText varchar(64) PRIMARY KEY NOT NULL,\
	tagScore float NOT NULL,\
    tagNcall int NOT NULL\
)"
			cursor.execute(sql)

		# connection is not autocommit by default. So you must commit to save your changes.
		connection.commit()
	finally: 
		connection.close()
		return None

"""
####################################################################
"""




"""
####################################################################
# test code for Stage1 main
"""

if __name__ == "__main__":

	MySQL_DBkey = {'host':'localhost', 'user':'sa', 'password':'fanyu01', 'db':'ultra_v3_feb15vh','charset':'utf8mb4'}

	Stage0_Main(MySQL_DBkey = MySQL_DBkey)

