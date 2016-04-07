from collections import defaultdict
from heapq import nlargest

from luigi import six
import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs
import luigi.postgres

from Stage4_Gross_ML import Gross_K_means
from Stage3_Main import Stage3_Gross
from Stage2_Main import Stage2_Gross
from Stage1_Main import Stage1_Main
from Stage0_DBprep import Stage0_Main


# global variables
file_name = '.txt'
keyword_list = ['trump', 'hillary']
DB_Stage3 = {'host':'localhost', 'user':'', 'password':'', 'db':'','charset':'utf8mb4'}


###############################################################################
class Stage4_twitter_network(luigi.Task):

	def requires(self):
		return [Stage3()]

	def run(self):
		Gross_K_means()
		
		with self.output().open('w') as output:
			output.write('Done Stage4\n')

	def output(self):
		return luigi.LocalTarget("../DataBase/Stage4.txt")

###############################################################################
# stage3 is the point where system cycle through list of key words
class Stage3(luigi.Task):

	keyword_list = ['trump', 'hillary']

	def requires(self):
		return [Stage2(keyword) for keyword in self.keyword_list]

	def run(self):
		Stage3_Gross(DB_Stage3 = DB_Stage3)

		with self.output().open('w') as output:
			output.write('Done Stage3\n')

	def output(self):
		return luigi.LocalTarget("../DataBase/Stage3.txt")

###############################################################################
class Stage2(luigi.Task):

	db_stage1_key0 = {'host':'localhost', 'user':'', 'password':'', 'db':'', 'charset':'utf8mb4'}
	db_stage2_key0 = {'host':'localhost', 'user':'', 'password':'', 'db':'', 'charset':'utf8mb4'}

	# keyword 
	stage2_keyword = luigi.Parameter()

	def requires(self):
		return [Stage1(self.stage2_keyword)]

	def run(self):
		# update db key
		self.db_stage1_key0['db'] = self.db_stage1_key0['db'] + self.stage2_keyword
		self.db_stage2_key0['db'] = self.db_stage2_key0['db'] + self.stage2_keyword

		Stage2_Gross(MySQL_DBkey1=db_stage1_key0, MySQL_DBkey2=db_stage2_key0)
		
		with self.output().open('w') as output:
			output.write('Done Stage2\n')

	def output(self):
		return luigi.LocalTarget("../DataBase/Stage2.txt")

###############################################################################
class Stage1(luigi.Task):

	db_stage1_key0 = {'host':'localhost', 'user':'', 'password':'', 'db':'','charset':'utf8mb4'}

	# keyword 
	stage1_keyword = luigi.Parameter()

	# load into stage0
	def requires(self):
		return [Stage0(self.stage1_keyword)]

	# file_name global, 
	def run(self):
		# update db_stage1_key0
		self.db_stage1_key0['db'] = self.db_stage1_key0['db'] + self.stage1_keyword		

		Stage1_Main(file_name=file_name, keyword=self.stage1_keyword, MySQL_DBkey=self.db_stage1_key0)
		
		with self.output().open('w') as output:
			output.write('Done Stage1\n')

	def output(self):
		return luigi.LocalTarget("../DataBase/Stage1.txt")

###############################################################################
class Stage0(luigi.Task):
	
	db_key = luigi.Parameter()
	db_stage1_key0 = {'host':'localhost', 'user':'', 'password':'', 'db':'','charset':'utf8mb4'}

	def run(self):
		self.db_stage1_key0['db'] = self.db_stage1_key0['db'] + self.db_key
		Stage0_Main(MySQL_DBkey=self.db_stage1_key0)
		with self.output().open('w') as output:
			output.write('Done Stage0\n')

	def output(self):
		return luigi.LocalTarget("../DataBase/Stage0.txt")



