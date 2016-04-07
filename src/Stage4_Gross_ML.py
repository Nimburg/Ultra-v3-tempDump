import json
import os
import numpy as np 
import pandas as pd
import collections as col
import time

import pymysql.cursors

import matplotlib.pyplot as plt

from sklearn.cluster import MiniBatchKMeans, KMeans
from sklearn.metrics.pairwise import pairwise_distances_argmin
from sklearn.datasets.samples_generator import make_blobs


def Gross_K_means():

	#######################################################################

	MySQL_DBkey2 = {'host':'localhost', 'user':'', 'password':'', 'db':'','charset':'utf8mb4'}

	# command
	comd_Score_TH = "\
	select tagScore_T, tagSCore_H, step1_score_t, step1_score_h\n\
	from tag_compare_all\n\
	where step1_score_t > 0 or step1_score_h > 0 or tagScore_t >= 5 or tagScore_h >= 5;\n"

	temp_data = [[],[],[],[]]

	# Connect to the database
	connection = pymysql.connect(host=MySQL_DBkey2['host'],
								 user=MySQL_DBkey2['user'],
								 password=MySQL_DBkey2['password'],
								 db=MySQL_DBkey2['db'],
								 charset=MySQL_DBkey2['charset'],
								 cursorclass=pymysql.cursors.DictCursor)
	try: 
		with connection.cursor() as cursor:
			cursor.execute(comd_Score_TH)
			result = cursor.fetchall()
			# result is a list of dicts: {u'tagText': u'100yearsold'}
			for item in result:
				temp_data[0].append(item['tagScore_T'])
				temp_data[1].append(item['tagSCore_H'])
				temp_data[2].append(item['step1_score_t'])
				temp_data[3].append(item['step1_score_h'])
	finally:
		pass
	connection.close()

	#######################################################################
	# data check

	Data_TH = np.array(temp_data)

	plt.scatter(Data_TH[0],Data_TH[1],color='black')
	axes = plt.gca()
	axes.set_xlim([-1,11])
	axes.set_ylim([-1,11])

	plt.show()

	plt.scatter(Data_TH[2],Data_TH[3],color='black')
	axes = plt.gca()
	axes.set_xlim([-1,11])
	axes.set_ylim([-1,11])

	plt.show()

	##################################################################
	# rewrite array format into data points

	# only tagScores, x trump y hillary
	Data_tagScore_TH = []
	for i in range(len(Data_TH[1])):
		Data_tagScore_TH.append([Data_TH[0,i],Data_TH[1,i]])
	Data_tagScore_TH = np.array(Data_tagScore_TH)

	# only StepScores, x trump y hillary
	Data_stepScore_TH = []
	for i in range(len(Data_TH[1])):
		Data_stepScore_TH.append([Data_TH[2,i],Data_TH[3,i]])
	Data_stepScore_TH = np.array(Data_stepScore_TH)

	# 4D, x trump y hillary, tagscore then stepscore
	Data_4D_TH = []
	for i in range(len(Data_TH[1])):
		Data_4D_TH.append([Data_TH[0,i],Data_TH[1,i],Data_TH[2,i],Data_TH[3,i]])
	Data_4D_TH = np.array(Data_4D_TH)

	# re-fill empty step-score dimensions
	for i in range(len(Data_stepScore_TH)):
		# Data_stepScore_TH
		if Data_stepScore_TH[i,0] == 0:
			Data_stepScore_TH[i,0] = Data_tagScore_TH[i,0]
		if Data_stepScore_TH[i,1] == 0:
			Data_stepScore_TH[i,1] = Data_tagScore_TH[i,1]
		
	for i in range(len(Data_4D_TH)):
		# Data_4D_TH
		if Data_4D_TH[i,2] == 0:
			Data_4D_TH[i,2] = Data_tagScore_TH[i,0]
		if Data_4D_TH[i,3] == 0:
			Data_4D_TH[i,3] = Data_tagScore_TH[i,1]

	##############################################################################
	# post refil data check
	fig = plt.figure()

	ax = fig.add_subplot(3, 1, 1)
	ax.plot(Data_tagScore_TH[:, 0], Data_tagScore_TH[:, 1], 'w', markerfacecolor='blue', marker='.')
	ax.set_title('tagScore')
	ax.set_xticks(())
	ax.set_yticks(())

	ax = fig.add_subplot(3, 1, 2)
	ax.plot(Data_stepScore_TH[:, 0], Data_stepScore_TH[:, 1], 'w', markerfacecolor='blue', marker='.')
	ax.set_title('StepScore')
	ax.set_xticks(())
	ax.set_yticks(())

	ax = fig.add_subplot(3, 1, 3)
	ax.plot(Data_4D_TH[:, 2], Data_4D_TH[:, 3], 'w', markerfacecolor='blue', marker='.')
	ax.set_title('StepScore')
	ax.set_xticks(())
	ax.set_yticks(())

	plt.show()

	##############################################################################
	# K-means
	n_clusters = 8
	n_init = 500
	max_iter = 500
	# top 11 colors
	colors = ['firebrick', 'red', 'orange', 'yellow','tan', 'green', 'skyblue', 'blue', 'violet', 'magenta','black']

	##############################################################################
	# Compute tagScores with K-means
	k_means = KMeans(init='k-means++', n_clusters=n_clusters, n_init=n_init, max_iter = max_iter)
	k_means.fit(Data_tagScore_TH)
	TS_labels = k_means.labels_
	TS_cluster_centers = k_means.cluster_centers_
	TS_labels_unique = np.unique(TS_labels)

	##############################################################################
	# Compute StepScores with K-means
	k_means.fit(Data_stepScore_TH)

	SS_labels = k_means.labels_
	SS_cluster_centers = k_means.cluster_centers_
	SS_labels_unique = np.unique(SS_labels)

	##############################################################################
	# Compute in 4D with K-means
	k_means.fit(Data_4D_TH)

	full4D_labels = k_means.labels_
	full4D_cluster_centers = k_means.cluster_centers_
	full4D_labels_unique = np.unique(full4D_labels)

	##############################################################################
	# We want to have the same colors for the same cluster from the
	# MiniBatchKMeans and the KMeans algorithm. Let's pair the cluster centers per
	# closest one.

	order = pairwise_distances_argmin(TS_cluster_centers, SS_cluster_centers)
	order = pairwise_distances_argmin(TS_cluster_centers, full4D_cluster_centers[:,0:2])

	print "tagScore centers: ", TS_cluster_centers
	print "StepScore centers: ", SS_cluster_centers
	print "full 4D centers: ", full4D_cluster_centers

	##############################################################################
	# Plot result
	fig = plt.figure(figsize=(16, 16))
	fig.subplots_adjust(left=0.02, right=0.98, bottom=0.05, top=0.9)
	# tagScore
	ax = fig.add_subplot(2, 3, 1)
	for k, col in zip(range(n_clusters), colors):
		my_members = TS_labels == k
		cluster_center = TS_cluster_centers[k]	
		ax.plot(Data_tagScore_TH[my_members, 0], Data_tagScore_TH[my_members, 1], 'w',
				markerfacecolor=col, marker='.')
		ax.plot(cluster_center[0], cluster_center[1], 'o', markerfacecolor=col,
				markeredgecolor='k', markersize=6)

	ax.set_title('tagScore')
	ax.set_xticks(())
	ax.set_yticks(())

	# StepScore
	ax = fig.add_subplot(2, 3, 2)
	for k, col in zip(range(n_clusters), colors):
		my_members = SS_labels == k
		cluster_center = SS_cluster_centers[order[k]]	
		ax.plot(Data_stepScore_TH[my_members, 0], Data_stepScore_TH[my_members, 1], 'w',
				markerfacecolor=col, marker='.')
		ax.plot(cluster_center[0], cluster_center[1], 'o', markerfacecolor=col,
				markeredgecolor='k', markersize=6)

	ax.set_title('StepScore')
	ax.set_xticks(())
	ax.set_yticks(())

	####################################################################
	# migrating blocks
	ax = fig.add_subplot(2, 3, 3)
	# migration
	for k, col in zip(range(n_clusters), colors):
		my_members = TS_labels == k
		cluster_center = TS_cluster_centers[k]
		ax.plot(Data_4D_TH[my_members, 2], Data_4D_TH[my_members, 3], 'w',
				markerfacecolor=col, marker='.')
	ax.set_title('Step.S clusters migrating in Step.S frame')
	ax.set_xticks(())
	ax.set_yticks(())

	####################################################################
	# 4D, TagScore
	ax = fig.add_subplot(2, 3, 4)
	for k, col in zip(range(n_clusters), colors):
		my_members = full4D_labels == order[k]
		cluster_center = full4D_cluster_centers[order[k]]
		ax.plot(Data_4D_TH[my_members, 0], Data_4D_TH[my_members, 1], 'w',
				markerfacecolor=col, marker='.')
		ax.plot(cluster_center[0], cluster_center[1], 'o', markerfacecolor=col,
				markeredgecolor='k', markersize=6)

	ax.set_title('full 4D, in TagScore')
	ax.set_xticks(())
	ax.set_yticks(())

	# 4D, StepScore
	ax = fig.add_subplot(2, 3, 5)
	for k, col in zip(range(n_clusters), colors):
		my_members = full4D_labels == order[k]
		cluster_center = full4D_cluster_centers[order[k]]
		ax.plot(Data_4D_TH[my_members, 2], Data_4D_TH[my_members, 3], 'w',
				markerfacecolor=col, marker='.')
		ax.plot(cluster_center[2], cluster_center[3], 'o', markerfacecolor=col,
				markeredgecolor='k', markersize=6)

	ax.set_title('full 4D, in StepScore')
	ax.set_xticks(())
	ax.set_yticks(())

	####################################################################
	# migrating blocks

	ax = fig.add_subplot(2, 3, 6)

	different = (full4D_labels == n_clusters+1)

	for k in range(n_clusters):
		different += ((TS_labels == k) != (full4D_labels == order[k]))

	identic = np.logical_not(different)
	ax.plot(Data_4D_TH[identic, 0], Data_4D_TH[identic, 1], 'w',
			markerfacecolor='#bbbbbb', marker='.')
	ax.plot(Data_4D_TH[different, 0], Data_4D_TH[different, 1], 'w',
			markerfacecolor='m', marker='.')

	ax.set_title('4D.S diff Tag.S in Tag.S frame')
	ax.set_xticks(())
	ax.set_yticks(())

	plt.savefig('../output/Gross_Kmeans_setNcluster_{}.png'.format(n_clusters))
	plt.show()
	####################################################################

	return '../output/Gross_Kmeans_setNcluster_{}.png'.format(n_clusters)



"""
#################################################################################
"""

if __name__ == "__main__":
	Gross_K_means()


