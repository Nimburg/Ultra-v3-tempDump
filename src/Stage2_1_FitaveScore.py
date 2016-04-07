import json
import os
import numpy as np 
import pandas as pd
import collections as col

import matplotlib.pyplot as plt
from scipy import stats

######################################################################

def Lfit_Intercept(tagText, index, Xo, Yo):
	# screen data; Xo should start from at least 8 as 15 is the threshold
	for i in range(len(Xo)):
		if Xo[i] < 8:
			# truncate data
			Xo = Xo[:i]
			Yo = Yo[:i]
			break
	# X' = 1/Xo data set
	if len(Xo) > 5 and len(Yo) > 5:
		Xt = [1.0/Xo[i] for i in range(len(Xo))]
		Yt = [Yo[i] for i in range(len(Xo))]
		# linear fit
		slope, intercept, r_value, p_value, std_err = stats.linregress(Xt,Yt)
		print "intercept: ", intercept
		Xf = np.linspace(Xt[-1], Xt[0], num = 100)
		Yf = [slope*Xf[i] + intercept for i in range(len(Xf))]
		# plot 1
		plt.subplot(211)
		plt.scatter(Xo,Yo,color='black',label="Original Data")
		axes = plt.gca()
		axes.set_xlim([0,100])
		axes.set_title('original')
		plt.xlabel('Xo')
		plt.ylabel('Yo')
		plt.title('Plot of {}, NO.{}'.format(tagText, index))
		# plot 2
		plt.subplot(212)
		plt.scatter(Xt,Yt,color='black',label="1/x Data")
		axes = plt.gca()
		axes.set_xlim([0,1])
		plt.xlabel('Xt')
		plt.ylabel('Yt')
		# fit result
		plt.plot(Xf,Yf,color='red',label="1/x fit result")
		# save plot
		plt.savefig('../output/Lfit_{}_{}.png'.format(tagText, index))
		plt.show()
		plt.close()
		return intercept
	# empty Xo post filtering
	else:
		return 0



