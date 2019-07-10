import numpy as np
import matplotlib.pyplot as plt

file = open("/home/ubuntu/BA_Project/log/jaccard_data_log.txt", "r")

for line in file:
    pass
last = line
file.close()

parts = line.split(">>>")

if int(parts[0]) > 1:
    outfile = "/home/ubuntu/jaccard_dist_cross_" + parts[0] + "_files.png"
else:
    outfile = "/home/ubuntu/jaccard_dist_cross_f_" + parts[1] + ".png"

vals = parts[2][1:-1].split(", ")
intvals = [int(s) for s in vals]

bins = []
for i in range(len(intvals)):
    bins.append(i * 0.05)

dic = dict(zip(bins, intvals))
allvals = []

for key in dic:
    for i in range(dic[key]):
        allvals.append(key)

npvals = np.array(allvals)
plt.hist(npvals, bins=len(intvals))
plt.xlabel("Jaccard Distanz")
plt.ylabel("Anzahl der Autoren")
plt.savefig(outfile)
