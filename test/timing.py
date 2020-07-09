import subprocess
import re
import csv
import random
import os
import time
import string
from subprocess import TimeoutExpired

def timeChecksum(method, removeQCD=False):
    timeout=int(10)
    min_time=None
    for i in range(3):
        #create the process into a pipe
        process = subprocess.Popen(['bash', '-c', 'time ' + method],
                        stdin=open('data.csv', 'r'),
                        stdout=subprocess.PIPE, 
                        stderr=subprocess.PIPE,
                        universal_newlines=True)
        #only remove qcd if doing "before" timing
        if removeQCD and os.path.exists("qcd.txt"):
                os.remove("qcd.txt")
        #run the process and get the time
        try:
            stdout, stderr = process.communicate(timeout=timeout) #run the process
            realtime = re.search("real\\t(.*?)\\n",stderr).group(0)[5:-1] #get the real time
            m=re.search("(.*?m)",realtime).group(0)
            m=m[:len(m)-1]
            s=re.search("m(.*?)s",realtime).group(0)
            s=s[1:len(s)-1]
            total_seconds = float(m)*60 + float(s)
        except TimeoutExpired: #if process took longer than <timeout> seconds
            # print("took too long")
            total_seconds = timeout
        #keep the minimum of the 3 loops)
        if min_time == None or min_time > total_seconds:
            min_time = total_seconds
    # print(min_time)
    return min_time

#open and write random data to data.csv given the number of rows and cols
def writeData(row, col):
    with open('data.csv', mode='w+') as data:
        data_writer = csv.writer(data, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        cols = []
        for c in range(0,col):
            cols.append("col"+str(c))
        data_writer.writerow(cols)
        for r in range(0,row):
            active_row = []
            num_med_text_cols = 0
            for c in range(0,col):
                if(c == 0 or (c==1 and cols > 5)):
                    active_row.append(''.join(random.choice(string.ascii_letters) for _ in range(random.randint(30,100))))
                elif(c%4 == 0 and num_med_text_cols < 5):
                    active_row.append(''.join(random.choice(string.ascii_letters) for _ in range(random.randint(10,30))))
                    num_med_text_cols += 1
                elif(c%3 == 0):
                    active_row.append(''.join(random.choice(string.ascii_letters) for _ in range(random.randint(1,5))))
                else:
                    active_row.append(random.randint(1,1000000))
            data_writer.writerow(active_row)

start = time.time()
print(time.ctime(start))
with open('output.csv', mode='w+') as output:
    output_writer = csv.writer(output, delimiter=",")
    
    for cols in [5,30,100]:
        output_writer.writerow(["COLS", cols])
        output_writer.writerow(["ROWS","QCD (before)","QCD (after)","MD5","MD5 (sorted)","SHA","SHA (sorted)"])
        rows=100
        while rows <= 100000:
            #could add loop to get times for sorted on random col & shuffled rows here
            #or change timechecksum() to return the average (would need special case for "qcd before")
            #sort rand1, rand2, rand3, shuffle --> get average times
            print("Computing... rows: "+str(rows)+" cols: "+str(cols))
            if os.path.exists("qcd.txt"):
                os.remove("qcd.txt")
            #timing how long it takes to write the csv data
            writeTime = time.time()
            # print(time.ctime(writeTime))
            writeData(rows, cols) #write csv data
            writeTimeEnd = time.time()
            # print(time.ctime(writeTimeEnd))
            m=int((writeTimeEnd-writeTime)/60)
            s=int((writeTimeEnd-writeTime)%60)
            print("Writing data: " + str(m) + "m" +str(s)+"s")
            output_writer.writerow([
                rows,
                timeChecksum('./qcd -v qcd.txt', removeQCD=True), #qcd before (no qcd.txt should be found)
                timeChecksum('./qcd -v qcd.txt'), #qcd after (checksum ok)
                timeChecksum('md5'), #md5 without time it would take to pipe the sort
                #100,000 rows & 100 cols ~13 secs of sorting on col 1 (same for sha)
                timeChecksum('sort -k 1 -t , data.csv | md5'),  #md5 with time to pipe the sort
                timeChecksum('shasum -a 256'), #sha without sort
                timeChecksum('sort -k 1 -t , data.csv | shasum -a 256')] #sha with sort
            )
            rows *= 10
        output_writer.writerow([])
        output_writer.writerow([])
print("Done!")
os.remove('data.csv')
os.remove("qcd.txt")
end = time.time()
print(time.ctime(end))
m=int((end-start)/60)
s=int((end-start)%60)
print("diff: " + str(m) + "m" +str(s)+"s")