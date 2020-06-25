import subprocess
import re
import csv
import random
import os
import time
import string

def timeChecksum(method):
    #create the process into a pipe
    process = subprocess.Popen(['bash', '-c', 'time ' + method],
                     stdin=open('data.csv', 'r'),
                     stdout=subprocess.PIPE, 
                     stderr=subprocess.PIPE,
                     universal_newlines=True)
    stdout, stderr = process.communicate() #run the process
    realtime = re.search("real\\t(.*?)\\n",stderr).group(0)[5:-1] #get the real time
    m=re.search("(.*?m)",realtime).group(0)
    m=m[:len(m)-1]
    s=re.search("m(.*?)s",realtime).group(0)
    s=s[1:len(s)-1]
    total_seconds = float(m)*60 + float(s)
    return total_seconds

#open and write random data to data.csv given the number of rows and cols
def writeData(row, col):
    with open('data.csv', mode='w+') as data:
        data_writer = csv.writer(data, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        cols = []
        for c in range(0,col):
            cols.append("col"+str(c))
        data_writer.writerow(cols)
        for r in range(0,row):
            row = []
            for c in range(0,col):
                if(c%5 == 0):
                    row.append(''.join(random.choice(string.ascii_letters) for _ in range(random.randint(30,100))))
                elif(c%4 == 0):
                    row.append(''.join(random.choice(string.ascii_letters) for _ in range(random.randint(10,30))))
                elif(c%3 == 0):
                    row.append(''.join(random.choice(string.ascii_letters) for _ in range(random.randint(1,5))))
                else:
                    row.append(random.randint(1,1000))
            data_writer.writerow(row)

start = time.time()
print(time.ctime(start))
with open('output.csv', mode='w+') as output:
    output_writer = csv.writer(output, delimiter=",")
    

    
    for cols in [5,30,100]:
        output_writer.writerow(["COLS", cols])
        output_writer.writerow(["ROWS","QCD (before)","QCD (after)","MD5","MD5 (sorted)","SHA","SHA (sorted)"])
        rows=100
        while rows <= 1000000:
            #could add loop to get times for sorted on random col & shuffled rows here
            #or change timechecksum() to return the average (would need special case for "qcd before")
            #sort rand1, rand2, rand3, shuffle --> get average times
            print("Computing... rows: "+str(rows)+" cols: "+str(cols))
            if os.path.exists("qcd.txt"):
                print("removed")
                os.remove("qcd.txt")

            writeData(rows, cols)
            output_writer.writerow([
                rows,
                timeChecksum('./qcd -v qcd.txt'), #qcd before (no qcd.txt should be found)
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