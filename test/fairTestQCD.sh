#!/bin/bash
original=$1

line=$(head -n 1 $original)
columns=$(awk -F "," '{print NF-1}' <<< $line)

if [[ columns -gt 3 ]]
then
  columns=3
fi

rm *.txt

#qcd original
(time ./qcd -v qcd.txt < $original) 2> timeqcd.txt

qcd=$(cat qcd.txt | jq '.content_hash')

cat timeqcd.txt

#supress output
exec >/dev/null 2>&1

time="timeqcd.txt"
qcdreal=$(sed -n 7p $time )
qcduser=$(sed -n 8p $time )
qcdsys=$(sed -n 9p $time )


#sha1 original (currently not in log)
(time shasum 1 $original) 2> timesha1.txt 1>/dev/null

time="timesha1.txt"
sha1real=$(sed -n 3p $time )
sha1user=$(sed -n 4p $time )
sha1sys=$(sed -n 5p $time )
echo "test"
shasum1=$(shasum 1 $original | awk '{print $1}' ) 
echo "test2"

echo $shasum1 >> sha1.txt

#sha256 original
(time shasum 256 $original) 2> timesha256.txt
time="timesha256.txt"
sha256real=$(sed -n 3p $time )
sha256user=$(sed -n 4p $time )
sha256sys=$(sed -n 5p $time )
shasum256=$(shasum 256 $original  | awk '{print $1}')
echo $shasum256 >> sha256.txt

#md5 original
(time md5 $original) 2> timemd5.txt
time="timemd5.txt"
md5real=$(sed -n 2p $time )
md5user=$(sed -n 3p $time )
md5sys=$(sed -n 4p $time )
md5=$(md5 $original | awk '{print $4}')
echo $md5 >> md5.txt

#append original information to log
echo -e "File\tQCD\ttime\t \tSHA256\ttime\t \tmd5\ttime" > log.txt
echo -e "$original\t$qcd\t$qcdreal\t$shasum256\t$sha256real\t$md5\t$md5real" >> log.txt
# echo -e ".\t.\t$qcduser\t.\t$sha256user\t.\t$md5user" >> log.txt
# echo -e ".\t.\t$qcdsys\t.\t$sha256sys\t.\t$md5sys" >> log.txt


#IGNORE GZIP FOR NOW
#cp $original original.csv
#gzip original.csv
#echo "Original Checksum after gzip"
#time ./qcd -v qcdzip.txt < original.csv.gz

echo
i="1"
while [ $i -le $columns ]
do

  #sort to new_data
  sort -k $i -t , $original > new_data.csv

  #qcd sorted
  (time ./qcd -v qcd.txt < new_data.csv) 2> timeqcd.txt

  time=timeqcd.txt
  qcd=$(sed -n 1p $time )
  time="timeqcd.txt"
  qcdreal=$(sed -n 3p $time )
  qcduser=$(sed -n 4p $time )
  qcdsys=$(sed -n 5p $time )

  #sha1 sorted
  ( time ( sort -k 1 -t , $original > new_data.csv && shasum 1 new_data.csv ) ) 2> timesha1.txt

  time="timesha1.txt"
  sha1real=$(sed -n 3p $time )
  sha1user=$(sed -n 4p $time )
  sha1sys=$(sed -n 5p $time )
  shasum1Check=$(shasum 1 new_data.csv | awk '{print $1}')
  if [[ $shasum1Check == $shasum1 ]]
  then
    sha1="CHECKSUM OK"
  else
    sha1=$shasum1Check
  fi
  echo $shasum1Check >> sha1.txt


  #sha256 sorted
  ( time ( sort -k 1 -t , $original > new_data.csv && shasum 256 new_data.csv ) ) 2> timesha256.txt

  time="timesha256.txt"
  sha256real=$(sed -n 3p $time )
  sha256user=$(sed -n 4p $time )
  sha256sys=$(sed -n 5p $time )
  shasum256Check=$(shasum 256 new_data.csv | awk '{print $1}')
  if [[ $shasum256Check == $shasum256 ]]
  then
    sha256="CHECKSUM OK"
  else
    sha256=$shasum256Check
  fi
  echo $shasum256Check >> sha256.txt

  #md5 sorted
  ( time ( sort -k 1 -t , $original > new_data.csv && md5 new_data.csv ) ) 2> timemd5.txt

  time="timemd5.txt"
  md5real=$(sed -n 2p $time )
  md5user=$(sed -n 3p $time )
  md5sys=$(sed -n 4p $time )
  md5Check=$(md5 new_data.csv | awk '{print $4}')
  if [[ $md5Check == $md5 ]]
  then
    md5ok="CHECKSUM OK"
  else
    md5ok=$md5Check
  fi
  echo $md5Check >> md5.txt

  echo -e "Sort col $i\t$qcd\t$qcdreal\t$sha256\t$sha256real\t$md5ok\t$md5real" >> log.txt
  # echo -e ".\t.\t$qcduser\t.\t$sha256user\t.\t$md5user" >> log.txt
  # echo -e ".\t.\t$qcdsys\t.\t$sha256sys\t.\t$md5sys" >> log.txt

  #check that .gz files are the same (do for multiple compression levels)
  #gzip new_data.csv
  #echo "gzip"
  #time ./qcd -v qcdzip.txt < new_data.csv.gz
  #gzip -d new_data.csv.gz

  i=$[$i+1]
done

#random data
sort -R -t , $original > new_data.csv

#qcd randomized
  (time ./qcd -v qcd.txt < new_data.csv) 2> timeqcd.txt

  time=timeqcd.txt
  qcd=$(sed -n 1p $time )
  time="timeqcd.txt"
  qcdreal=$(sed -n 3p $time )
  qcduser=$(sed -n 4p $time )
  qcdsys=$(sed -n 5p $time )

  #sha1 randomized
  ( time ( sort -k 1 -t , $original > new_data.csv && shasum 1 new_data.csv ) ) 2> timesha1.txt

  time="timesha1.txt"
  sha1real=$(sed -n 3p $time )
  sha1user=$(sed -n 4p $time )
  sha1sys=$(sed -n 5p $time )
  shasum1Check=$(shasum 1 new_data.csv | awk '{print $1}')
  if [[ $shasum1Check == $shasum1 ]]
  then
    sha1="CHECKSUM OK"
  else
    sha1=$shasum1Check
  fi
  echo $shasum1Check >> sha1.txt


  #sha256 randomized
  ( time ( sort -k 1 -t , $original > new_data.csv && shasum 256 new_data.csv ) ) 2> timesha256.txt

  time="timesha256.txt"
  sha256real=$(sed -n 3p $time )
  sha256user=$(sed -n 4p $time )
  sha256sys=$(sed -n 5p $time )
  shasum256Check=$(shasum 256 new_data.csv | awk '{print $1}')
  if [[ $shasum256Check == $shasum256 ]]
  then
    sha256="CHECKSUM OK"
  else
    sha256=$shasum256Check
  fi
  echo $shasum256Check >> sha256.txt

  #md5 randomized
  ( time ( sort -k 1 -t , $original > new_data.csv && md5 new_data.csv ) ) 2> timemd5.txt

  time="timemd5.txt"
  md5real=$(sed -n 2p $time )
  md5user=$(sed -n 3p $time )
  md5sys=$(sed -n 4p $time )
  md5Check=$(md5 new_data.csv | awk '{print $4}')
  #we also want to compare sorting the new_data, piping that to md5, and comparing that time to qcd (in another script)
  #"is qcd faster than simply sorting and piping to md5?"
  if [[ $md5Check == $md5 ]]
  then
    md5ok="CHECKSUM OK"
  else
    md5ok=$md5Check
  fi
  echo $md5Check >> md5.txt

  echo -e "Randomized\t$qcd\t$qcdreal\t$sha256\t$sha256real\t$md5ok\t$md5real" >> log.txt
  # echo -e ".\t.\t$qcduser\t.\t$sha256user\t.\t$md5user" >> log.txt
  # echo -e ".\t.\t$qcdsys\t.\t$sha256sys\t.\t$md5sys" >> log.txt

#check that .gz files are the same (do for multiple compression levels)
#gzip new_data.csv
#echo "gzip"
#./qcd -v qcdzip.txt < new_data.csv.gz
#gzip -d new_data.csv.gz

# rm qcd.txt
#rm qcdzip.txt
rm new_data.csv
#rm *.gz

#un-supress output
exec >/dev/tty 2>&1

#display formatted log data 
column -t -s $'\t' log.txt