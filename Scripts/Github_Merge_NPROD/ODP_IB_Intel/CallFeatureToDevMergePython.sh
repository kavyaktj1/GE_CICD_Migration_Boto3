#!/bin/bash
python3 Scripts/Github_Merge_NPROD/ODP_IB_Intel/AutoMergePrFromFeatureToDev.py > AutoMergePrFromFeatureToDEVIBOutput.txt
##v_return=`python3 Scripts/AutoMergePrFromFeatureToDev.py`
#echo $v_return
##v_trim_return=`echo $v_return|cut -d' ' -f2`
##echo $v_trim_return
#v_pr_number=`echo $v_trim_return|cut -d '|' -f1`
#v_success_status=`echo $v_trim_return|cut -d '|' -f2`

#echo $v_pr_number
#echo $v_success_status

if grep success AutoMergePrFromFeatureToDEVIBOutput.txt  &> /dev/null
  then
    v_return=`grep success AutoMergePrFromFeatureToDEVIBOutput.txt`
    echo "$v_return"
elif grep mergeconflictrelease AutoMergePrFromFeatureToDEVIBOutput.txt &> /dev/null
   then
    v_return=`grep mergeconflictrelease AutoMergePrFromFeatureToDEVIBOutput.txt`
    echo "$v_return"
elif grep mergeconflictdev AutoMergePrFromFeatureToDEVIBOutput.txt &> /dev/null
   then
    v_return=`grep mergeconflictdev AutoMergePrFromFeatureToDEVIBOutput.txt`
    echo "$v_return"
 else
    echo "0|NoPrToProcess"
  fi
