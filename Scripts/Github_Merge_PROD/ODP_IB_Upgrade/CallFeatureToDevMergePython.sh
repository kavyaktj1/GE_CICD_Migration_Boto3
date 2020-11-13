#!/bin/bash
python3 Scripts/Github_Merge_PROD/ODP_IB_Upgrade/AutoMergePrFromFeatureToDev.py > AutoMergePrFromFeatureToDEVIBUOutput.txt
##v_return=`python3 Scripts/AutoMergePrFromFeatureToDev.py`
#echo $v_return
##v_trim_return=`echo $v_return|cut -d' ' -f2`
##echo $v_trim_return
#v_pr_number=`echo $v_trim_return|cut -d '|' -f1`
#v_success_status=`echo $v_trim_return|cut -d '|' -f2`

#echo $v_pr_number
#echo $v_success_status

if grep success AutoMergePrFromFeatureToDEVIBUOutput.txt  &> /dev/null
  then
    v_return=`grep success AutoMergePrFromFeatureToDEVIBUOutput.txt`
    echo "$v_return"
elif grep mergeconflictrelease AutoMergePrFromFeatureToDEVIBUOutput.txt &> /dev/null
   then
    v_return=`grep mergeconflictrelease AutoMergePrFromFeatureToDEVIBUOutput.txt`
    echo "$v_return"
elif grep mergeconflictdev AutoMergePrFromFeatureToDEVIBUOutput.txt &> /dev/null
   then
    v_return=`grep mergeconflictdev AutoMergePrFromFeatureToDEVIBUOutput.txt`
    echo "$v_return"
 else
    echo "0|NoPrToProcess"
  fi
