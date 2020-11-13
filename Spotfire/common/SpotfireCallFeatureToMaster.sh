#!/bin/bash
python3 Spotfire/Scripts/SpotfireMergeFromFeatureToMaster.py > SpotfireMergePrFromFeatureToMaster.txt
##v_return=`python3 Scripts/AutoMergePrFromFeatureToDev.py`
#echo $v_return
##v_trim_return=`echo $v_return|cut -d' ' -f2`
##echo $v_trim_return
#v_pr_number=`echo $v_trim_return|cut -d '|' -f1`
#v_success_status=`echo $v_trim_return|cut -d '|' -f2`

#echo $v_pr_number
#echo $v_success_status

if grep success SpotfireMergePrFromFeatureToMaster.txt  &> /dev/null
  then
    v_return=`grep success SpotfireMergePrFromFeatureToMaster.txt`
    echo "$v_return"
elif grep mergeconflictmaster SpotfireMergePrFromFeatureToMaster.txt &> /dev/null
   then
    v_return=`grep mergeconflictmaster SpotfireMergePrFromFeatureToMaster.txt`
    echo "$v_return"
elif grep zeroapprove SpotfireMergePrFromFeatureToMaster.txt &> /dev/null
   then
    v_return=`grep zeroapprove SpotfireMergePrFromFeatureToMaster.txt`
    echo "$v_return"
 else
    echo "0|NoPrToProcess"
  fi
