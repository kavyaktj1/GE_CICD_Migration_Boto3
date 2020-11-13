#!/bin/bash
python3 Scripts/Github_Merge_NPROD/ODP_Commercial_Intensity/AutoMergePrFromFeatureToDev.py > AutoMergePrFromFeatureToDEVCIOutput.txt
##v_return=`python3 Scripts/AutoMergePrFromFeatureToDev.py`
#echo $v_return
##v_trim_return=`echo $v_return|cut -d' ' -f2`
##echo $v_trim_return
#v_pr_number=`echo $v_trim_return|cut -d '|' -f1`
#v_success_status=`echo $v_trim_return|cut -d '|' -f2`

#echo $v_pr_number
#echo $v_success_status

if grep success AutoMergePrFromFeatureToDEVCIOutput.txt  &> /dev/null
  then
    v_return=`grep success AutoMergePrFromFeatureToDEVCIOutput.txt`
    echo "$v_return"
elif grep mergeconflictrelease AutoMergePrFromFeatureToDEVCIOutput.txt &> /dev/null
   then
    v_return=`grep mergeconflictrelease AutoMergePrFromFeatureToDEVCIOutput.txt`
    echo "$v_return"
elif grep mergeconflictdev AutoMergePrFromFeatureToDEVCIOutput.txt &> /dev/null
   then
    v_return=`grep mergeconflictdev AutoMergePrFromFeatureToDEVCIOutput.txt`
    echo "$v_return"
 else
    echo "0|NoPrToProcess"
  fi
