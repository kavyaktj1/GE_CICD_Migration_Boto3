#/* --------------------------------------------------------------------*/
#/* Created By  :  Pawan Vishwakarma                                    */
#/* Creted  On  :  14/Feb/2019                                          */
#/* Description :  Script to export Spotfire inventory at development   */
#/* Paramter    : $1 Jenkins Request Id#                                */
#/* --------------------------------------------------------------------*/

ID="spotfire_data"
CHG=$ID
LINE_COUNT=0
input="${CHG//[$'\t\r\n ']}"
filename="/data/impex_data/AUTOMATION/${CHG//[$'\t\r\n ']}.csv"



rm -rf /data/impex_data/Migration_Automation/"$ID"
mkdir -p /data/impex_data/Migration_Automation/"$ID"
chmod 777 /data/impex_data/Migration_Automation/"$ID"
mkdir -p /data/impex_data/Migration_Automation/"$ID"/all_items
chmod 777 /data/impex_data/Migration_Automation/"$ID"/all_items
mkdir -p /data/impex_data/Migration_Automation/"$ID"/dataconnection
chmod 777 /data/impex_data/Migration_Automation/"$ID"/dataconnection
mkdir -p /data/impex_data/Migration_Automation/"$ID"/datafunction
chmod 777 /data/impex_data/Migration_Automation/"$ID"/datafunction
mkdir -p /data/impex_data/Migration_Automation/"$ID"/datasource
chmod 777 /data/impex_data/Migration_Automation/"$ID"/datasource
mkdir -p /data/impex_data/Migration_Automation/"$ID"/dxp
chmod 777 /data/impex_data/Migration_Automation/"$ID"/dxp
mkdir -p /data/impex_data/Migration_Automation/"$ID"/filter
chmod 777 /data/impex_data/Migration_Automation/"$ID"/filter
mkdir -p /data/impex_data/Migration_Automation/"$ID"/join
chmod 777 /data/impex_data/Migration_Automation/"$ID"/join
mkdir -p /data/impex_data/Migration_Automation/"$ID"/procedure
chmod 777 /data/impex_data/Migration_Automation/"$ID"/procedure
mkdir -p /data/impex_data/Migration_Automation/"$ID"/query
chmod 777 /data/impex_data/Migration_Automation/"$ID"/query
mkdir -p /data/impex_data/Migration_Automation/"$ID"/sbdf
chmod 777 /data/impex_data/Migration_Automation/"$ID"/sbdf
mkdir -p /data/impex_data/Migration_Automation/"$ID"/connectiondatasource
chmod 777 /data/impex_data/Migration_Automation/"$ID"/connectiondatasource

if [ ! -d /data/impex_data/Migration_Automation/"$ID" ]; then
        exit 1
else
        echo " Folder "$ID" Created at Development server"
fi

  PASWD=`cat /data/impex_data/AUTOMATION/old_pass.txt`
  CONFIG=`echo $PASWD | openssl enc -aes-128-cbc -a -d -salt -pass pass:qwerty`
  echo $input
  echo "$filename Trying to Fetch csv file form the server"
  old="$IFS"
  IFS=''
  file='/data/impex_data/AUTOMATION/$ID.csv'
  if [ -s "$file" ]
  then
  echo "$0: File ${file} not found please rename csv file with CC number."
  else
IFS='
'='
'
    cd /data/impex_data/AUTOMATION
    sed -i 1d $filename
     for LINE in `cat $filename`
      do
        #cd /data/tibco/tss-10.6.0.x86_64/tomcat/spotfire-bin
	IFS="$old"
        LINE_COUNT=$((LINE_COUNT+1))
        OBJECT_NAME=`echo "$LINE" | cut -f1 -d','`;
        INV_TYPE=`echo $LINE | cut -f2 -d','`;
        SRC_PATH=`echo "$LINE" | cut -f3 -d','`;
        INV_STATUS=`echo $LINE | cut -f5 -d','`;
        echo "$OBJECT_NAME"
        echo  "$INV_TYPE"
        echo  "$SRC_PATH"

        echo "------------------------------------------Reading $OBJECT_NAME from line# $LINE_COUNT from the DD-------------------------------------------------------------- "
        /data/tibco/tss-10.6.0.x86_64/tomcat/spotfire-bin/config.sh export-library-content -f -t "$CONFIG" -p /data/impex_data/Migration_Automation/"$ID"/$INV_TYPE/"$OBJECT_NAME" -u 502722160@vds.logon -i $INV_TYPE -l "$SRC_PATH/$OBJECT_NAME"
          if [ $? -eq 0 ]; then
            #cd /data/impex_data/Migration_Automation/$input/$INV_TYPE
           # svn --no-auth-cache --force add "$OBJECT_NAME".part0.zip --username=$Username --password=$Password
            SUCESS_COUNT=$((SUCESS_COUNT+1))
            echo "-----------------------------------------Successfully Exported $OBJECT_NAME of type $INV_TYPE----------------------------------------------------------------------"
		  else
            FAILURE_COUNT=$((FAILURE_COUNT+1))
            echo "------------------------------------------Failed to Export $OBJECT_NAME of type $INV_TYPE please check line number $LINE_COUNT-------------------------------------------------------------------"
            exit 1
          fi
      done
      cp /data/impex_data/AUTOMATION/$input.csv /data/impex_data/Migration_Automation/$input
      cd /data/impex_data/Migration_Automation/$input
     Total=$((SUCESS_COUNT+FAILURE_COUNT))
      echo "Total Objects present at DD is $Total"
      echo "Successfully exported objects is $SUCESS_COUNT"
       fi

exit;
