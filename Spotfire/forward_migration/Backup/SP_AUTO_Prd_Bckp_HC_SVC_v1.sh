#/* --------------------------------------------------------------------*/
#/* Description :  Script to export Spotfire inventory from Prod                    */
#/* Paramter    : $1 Jenkins Request ID#                           */
#/* Paramter    : $2 SSO ID                           */
#/* --------------------------------------------------------------------*/

ID="spotfire_data"
CHG=$ID
input="${CHG//[$'\t\r\n ']}"
#SSO="${2//[$'\t\r\n ']}"
echo "$input"
LINE_COUNT=1
EXISTING_SUCESS=0
EXISTING_FAIL=0
TOTAL_EXISTING_OBJECTS=0
filename="/data/impex_data/Migration_Automation/${CHG//[$'\t\r\n ']}.csv"

#dos2unix /data/impex_data/Migration_Automation/backup/$ID.csv

CONFIG=`echo U2FsdGVkX196UT1gG1i3qlPQqP9K/ZopXrNj7kvmQI4= | openssl enc -aes-128-cbc -a -d -salt -pass pass:qwerty`

old="$IFS"
IFS=''

file='/data/impex_data/Migration_Automation/$ID.csv'


IFS='
'
cd /data/impex_data/Migration_Automation/backup/
sed -i 1d $filename
for LINE in `cat $filename`

do

        IFS="$old"
        #cd /tibco/tss-7.11.1.x86_64/tomcat/bin;
        OBJECT_NAME=`echo "$LINE" | cut -f1 -d','`;
	OBJ_NAME="$(echo "$OBJECT_NAME"|tr -d '\r')"
        INV_TYPE=`echo $LINE | cut -f2 -d','`;
        SRC_PATH=`echo "$LINE" | cut -f3 -d','`;
	SOURCE_PATH="$(echo "$SRC_PATH"|tr -d '\r')"
        TARGT_PATH=`echo "$LINE" | cut -f4 -d','`;
	
	echo "$OBJ_NAME"
        echo  "$INV_TYPE"
        echo  "$SOURCE_PATH"
		echo "$TARGT_PATH"

 echo "------------------------------------------Reading $OBJECT_NAME from line# $LINE_COUNT from the DD-------------------------------------------------------------- "
/data/tibco/tss-10.6.0.x86_64/tomcat/spotfire-bin/config.sh export-library-content -f -t "$CONFIG" -p /data/impex_data/Migration_Automation/backup/"$ID"/$INV_TYPE/"$OBJECT_NAME" -u 502722160@vds.logon -i $INV_TYPE -l "$SOURCE_PATH/$OBJ_NAME"
               if [ $? -eq 0 ]; then
                  EXISTING_SUCESS=$((EXISTING_SUCESS+1))
                    echo "-----------------------Successfully Exported $OBJECT_NAME of type $INV_TYPE--------------------------------------"
                 else
                   echo "------------------------Failed to Export $OBJECT_NAME of type $INV_TYPE please check $LINE_COUNT--------------------"
                  EXISTING_FAIL=$((EXISTING_FAIL+1))
               fi
                LINE_COUNT=$((LINE_COUNT+1))

done
	
       Total=$((EXISTING_FAIL+EXISTING_SUCESS))
      echo "Total Objects present at DD is $Total"
      echo "Backup created successfully for $EXISTING_SUCESS objects"
      echo "Failed to create backup for $EXISTING_FAIL objects since object might be new / object details is invalid "

exit;

