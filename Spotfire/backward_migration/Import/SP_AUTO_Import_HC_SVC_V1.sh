#/* --------------------------------------------------------------------*/
#/* Description :  Calling Script to invoke import scripts at spotfire server                   */
#/* Paramter    : $ID Jenkins Request ID#                           */
#/* Paramter    : $2 SSO ID#                           */
#/* --------------------------------------------------------------------*/

ID="spotfire_data"
SSO_ID="502722160@vds.logon"
CHG=$ID
LINE_COUNT=1
input="${CHG//[$'\t\r\n ']}"
SSO="${SSO_ID//[$'\t\r\n ']}"
echo $SSO

SUCESS=0
FAILURE=0
CONFIG_KEY=`echo U2FsdGVkX18LIFeIkseS5c6vSgHzj9A3Gbr6NeC0xkc= | openssl enc -aes-128-cbc -a -d -salt -pass pass:qwerty`
echo $CONFIG_KEY

echo $input
filename="/data/impex_data/Migration_Automation/BCKPCSV/${CHG//[$'\t\r\n ']}.csv"
echo $filename
old="$IFS"
IFS=''

file='/data/impex_data/Migration_Automation/BCKPCSV/$ID.csv'

if [ -s "$file" ]
then
echo "$0: File ${file} not found please check the CC number entered"
else
IFS='
'='
'
#cd /data/impex_data/Migration_Automation/BCKPCSV
sed -i 1d $filename
for LINE in `cat $filename`

do

        IFS="$old"
          OBJECT_NAME=`echo "$LINE" | cut -f1 -d','`;
	  echo $OBJECT_NAME
          INV_TYPE=`echo $LINE | cut -f2 -d','`;
	  echo $INV_TYPE
          SRC_PATH=`echo $LINE | cut -f3 -d','`;
	  echo $SRC_PATH
          TARGT_PATH=`echo "$LINE" | cut -f4 -d','`;
	  TARGET_PATH="$(echo "$TARGT_PATH"|tr -d '\r')"
	  echo \"$TARGET_PATH\"

                echo "------------------------------------------Reading $OBJECT_NAME from line# $LINE_COUNT from the DD-------------------------------------------------------------- "
              /data/tibco/tss-10.6.0.x86_64/tomcat/spotfire-bin/config.sh import-library-content -t $CONFIG_KEY -p /data/impex_data/Migration_Automation/Imports/$INV_TYPE/"$OBJECT_NAME.part0.zip" -m keep_new -u "$SSO" -i $INV_TYPE -l "$TARGET_PATH"
                if [ $? -eq 0 ]; then
                  echo "Successfully Imported $OBJECT_NAME at $TARGET_PATH "
                  SUCESS=$((SUCESS+1))
                else
                  FAILURE=$((FAILURE+1))
                  echo "Failed to Import $OBJECT_NAME of type $INV_TYPE "
                fi
                  LINE_COUNT=$((LINE_COUNT+1))
done


          Total=$((SUCESS+FAILURE))
          echo "Total Objects Supposed to be imported as per DD is $Total"
          echo "Successfully Imported objects is $SUCESS "
          echo "Number of failed objects $FAILURE "
	
	
if [ $Total -eq $SUCESS ]
then
Status_Flag="Success"
else
Status_Flag="Failure"
fi
		  
# rm -f Msg.txt > /dev/null 2>&1
	  
# from="biautotcs@ge.com"
# subject="BI - Request ID: $ID - SPOTFIRE Production Migration Status - $Status_Flag";
# to="$mail_id"

# echo "Hello All,

# Prod migration Status of the spotfire components of request id $ID : $Status_Flag .

# Total Objects Suppose to be imported as per DD is $Total
# Successfully Imported objects is $SUCESS
# Number of failed objects $FAILURE


# Regards,
# BI Automation Team"  > Msg.txt

# cat Msg.txt | mailx -r "$from" -s "$subject"  "$to" 		  
		  
		  
		  
fi

exit;


