#!/bin/bash

year=`date +%Y`;month=`date +%m`;day=`date +%d`
aws s3 cp s3://odp-us-prod-spotfire/CICD-prod/backup/$year/$month/$day/ /data/impex_data/Migration_Automation/Imports/  --recursive