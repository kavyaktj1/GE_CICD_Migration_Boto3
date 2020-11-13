#!/bin/bash

year=`date +%Y`;month=`date +%m`;day=`date +%d`
aws s3 cp /data/impex_data/Migration_Automation/backup/ s3://odp-us-innovation-spotfire/CICD-dev/backup/$year/$month/$day/ --recursive