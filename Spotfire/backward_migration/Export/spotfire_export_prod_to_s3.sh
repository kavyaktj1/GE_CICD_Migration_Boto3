#!/bin/bash

year=`date +%Y`;month=`date +%m`;day=`date +%d`
aws s3 cp /data/impex_data/Migration_Automation/spotfire_data/ s3://odp-us-prod-spotfire/CICD-502820443/spotfire/prod/$year/$month/$day/ --recursive
