# *********************************************************************************************
# Program to update dynamodb with latest data from mta feed. It also cleans up stale entried from db
# Usage python dynamodata.py
# *********************************************************************************************
import json,time,sys
from collections import OrderedDict
from threading import Thread

import boto3
from boto3.dynamodb.conditions import Key,Attr

sys.path.append('../utils')
import tripupdate,vehicle,alert,mtaUpdates,aws

### YOUR CODE HERE ####
d = mtaUpdates.mtaUpdates('aaea8eb4efba70f2800c446e446b27e3')
updates = d.getTripUpdates()
for update in updates:
    print update.tripId, update.routeId, update.startDate, update.direction
    if update.vehicleData:
    	print update.vehicleData.currentStopNumber, update.vehicleData.currentStopId, update.vehicleData.timestamp, update.vehicleData.currentStopStatus 
    print update.futureStops