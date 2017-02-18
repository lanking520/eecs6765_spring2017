# *********************************************************************************************
# Program to update dynamodb with latest data from mta feed. It also cleans up stale entried from db
# Usage python dynamodata.py
# *********************************************************************************************
import json,time,sys
from collections import OrderedDict
from threading import Thread
from datetime import datetime
from pytz import timezone

import boto3
from boto3.dynamodb.conditions import Key,Attr

sys.path.append('../utils')
import tripupdate,vehicle,alert,mtaUpdates,aws

### YOUR CODE HERE ####

def configuration():
	ACCOUNT_ID = '031633745261'
	IDENTITY_POOL_ID = 'us-west-2:ad98af0c-3377-41b9-b469-1fa207c32bec'
	ROLE_ARN ='arn:aws:iam::031633745261:role/Cognito_edisonDemoKinesisUnauth_Role'
	DYNAMODB_TABLE_NAME='mtaData'

	client = boto3.client('cognito-identity')
	IdentityId = client.get_id(AccountId=ACCOUNT_ID,IdentityPoolId=IDENTITY_POOL_ID)
	identity = boto3.client('cognito-identity', region_name='us-west-2')
	response = identity.get_credentials_for_identity(IdentityId=IdentityId['IdentityId'])
	access_key = response['Credentials']['AccessKeyId']
	secret_key = response['Credentials']['SecretKey']

	dynamodb = boto3.client('dynamodb',region_name='us-west-2')
	#print dir(dynamodb)
	try:
		dynamodb.describe_table(TableName=DYNAMODB_TABLE_NAME)
	except:
		dynamodb.create_table(TableName=DYNAMODB_TABLE_NAME,
			KeySchema=[
			{
			'AttributeName': 'tripId',
			'KeyType': 'HASH'
			}
			],
			AttributeDefinitions=[
			{
			'AttributeName': 'tripId',
			'AttributeType': 'S'
			}
			],
			ProvisionedThroughput={
			'ReadCapacityUnits': 10,
			'WriteCapacityUnits': 10
			}
			)
		print "Creating table... Wait for 10 sec"
		time.sleep(10)
	dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
	table = dynamodb.Table(DYNAMODB_TABLE_NAME)
	return table

def getnewdata():
	d = mtaUpdates.mtaUpdates('aaea8eb4efba70f2800c446e446b27e3')
	updates = d.getTripUpdates()
	return updates

def addnewdata(table,updates):
	with table.batch_writer() as batch:
		for update, times in updates:  
			if not update.vehicleData:
				update.vehicleData = vehicle.vehicle()
			batch.put_item(Item= {
				"tripId":update.tripId,
				"routeId": update.routeId,
				"startDate":update.startDate,
				"direction":update.direction,
				"futureStops": update.futureStops,
				"currentStopId":update.vehicleData.currentStopId,
				"currentStopStatus":update.vehicleData.currentStopStatus,
				"vehicleTimeStamp":str(update.vehicleData.timestamp),
				"timeStamp" : times
			})
		print "Finish writing " + str(len(updates)) + " lines of data"

def deletedata(table):
	bar = int(time.time() - 120)
	response = table.scan(FilterExpression=Attr('timeStamp').lt(bar))
	items = response['Items']
	with table.batch_writer() as batch:
		for item in items:
			batch.delete_item(Key={'tripId':item['tripId']})
		print "Finish deleting " + str(len(items)) + " lines of data"

table = configuration()
updates = getnewdata()
addnewdata(table, updates)
deletedata(table)

