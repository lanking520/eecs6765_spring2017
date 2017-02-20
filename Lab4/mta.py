# *********************************************************************************************
# Usage python mta.py

import json,time,csv,sys

import boto3
from boto3.dynamodb.conditions import Key,Attr

sys.path.append('../utils')
import aws


DYNAMODB_TABLE_NAME = "mtaData"

# prompt
def prompt():
    print ""
    print ">Available Commands are : "
    print "1. plan trip"
    print "2. subscribe to messages"
    print "3. exit"  

def main():
	return 0


if __name__ == "__main__":
    main()
    

   

        
