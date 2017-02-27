import time,csv,sys
from pytz import timezone
from datetime import datetime

sys.path.append('../utils')
import mtaUpdates

# This script should be run seperately before we start using the application
# Purpose of this script is to gather enough data to build a training model for Amazon machine learning
# Each time you run the script it gathers data from the feed and writes to a file
# You can specify how many iterations you want the code to run. Default is 50
# This program only collects data. Sometimes you get multiple entries for the same tripId. we can timestamp the 
# entry so that when we clean up data we use the latest entry

# Change DAY to the day given in the feed
DAY = datetime.today().strftime("%A")
TIMEZONE = timezone('America/New_York')

global ITERATIONS

#Default number of iterations
ITERATIONS = 50


#################################################################
####### Note you MAY add more datafields if you see fit #########
#################################################################

# column headers for the csv file
columns =['timestamp','tripId','route','day','timeToReachExpressStation','timeToReachDestination']


def main(fileName):
    # API key
    with open('../../key.txt', 'rb') as keyfile:
        APIKEY = keyfile.read().rstrip('\n')
        keyfile.close()

	### INSERT YOUR CODE HERE ###

