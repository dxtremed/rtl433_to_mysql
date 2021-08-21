# Please create a mysql database for user rtl433db with create rights so table can be created
# change ip for database server
# install mysql connector
# install phython 2.7
# let it run ;)
#!/usr/bin/python
#import sys
import subprocess
import time
import threading
import Queue
import mysql.connector
from mysql.connector import errorcode

config = {
  'user': 'xMGTPKuFUF',
  'password': 'uF5D6o59iz',
  'host': 'remotemysql.com',
  'database': 'xMGTPKuFUF',
  'raise_on_warnings': True,
}

class AsynchronousFileReader(threading.Thread):
    '''
    Helper class to implement asynchronous reading of a file
    in a separate thread. Pushes read lines on a queue to
    be consumed in another thread.
    '''

    def __init__(self, fd, queue):
        assert isinstance(queue, Queue.Queue)
        assert callable(fd.readline)
        threading.Thread.__init__(self)
        self._fd = fd
        self._queue = queue

    def run(self):
        '''The body of the tread: read lines and put them on the queue.'''
        for line in iter(self._fd.readline, ''):
            self._queue.put(line)

    def eof(self):
        '''Check whether there is no more content to expect.'''
        return not self.is_alive() and self._queue.empty()

def replace(string):
    while '  ' in string:
        string = string.replace('  ', ' ')
    return string

def startsubprocess(command):
    '''
    Example of how to consume standard output and standard error of
    a subprocess asynchronously without risk on deadlocking.
    '''
    print "\n\nStarting sub process " + command + "\n"
    # Launch the command as subprocess.

    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Launch the asynchronous readers of the process' stdout and stderr.
    stdout_queue = Queue.Queue()
    stdout_reader = AsynchronousFileReader(process.stdout, stdout_queue)
    stdout_reader.start()
    stderr_queue = Queue.Queue()
    stderr_reader = AsynchronousFileReader(process.stderr, stderr_queue)
    stderr_reader.start()
    # do database stuff init
    try:
        print("Connecting to database")
        cnx = mysql.connector.connect(**config)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exists, please create it before using this script.")
            print("Tables can be created by the script.")
        else:
            print(err)
    reconnectdb=0#if 0 then no error or need ro be reconnected


    cursor = cnx.cursor()
    TABLES = {}
    TABLES['TempHumData'] = (
        "CREATE TABLE `TempHumData` ("
        "  `timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
	"  `sensorid` CHAR(2) UNSIGNED NOT NULL,"
        "  `temperature` DECIMAL(5,2) NOT NULL,"
        "  `humidity` DECIMAL(5,2) NOT NULL"
        ") ENGINE =InnoDB DEFAULT CHARSET=latin1")
    for name, ddl in TABLES.iteritems():
        try:
            print("Checking table {}: ".format(name))
            cursor.execute(ddl)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("Table seams to exist, no need to create it.")
            else:
                print(err.msg)
        else:
            print("OK")
    add_sensordata= ("INSERT INTO TempHumData "
                     "(timestamp, sensorid, temperature, humidity) "
                     "VALUES (%s, %s, %s, %s)")

    # do queue loop, entering data to database
    # Check the queues if we received some output (until there is nothing more to get).
    # add time limit for 1 min
    nowTime = time.time()
    endTime = time.time() + 61
    while (not stdout_reader.eof() or not stderr_reader.eof()) and not nowTime > endTime:
	# Show what we received from standard output.
        while not stdout_queue.empty():

            line = replace(stdout_queue.get())

            if ('LaCrosse-TX141THBv2'in line):
                print '======== LaCrosse EVENT ========'
		#print "Time : %s" % time.ctime()
		#Data starts after ": "
		myData=line.split(': ')
		#igrone item 0		
		myData=myData[1]
		#split to different types of information, e.g. Temperature, Channel		
		myData=myData.split(', ')

                for myText in myData:
			print myText
			myTemp=myText.split(' ')
                        if 'time'in myText:
				time=myTemp[1]                        
			elif 'Sensor ID'in myText:
                                sensorid=myTemp[1]
                        elif 'Temperature'in myText:
                                temperature=myTemp[1]
                        elif 'Humidity'in myText:
                                humidity=myTemp[1]
		#######################
                #last field, put in db
                # UPDATE DB
                #########################
                if reconnectdb:
                    #need to reconnect as there was a problem on last update
		    reconnectdb=0
		    try:
			print("Reconnecting to database")
			cnx.reconnect()
		    except mysql.connector.Error as err:
			print "Error happened during reconnection @ %s" % time.ctime()
			print(err.msg)
                try:
                    sensordata = (time,sensorid,temperature,humidity)
                    cursor.execute(add_sensordata,sensordata)
                    # Make sure data is committed to the database
                    cnx.commit()
                except:
                    reconnectdb=1
                    print("Error connecting to database")

		
            else:
                print "stdout: " + str(line) #Print stuff without processing



        # Show what we received from standard error.
        while not stderr_queue.empty():
            line = replace(stderr_queue.get())

            print str(line) #Print stuff without processing


        # Sleep a bit before asking the readers again.
	# print("entering wait state")
        time.sleep(0.1)
	nowTime=time.time()

    # Let's be tidy and join the threads we've started.
    print ("Cleaning")
    try:
        cursor.close()
	print("cursor.close() completed")
        cnx.close()
	print("cnx.close() completed")
    except:
	print("except")
        pass


    # Close subprocess' file descriptors.
    print ("process.close()")
    process.terminate()
    print ("stdout_reader.close completed")
    process.wait()
    print ("Cleaned")

if __name__ == '__main__':
    # The main flow:
        #check if database is present, create tablesif no tables present

    #loop every 5 min
    while 0==0:
    	startsubprocess("./rtl_433")
	print "sleeping 5 min : %s" % time.ctime()
	time.sleep(300)
	print "End : %s" % time.ctime()    	
    print("Closing down")
