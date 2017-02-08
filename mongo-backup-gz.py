#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Beta version. Date 25.01.2017 
# Backup script Made for Abbyy-LS by D.Chernyakov
# Script connects to MongoDB, gets all db names, then for each database except "local" performs mongodump with archive key and saves the result  to our backup storage directory.

 
import sys
import os
import time
import argparse
import logging
import datetime
import subprocess
import psutil
import zc.lockfile
from shutil import copyfile, rmtree, copytree, move
from pymongo import MongoClient

exclude_db = ('local','et_api') 
db_path = "/datadisk/"
work_dir = "/backup/mongodbbackup/work/"
cleanup_dir = "/backup//mongodbbackup/storage/daily"
lockfile = "/tmp/mongo-backup.lock"
logfile = '/var/log/mongodb/mongo-backup.log'
backup_time = datetime.datetime.today().strftime('%Y-%m-%d_%H-%M-%S')
 
logging.basicConfig(format=u'%(levelname)-8s [%(asctime)s]  %(message)s', datefmt='%m/%d/%Y %H:%M:%S-%Z', filename = logfile , level=logging.INFO)
 
# Check if  directory exists? otherwise creates it
def check_dir(path):
    if not os.path.exists(path): 
        os.makedirs(path)
 
def get_size(folder):
    total_size = 0
    if os.path.exists(folder):
	for dirpath, dirnames, filenames in os.walk(folder):
	    for f in filenames:
		fp = os.path.join(dirpath, f)
		total_size += os.path.getsize(fp)
		final_size = total_size / 1024 / 1024
	logging.info("Final size of a %s is %s. We will multiply result by 1,5 %s Mbytes" % (folder, total_size,final_size*1.5))
	return total_size*1.5 #Returns the size of a folder in bytes
    else:
	return 0

# Check disk space usage
def check_disk_space(folder):
    check_dir(folder)
    free_disk_space = psutil.disk_usage(storage_dir)
    if (get_size(folder) > free_disk_space.free):
	logging.error("Last backup size %s Bytes is greater than free disk space %s Bytes " % (get_size(folder),free_disk_space.free))
        return True
    else:
	logging.info("Free space %s Bytes is greater than last backup size %s Bytes" % (free_disk_space.free, get_size(folder)))
        return False
    
def get_last_backup(folder):
    a = []
    for dirs in os.listdir(folder): 
	    a.append(dirs)
	    logging.info("Array len: %s , Dirs %s" % (len(a), dirs))
	    a.sort()
	    if len(a) > 0:
		last_backup = a[-1]
		path = os.path.join(folder,last_backup)
		logging.info("Last backup. %s, path %s" % (last_backup, path))
		return path
	    else:
		return db_path
     
# Key options for script launch
parser = argparse.ArgumentParser(description='Backup schedule options - Monthly,Weekly,Daily')
parser.add_argument('--monthly', '-m', action="store_true", help='Option for Monthly Backup')
parser.add_argument('--weekly', '-w', action="store_true", help='Option for Weekly Backup')
parser.add_argument('--daily', '-d', action="store_true", help='Option for Daily Backup')

# Getting Auth DB credentials 
parser.add_argument('--pwd', '-p', action="store", help='Option for password')
parser.add_argument('--user', '-u', action="store", help='Option for user')
                                      
args = parser.parse_args()
 
# Checking input arguments
if args.monthly:
    storage_dir = "/backup/mongodbbackup/storage/monthly"
    check_dir(storage_dir)
    max_backups = 2
    logging.info("Starting monthly MongoDB backup")
elif args.weekly:
    storage_dir = "/backup/mongodbbackup/storage/weekly"
    check_dir(storage_dir)
    max_backups = 4
    logging.info("Starting weekly MongoDB backup")
elif args.daily:
    storage_dir = "/backup/mongodbbackup/storage/daily"
    check_dir(storage_dir)
    max_backups = 100
    logging.info("Starting daily MongoDB backup")   
else:
    logging.info("Please specify key arguments.--monthly - Option for Monthly Backup,--weekly - Option for Weekly Backup , -daily - Option for Daily Backup")
    sys.exit("Please specify key arguments.--monthly - Option for Monthly Backup,--weekly - Option for Weekly Backup , -daily - Option for Daily Backup")   
 
#Pase DB auth credentials
db_pass = args.pwd
db_login = args.user

# Unlock and delete lock file.
def un_lock():
    lock.close()
    os.remove(lockfile)

def check_fsync_lock(conn):
    if conn.is_locked is True:
	logging.error("Check MongoDB Instance. MongoDB Instance Lock state: %s" % conn.is_locked)
    else:
	logging.info("Lock state: %s" % conn.is_locked)

def mongo_fsync_lock(conn, state):  
        if state is True:
	    conn.fsync(lock=True)
        if state is False:
            conn.unlock()
        
	
        

class MongoDB:
    mongodb_list = []
    
 
    def __init__(self):
        self.db_name = db_name
        self.mongodb_list.append(self)
    
    def mongo_backup(self):
	
	logging.info("Running mongodump for MongoDB Instance MongoC04 Database: %s, dumptime: %s" % (self.db_name, backup_time))
	archive_name = self.db_name + '_' + backup_time
	
	archive_path = os.path.join(storage_dir, backup_time)
	check_dir(archive_path)
	gz_name = os.path.join(archive_path, archive_name)
	logging.info("Archive name  %s " % (gz_name))
        
	try:
	    backup_output = subprocess.check_call(  # Run Mongodump for each Database
	        [
	            'mongodump',
	            '-u', '%s' % db_login,
	            '-p', '%s' % db_pass,
	            '--authenticationDatabase','%s' % 'admin',	                
	            '--db', '%s' % self.db_name,
	            #'--oplog',
	            '--gzip',
	            '--out=%s' % archive_path
	        ])
	except subprocess.CalledProcessError as e:
	    logging.error("Failed to run mongodump. Output Error %s" % e.output)
	    un_lock()
	    sys.exit("Failed to run mongodump. Output Error %s" % e.output)       
	logging.info("Mongodump for DB Instance  ended Successfully" )
	    
def mongo_clean_up():
    #archive_path = os.path.join(storage_dir, backup_time)
    a = []
    #check_dir(storage_dir) 
                 
    for dirs in os.listdir(storage_dir): 
	a.append(dirs)               
 	while len(a) > max_backups:
	    a.sort()
	    dirtodel = a[0]
	    del a[0]
	    rmtree(os.path.join(storage_dir,dirtodel))
	    logging.info("Starting cleanup process. File %s was deleted from directory %s" % (dirtodel, storage_dir))
	    logging.info("Cleanup Done. Total Backups:%d in Backup Directory" % len(a))	
                 
 
def disk_clean_up():  # Delete old archive backup files when free disk space is less than 15%
	logging.info("Starting disk_clean_up function for %s" % cleanup_dir)
	#cleanup_path = os.path.join(cleanup_dir, backup_time)
	a = []
	for dirs in os.listdir(cleanup_dir):
	    a.append(dirs)
         
	if len(a) > 6 :
	    a.sort()
	    dirtodel = a[0]
	    del a[0]
	    logging.info("Removing Directory %s" % dirtodel)
	    rmtree(os.path.join(cleanup_dir, dirtodel))
	    logging.info("Done. Cleanup process started.Directory was Deleted %s" % dirtodel)
	else :
	    logging.error("Disk cleanup failed. Nothing to delete.")
	    un_lock()
	    sys.exit("Disk cleanup failed. Nothing to delete.")
	    	
                     
 
"""Script run start's here"""
 
# Check, if file is locked and exits, if true
if os.path.exists(lockfile):
    logging.info("Another instance of this script is Running")
    sys.exit("Another instance of this script is Running")
else:
    lock = zc.lockfile.LockFile(lockfile, content_template='{pid}; {hostname}')
 


# Connect to Mongodb. Get list of all database names
db_conn = MongoClient('localhost', 27017)
db_conn.the_database.authenticate(db_login, db_pass, source='admin')
db_names = db_conn.database_names()

# Checkin if MongoDB is locked.If result is true - Problem with Replication.Need to check Replication status 
check_fsync_lock(db_conn)

#to lock
try:
    mongo_fsync_lock(db_conn, True)
    logging.info("Locking Mongod Instance!")
except AssertionError, msg:
    logging.error(msg)

 
# Checks free disk space and cleans storage directory  if disk usage is higher than 77%
while check_disk_space(get_last_backup(cleanup_dir)):
    try:
        disk_clean_up()
    except AssertionError, msg:
        logging.error(msg)

# Backup
for db_name in db_names:
    if db_name not in exclude_db:
        try:
            db_name = MongoDB()
            db_name.mongo_backup() 
        except AssertionError, msg:
            logging.error(msg)

mongo_clean_up() 

#for db_name in MongoDB.mongodb_list:
    #try:
        #db_name.mongo_clean_up()
    #except AssertionError, msg:
        #logging.error(msg)
         
         


#Unlocking MongoDB
logging.info("Unlocking MongoDB Instance")
try:
    mongo_fsync_lock(db_conn, False)
    logging.info("Unlocking Mongod Instance!")
except AssertionError, msg:
    logging.error(msg)

# Unlocking and deleting temp file
un_lock()# Final Message

logging.info("All task's for current backup schedule done.")
