#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Backup script Made for Abbyy-LS
# Script connects to MongoDB, gets all db names, then for each database except "local" performs mongodump and archive's the result  to our backup storage directory.
# Script checks our storage directory and if free disk space is less than 15% - performs Disk Clean up.
 
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

exclude_db = ('local') 
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
 
# Check disk space usage
def get_disk_space():
    disk_space = psutil.disk_usage(storage_dir)
    return disk_space.percent
     
# Key options for script launch
parser = argparse.ArgumentParser(description='Backup schedule options - Monthly,Weekly,Daily')
parser.add_argument('--monthly', '-m', action="store_true", help='Option for Monthly Backup')
parser.add_argument('--weekly', '-w', action="store_true", help='Option for Weekly Backup')
parser.add_argument('--daily', '-d', action="store_true", help='Option for Daily Backup')

parser.add_argument('--pwd', '-p', action="store", help='Option for password')
parser.add_argument('--user', '-u', action="store", help='Option for user')
                                      
args = parser.parse_args()
 
# Checking input arguments
if args.monthly:
    storage_dir = "/backup/mongodbbackup/storage/monthly"
    check_dir(storage_dir) 
    max_backups = 2
    need_free_disk_space = 70
    logging.info("Starting monthly MongoDB backup")
elif args.weekly:
    storage_dir = "/backup/mongodbbackup/storage/weekly"
    check_dir(storage_dir)
    max_backups = 4
    need_free_disk_space = 70
    logging.info("Starting weekly MongoDB backup")
elif args.daily:
    storage_dir = "/backup/mongodbbackup/storage/daily"
    check_dir(storage_dir)  
    max_backups = 10
    need_free_disk_space = 68
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
    
#lock/unlock Mongod Instance
def mongod_fsync_lock():
    backup_output = subprocess.check_call( 
            [
                'mongo',
                '-u', '%s' % db_login,
                '-p', '%s' % db_pass,
                '--authenticationDatabase','%s' % 'admin',	                
                '--eval',
                "db.fsyncLock()"
            ])

def mongod_fsync_unlock():
    backup_output = subprocess.check_call( 
            [
                'mongo',
                '-u', '%s' % db_login,
                '-p', '%s' % db_pass,
                '--authenticationDatabase','%s' % 'admin',	                
                '--eval',
                "db.fsyncUnlock()"
            ])

	    
#Check if Mongod is locked
def Mongod_is_locked():
    db_conn = MongoClient('localhost', 27017)
    db_conn.the_database.authenticate(db_login, db_pass, source='admin')
    db = db_conn.admin 
    current_ops = db.current_op();
    if ((hasattr(current_ops,"fsyncLock")) and (current_ops.fsyncLock)):
	logging.error("Checking if Instance is Locked. Result: MongoDB is Locked ")
    else: 
	logging.info("Checking if Instance is Locked. Result: MongoDB isn't Locked ")
        

#def get_attr():
    #if hasattr(current_ops,'fsyncLock'):
	#return true
    #else:
	#print "Doesn't Exists"
	#return false



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
	    rmtree(os.path.join(cleanup_path, dirtodel))
	    logging.info("Not enough free disk space. Cleanup process started.File to Del %s" % dirtodel)
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
 
# Start cleaning working directory
#logging.info("Cleaning working directory")
#if os.path.exists(work_dir):
#    rmtree(work_dir) # Remove all files in work_dir                                       
Mongod_is_locked()

# Connect to Mongodb. Get list of all database names
db_conn = MongoClient('localhost', 27017)
db_conn.the_database.authenticate(db_login, db_pass, source='admin')
db_names = db_conn.database_names()

#Locking Mongod Instance
try:
    mongod_fsync_lock()
    logging.info("Mongo Instance Is locked!")
except AssertionError, msg:
    logging.error(msg)

# Checks free disk space and cleans storage directory  if disk usage is higher than 77%
while get_disk_space() > need_free_disk_space:
    try:
        disk_clean_up()
    except AssertionError, msg:
        logging.error(msg)
# Get Backup time.

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
         
         
# Unlocking and deleting temp file
un_lock()

#Unlocking MongoDB
logging.info("Unlocking MongoDB Instance")
try:
    mongod_fsync_unlock()
except AssertionError, msg:
    logging.error(msg)
    
# Final Message
logging.info("All task's for current backup schedule done.")
