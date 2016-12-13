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
 
# Unlock and delete lock file.
def un_lock():
    lock.close()
    os.remove(lockfile)
 
#DB auth credentials
db_login="backup"
db_pass="Ew7UAv12enOROikRasL3tk"

class MongoDB:
    mongodb_list = []
    
 
    def __init__(self):
        self.db_name = db_name
        self.mongodb_list.append(self)
    
    def mongo_backup():
		
		
	archive_path = os.path.join(storage_dir, self.db_name)
	zip_name = os.path.join(archive_path, "%s.gz" % archive_name)

	logging.info("Running mongodump for MongoDB Instance MongoC04 , dumptime: %s" % ( backup_time))
	archive_name = self.db_name + '_' + backup_time
	
	archive_path = os.path.join(storage_dir, archive_name)
	check_dir(archive_path)
	gz_name = os.path.join(archive_path, "%s.gz" % archive_name)
	logging.info("Archive path  %s " % (archive_path))
        
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
                 
 
	def disk_clean_up(db_name):  # Delete old archive backup files when free disk space is less than 15%
	    logging.info("Starting disk_clean_up function for %s" % db_name)
	    cleanup_path = os.path.join(cleanup_dir, db_name)
	    a = []
	    for files in os.listdir(cleanup_path):
		a.append(files)
         
	    if len(a) > 6 :
		a.sort()
		filetodel = a[0]
		del a[0]
		os.remove(os.path.join(cleanup_path, filetodel))
		logging.info("Not enough free disk space. Cleanup process started.File to Del %s" % filetodel)
	    else :
		logging.error("Disk cleanup failed. Nothing to delete.")
		un_lock()
		sys.exit("Disk cleanup failed. Nothing to delete.")
	    
	    def mongo_clean_up(self):
		    archive_path = os.path.join(storage_dir, self.db_name)
		    a = []
	
		    check_dir(archive_path) 
			 
		    for files in os.listdir(archive_path): 
			a.append(files)               
	 
		    while len(a) > max_backups:
			a.sort()
			filetodel = a[0]
			del a[0]
			os.remove(os.path.join(archive_path,filetodel))
			logging.info("Starting cleanup process. File %s was deleted from directory %s" % (filetodel, archive_path))
			logging.info("Cleanup Done. Total files:%d in Backup Directory %s" % (len(a), self.db_name))	
                     
 
"""Script run start's here"""
 
# Check, if file is locked and exits, if true
if os.path.exists(lockfile):
    logging.info("Another instance of this script is Running")
    sys.exit("Another instance of this script is Running")
else:
    lock = zc.lockfile.LockFile(lockfile, content_template='{pid}; {hostname}')
 
# Start cleaning working directory
logging.info("Cleaning working directory")
if os.path.exists(work_dir):
    rmtree(work_dir) # Remove all files in work_dir                                       
                                     
# Connect to Mongodb. Get list of all database names
db_conn = MongoClient('localhost', 27017)
db_conn.the_database.authenticate('backup','Ew7UAv12enOROikRasL3tk', source='admin')
db_names = db_conn.database_names()
db_conn.admin.command("fsync", lock=True)

# Checks free disk space and cleans storage directory  if disk usage is higher than 77%
#while get_disk_space() > need_free_disk_space:
    #try:
        #for db_name in db_names:
            #cleanup_path = os.path.join(cleanup_dir, db_name)
            #if not os.path.exists(cleanup_path):
                #continue
            #else:
                #disk_clean_up(db_name)
    #except AssertionError, msg:
        #logging.error(msg)
# Get Backup time.
backup_time = datetime.datetime.today().strftime('%Y-%m-%d_%H-%M-%S')

for db_name in db_names:
    if db_name not in exclude_db:
        try:
            db_name = MongoDB()
            db_name.mongo_backup() 
        except AssertionError, msg:
            logging.error(msg)
          
for db_name in MongoDB.mongodb_list:
    try:
        db_name.mongo_clean_up()
    except AssertionError, msg:
        logging.error(msg)
         
         
# Unlocking and deleting temp file
un_lock()

#Unlocking MongoDB
logging.info("Unlocking MongoDB Instance")
try:
    db_conn.admin['$cmd'].sys.unlock.find_one()
except AssertionError, msg:
	logging.error(msg)
# Final Message
logging.info("All task's for current backup schedule done.")
