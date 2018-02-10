import subprocess
import boto
import boto.s3
import time
import datetime
import re

'''
Script to get mongodump file from S3 and Restore
'''


def mongorestore(rdir):
    '''MongoRestore the Directory'''
    mongorestore_cmd = "mongorestore --port 27018 --oplogReplay " + rdir
    subprocess.check_output(mongorestore_cmd, shell=True)
    return


def get_s3_restore(name, rdir):
    """Get the latest restore file from S3"""

    # AWS ACCESS DETAILS
    AWS_ACCESS_KEY_ID = 'XXXXXXXXXX'
    AWS_SECRET_ACCESS_KEY = 'XXXXXXXXXXXXX'
    bucket_name = 'flextrip-db-dumps'

    c = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    b = c.get_bucket(bucket_name)
    rs = b.list()
    s3_check = re.compile(name)

    # Get the latest oplog filename from S3 list
    file_name = ''
    ref_time = datetime.datetime(1973, 01, 01)
    file_time = ref_time.isoformat()
    for key in rs:
    #print key.name
        if s3_check.match(key.name):
            tfile_name, tfile_time = key.name, key.last_modified
            #print tfile_name, tfile_time
            if tfile_time > file_time:
                file_name = key.name
                file_time = key.last_modified
                #print file_name, file_time

    # Now Downlaod the latest oplog file
    key = b.get_key(file_name)
    restore_filename = file_name.split('/')
    #print restored_file[-1]
    restore_file = rdir + "/" + restore_filename[-1]

    key.get_contents_to_filename(restore_file)

    return restore_file


def tar_extract(rdir, rfile):
    """Extracts the tar file in the directory"""
    tar_cmd = "tar -xf " + rfile + " -C " + rdir
    subprocess.check_output(tar_cmd, shell=True)

    return


def mv_oplog(rdir, rfile, odir):
    """Move the oplog.bson to right location"""
    mk_opdir_cmd = "mkdir -p " + odir
    mv_cmd = "mv " + rdir + "/" + rfile + "*" + "/local/oplog.rs.bson" + " " + odir + "/oplog.bson"
    subprocess.check_output(mk_opdir_cmd, shell=True)
    subprocess.check_output(mv_cmd, shell=True)

    return


def cleanup(rdir):
    """Remove the Backup dir"""
    rm_dir_cmd = "rm -rf " + rdir + "/*"
    subprocess.check_output(rm_dir_cmd, shell=True)


# Main Section
if __name__ == "__main__":

    # Main Part of the script
    restore_dir = "/data1/restore"
    oplog_dir = restore_dir + "/oplogRestore"
    s3_bdir = "daily"
    res_file_pref = "mongodump_oplog"
    cur_time = time.time()
    timestamp = datetime.datetime.fromtimestamp(cur_time).strftime('%Y-%m-%d-%u-%H-%M-%S')
    restore_file_name_pat = s3_bdir + res_file_pref

    # Store file into S3
    restored_s3file = get_s3_restore(restore_file_name_pat, restore_dir)

    # Extract the oplog content from the tar file
    tar_extract(restore_dir, restored_s3file)

    # Move the oplog.bson to right directory
    mv_oplog(restore_dir, res_file_pref, oplog_dir)

    # Perform Oplog restore
    mongorestore(oplog_dir)

    # Cleanup the files
    cleanup(restore_dir)
