import argparse
import subprocess
import time
import datetime
import s3_multipart_upload

'''
Script to create mongodump and store into S3
'''


def mongodump_full(tdir, exp_dir):
    '''Perform Mongodump'''
    mongodump_cmd = "mongodump --oplog -o " + tdir + "/" + exp_dir
    subprocess.check_output(mongodump_cmd, shell=True)
    return


def mongodump_oplog(tdir, exp_dir):
    '''Perform Mongodump OpLog Backup'''
    mongodump_cmd = "mongodump -d local -c oplog.rs -o " + tdir + "/" + exp_dir
    subprocess.check_output(mongodump_cmd, shell=True)
    return


def create_tar(tdir, exp_dir):
    '''Perform Compressed tar of the directory'''
    tar_fname = exp_dir + ".tbz2"
    tar_cmd = "cd " + tdir + ";tar -cjf " + tar_fname + " " + exp_dir
    subprocess.check_output(tar_cmd, shell=True)
    return(tar_fname)


def store_s3(tdir, name):
    """Store in S3"""
    # Use the .boto config for credentials
    bucket_name = 'flextrip-db-dumps'

    tnow = datetime.datetime.now()
    day = int(tnow.day)
    if "full" in name:
        if day >= 1 and day <= 7:
            kname = "monthly/"
        else:
            kname = "weekly/"
    else:
        kname = "daily/"

    key_name = kname + name
    fname = tdir + "/" + name
    #print kname, fname
    s3_multipart_upload.main(fname, bucket_name, s3_key_name=key_name, use_rr=False, make_public=False)


def cleanup(tdir, name):
    """Remove the Backup dir"""
    rm_dir_cmd = "cd " + tdir + ";rm -rf " + name
    subprocess.check_output(rm_dir_cmd, shell=True)


# Main Section
if __name__ == "__main__":
    # Read the script arguments
    parser = argparse.ArgumentParser(description='Script to Mongodump or Oplog dump and Store to S3')
    parser.add_argument("-o", action='store_true', help="Perform Opslog dump")
    args = parser.parse_args()

    oplog_flag = args.o

    # Main Part of the script
    tdir = "/backup"
    cur_time = time.time()
    timestamp = datetime.datetime.fromtimestamp(cur_time).strftime('%Y-%m-%d-%u-%H-%M-%S')

    # Perform Mongodump
    if not oplog_flag:
        exp_dir = "mongodump_full-" + timestamp
        mongodump_full(tdir, exp_dir)
    else:
        exp_dir = "mongodump_oplog-" + timestamp
        mongodump_oplog(tdir, exp_dir)

    # Create a tar compressed file
    tar_fname = create_tar(tdir, exp_dir)

    # Store file into S3
    store_s3(tdir, tar_fname)

    # Remove the backup direcory
    cleanup(tdir, exp_dir)
