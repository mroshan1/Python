import boto
from datetime import *
import re

# TODO: Cleanup old snapshots


def get_all_instances(conn):
    """Get the List of all instance id for our account"""

    reservations = conn.get_all_instances()
    return reservations


def get_instance_vols(conn, instance_id):
    """Get list of all the volns for the instance"""

    vols = conn.get_all_volumes(filters={'attachment.instance-id': instance_id})
    return vols


def create_snap(conn, instance_name, vol_id, vol_dev, ft):
    """Create snap for given vol"""

    # Create the tag based on the day the week
    if (int(ft[3]) == 7):
        if int(ft[2]) <= 7 and int(ft[2]) >= 1:
            snap_day_tag = "monthly"
        else:
            snap_day_tag = "weekly"
    else:
        snap_day_tag = "daily"

    snap_tag_name = instance_name + "_" + vol_dev + "_" + snap_day_tag + "_" + '-'.join(ft)
    snap_description = "Created by Snapshot Script : " + snap_day_tag
    snap_id = conn.create_snapshot(vol_id, snap_description)
    snap_id.add_tags({'Name': snap_tag_name})
    return snap_id


def cleanup_snap(conn, snapshot):
    """Remove old snap"""

    # Get Information on the snapshot
    snap_id, snap_desc = snapshot.id, snapshot.description
    snap_start_date = datetime.strptime(snapshot.start_time[:-5], '%Y-%m-%dT%H:%M:%S')
    current_date = datetime.now()
    current_date_fmt = datetime.strptime(str(current_date)[:-7], '%Y-%m-%d %H:%M:%S')

    if re.search('Created by Snapshot Script : daily', snap_desc):
        days_ret = 14
    elif re.search('Created by Snapshot Script : weekly', snap_desc):
        days_ret = 60
    else:
	days_ret = 99999999999

    num_days_snapshot = current_date_fmt - snap_start_date
    if num_days_snapshot.days > days_ret:
        #print 'Purge - %s %s %s' % (snap_id, snap_start_date,snap_desc)
        snapshot.delete()


if __name__ == "__main__":

    # Get AWS credentials
    # Uncomment if you not are using .boto config
    #AWS_ACCESS_KEY_ID = 'XXXXXXXXXXXXXXXX'
    #AWS_SECRET_ACCESS_KEY = 'XXXXXXXXXXXXXXXXXXXXXXXX'
    #conn = boto.connect_ec2(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    conn = boto.connect_ec2()

    reservations = get_all_instances(conn)
    date_now = datetime.now().strftime("%Y-%m-%d-%u-%H-%M")
    ft = date_now.split('-')

    for r in reservations:

        for i in r.instances:
            instance_id, instance_name = i.id, i.__dict__['tags']['Name']
            #print instance_id, instance_name
            instance_backup_tag = i.__dict__['tags']['Backup']
            backup_flag = False

            # Check if Backup needs to performed daily or weekly
            if instance_backup_tag == "daily":
                backup_flag = True
            elif instance_backup_tag == "weekly" and int(ft[3]) == 7:
                backup_flag = True

            # If backup_flag set to true, then create snapshot
            if backup_flag:
                # Get Instance volumes
                instance_vols = get_instance_vols(conn, instance_id)

                for v in instance_vols:
                    vol_id, vol_dev, vol_snapshots = v.id, v.attach_data.device.lstrip('/dev/'), v.snapshots()
                    #print vol_id, vol_dev

                    # Create Snapshot
                    snap_id = create_snap(conn, instance_name, vol_id, vol_dev, ft)
                    #print instance_id, instance_name, vol_id, vol_dev, snap_id

                    # Check if old snapshots need to be deleted
                    for s in vol_snapshots:
                        if s.id != snap_id.id:
                    	    #print instance_id, instance_name, vol_id, s
                            cleanup_snap(conn, s)
