#!/usr/bin/env python
# Copyright (c) 2013 Qumulo, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.


# Import python libraries
import os
import sys
import smtplib
from email.mime.text import MIMEText
import tempfile
import time
import json

# Import Qumulo REST libraries
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
import qumulo.lib.auth
import qumulo.lib.opts
import qumulo.lib.request
import qumulo.rest.fs as fs

# Size Definitions
KILOBYTE = 1024
MEGABYTE = 1024 * KILOBYTE
GIGABYTE = 1024 * MEGABYTE
TERABYTE = 1024 * GIGABYTE

# Import credentials
host = os.environ.get('QUMULO_CLUSTER')
port = 8000
user = os.environ.get('QUMULO_USER')
password = os.environ.get('QUMULO_PWD') or 'admin'

# Import config.json for environment specific settings
try:
    configpath = "./config.json"
    with open (configpath, 'r') as j:
        config = json.load(j)
    
    sender = str(config['email settings']['sender_address'])
    smtp_server = str(config['email settings']['server'])
    host = str(config['qcluster']['url'])
    storagename = str(config['qcluster']['name'])
    header = 'Group,SpaceUsed,QuotaSize,FileCount'
    
    quota_dict = {}
    for quota in config['quotas']:
        quotaname = str(quota)
        storage_path = str(config['quotas'][quota]['qumulo_path'])
        nfs_path = str(config['quotas'][quota]['nfs_path'])
        size = config['quotas'][quota]['quota_size']
        recipients = config['quotas'][quota]['mail_to']
        quota_dict[quotaname] = (storage_path, nfs_path, size, recipients)
    
    logfile = str(config['output_log']['logfile'])
except Exception, excpt:
    print "Improperly formatted {} or missing file:".format(configpath, excpt)
    sys.exit(1)

def login(host, user, passwd, port):
    '''Obtain credentials from the REST server'''
    conninfo = None
    creds = None

    try:
        # Create a connection to the REST server
        conninfo = qumulo.lib.request.Connection(host, int(port))

        # Provide username and password to retreive authentication tokens
        # used by the credentials object
        login_results, _ = qumulo.rest.auth.login(
                conninfo, None, user, passwd)

        # Create the credentials object which will be used for
        # authenticating rest calls
        creds = qumulo.lib.auth.Credentials.from_login_response(login_results)
    except Exception, excpt:
        print "Error connecting to the REST server: %s" % excpt
        print __doc__
        sys.exit(1)

    return (conninfo, creds)

def send_mail(smtp_server, sender, recipients, subject, body):
    mmsg = MIMEText(body, 'html')
    mmsg['Subject'] = subject
    mmsg['From'] = sender
    mmsg['To'] = ", ".join(recipients)

    session = smtplib.SMTP(smtp_server)
    session.sendmail(sender, recipients, mmsg.as_string())
    session.quit()

def build_mail(nfspath, quota, current_usage, smtp_server, sender, recipients, bodytype):
    if bodytype is 'warn':
        bodytext = "The usage on {} is greater than 90% of the quota. At 100%, writes will be disabled until some items are deleted.<br>".format(nfspath)
    elif bodytype is 'lock':
        bodytext = "The usage on {} has exceeded the quota. Writes have been disabled until some items are deleted.<br>".format(nfspath)
    elif bodytype is 'unlock':
        bodytext = "The usage on {} is now below the quota. Writes have been re-enabled.<br>".format(nfspath)
    sane_current_usage = float(current_usage) / float(TERABYTE)
    subject = storagename + " Quota Notification"
    body = ""
    body += bodytext
    body += "Current usage: %0.2f TB<br>" %sane_current_usage 
    body += "Quota: %0.2f TB<br>" %quota
    body += "<br>"
    send_mail(smtp_server, sender, recipients, subject, body)
        
# Build email and check against quota for notification, return current usage for tracking
def monitor_path(path, conninfo, creds):
    try:
        node = fs.read_dir_aggregates(conninfo, creds, path)
    except Exception, excpt:
        print 'Error retrieving path: %s' % excpt
    else:
        current_usage = int(node[0]['total_capacity'])
        total_files = int(node[0]['total_files'])
        return current_usage, total_files

def build_csv(quotaname, current_usage, quotaraw, total_files, tempfile):
    with open(tempfile, "a") as file:
        file.write("{},{},{},{}\n".format(quotaname, str(current_usage), str(quotaraw), str(total_files)))

def write_kwotafile(conninfo, creds, sourcepath, orig_acls):
    kwotafile = os.path.join(sourcepath,'.kwota')
    temp_file = tempfile.TemporaryFile(mode='a+b')
    orig_acls_json = json.dumps(orig_acls)
    temp_file.write(orig_acls_json)
    try:
        fs.create_file(conninfo, creds, '.kwota', dir_path=sourcepath)
    except:
        pass
    fs.write_file(conninfo, creds, temp_file, kwotafile)
    temp_file.close()
    response = fs.get_attr(conninfo, creds, path=kwotafile)
    attrs = json.loads(str(response))
    fsize = attrs['size']
    fowner = attrs['owner']
    fgroup = attrs['group']
    currtime = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + 'Z'
    fs.set_attr(conninfo, creds, mode='0600', owner=fowner, group=fgroup, modification_time=currtime, change_time=currtime, size=fsize, path=kwotafile)
    
def lock_share(conninfo, creds, sourcepath, locked_acls_list):
    for ace in locked_acls_list[0]['acl']['aces']:
        if ace[u'trustee'] in '500':
            continue
        try:
            ace[u'rights'].remove(u'WRITE_EA')
        except:
            pass
        try:
            ace[u'rights'].remove(u'WRITE_ATTR')
        except:
            pass
        try:
            ace[u'rights'].remove(u'WRITE_ACL')
        except:
            pass
        try:
            ace[u'rights'].remove(u'ADD_FILE')
        except:
            pass
        try:
            ace[u'rights'].remove(u'ADD_SUBDIR')
        except:
            pass

    locked_control=locked_acls_list[0]['acl']['control']
    locked_aces=locked_acls_list[0]['acl']['aces']       
    fs.set_acl(conninfo, creds, path=sourcepath, control=locked_control, aces=locked_aces) 

def unlock_share(conninfo, creds, sourcepath, kwotafile):
    temp_file2 = tempfile.TemporaryFile(mode='a+b')
    fs.read_file(conninfo, creds, temp_file2, path=kwotafile)
    temp_file2.seek(0)
    restore_acls = json.loads(str(temp_file2.read()))
    restore_control = restore_acls[0]['acl']['control']
    restore_aces = restore_acls[0]['acl']['aces']
    fs.set_acl(conninfo, creds, path=sourcepath, control=restore_control, aces=restore_aces)
    temp_file2.close()
    fs.delete(conninfo, creds, kwotafile)

### Main subroutine
def main(argv):
    # Get credentials
    (conninfo, creds) = login(host, user, password, port)
    
    # Overwrite log file
    with open(logfile, "w") as file:
        file.write(header + '\n')

    # Get quotas and generate CSV
    for quotaname in quota_dict.keys():
        path, nfspath, quota, recipients = quota_dict[quotaname]
        current_usage, total_files = monitor_path(path, conninfo, creds)
        quotaraw = int(quota) * TERABYTE
        build_csv(quotaname, current_usage, quotaraw, total_files, logfile)    
        kwarn_file = os.path.join(path, '.kwarn')
        kwotafile = os.path.join(path, '.kwota')
        try:
            fs.get_file_attr(conninfo, creds, kwotafile)
            if current_usage < quotaraw:
                unlock_share(conninfo, creds, path, kwotafile)
                bodytype = 'unlock'
                build_mail(nfspath, quota, current_usage, smtp_server, sender, recipients, bodytype)
            continue
        except:
            pass
        if current_usage is not None:
            soft_threshold = int(quotaraw * 0.90)
            if current_usage > soft_threshold and current_usage < quotaraw:
                try:
                    fs.create_file(conninfo, creds, '.kwarn', dir_path=path)
                    bodytype = 'warn'
                    build_mail(nfspath, quota, current_usage, smtp_server, sender, recipients, bodytype)
                except:
                    pass
            elif current_usage > quotaraw:
                orig_acls = fs.get_acl(conninfo, creds, path)
                write_kwotafile(conninfo, creds, path, orig_acls)
                lock_share(conninfo, creds, path, orig_acls)
                bodytype = 'lock'
                build_mail(nfspath, quota, current_usage, smtp_server, sender, recipients, bodytype)
            elif current_usage < soft_threshold:
                try:
                    fs.delete(conninfo, creds, kwarn_file)
                except:
                    pass

# Main
if __name__ == '__main__':
    main(sys.argv[1:])
