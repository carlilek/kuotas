# hard_quota
hard_quota.py will run a check against a Qumulo cluster running Qumulo Core 1.2.9 or later. Requires python 2.7.   
The script needs to be run as a cron job relatively frequently (at least 1x/hr) to be effective.   
The script will generate a csv file containing the quota name and the current usage and will email if the quota gets to 90%.  
At 100%, the script will remove all WRITE and ADD ACEs on the directory in question from the user and/or group(s) that have them, leaving the DELETE_CHILD ACE.  
If run again after the usage goes below 100%, the original ACEs are restored. 

Credentials are defined with environment variables, as follows:  
$QUMULO_CLUSTER  #Cluster FQDN  
$QUMULO_USER     #Username with rights to use API  
$QUMULO_PWD      #Password for that user  
  
Quotas, email address, smtp server, etc. are defined in the config.json file. A sample file is provided. Quotas should be specified in TB. 
 
