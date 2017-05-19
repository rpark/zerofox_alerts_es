# zerofox_alerts_es
## Synopsis

Uses the ZeroFOX API to copy alerts into ElasticSearch.

## Code Usage

Usage for the code is:

python alerts_es.py -h <ES host> -r <ES port> -u <user> -p <password>

If you leave out the host then it defaults to localhost. If you leave out the port then it defaults to 9200. If you leave out the username or password then you are prompted to type them in.

For example:

python alerts_es.py -h elk.acme.com -p 9201 -u jsmith -p 'mypassword'

You could either put the username/password on the command line or hard code them directly in the code around lines 25-26.

The code authenticates to our API, downloads the alerts and does some simple transformation, then uses the ElasticSearch bulk API to send the alerts to ES.

The ES index name is "alerts". If you want to personalize this (such as alerts_zerofox) then you can use the -c parameter:

python alerts_es.py -h elk.acme.com -p 9201 -c zerofox -u jsmith -p 'mypassword'

Every time the code runs, it deletes the existing alerts index, recreates it, then sends the alerts. This helps ensure that the alert status is up to date.

We had previously looked at only updating alert status for alerts modified in the last time period but this was unnecessarily complicated and still required us to iterate through every alert to look for status changes.

The code also does some simple logging. It mainly logs how many ZeroFOX alerts are retrieved and what ES host they are sent to.
