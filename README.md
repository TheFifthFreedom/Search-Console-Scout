# Search-Console-Scout
Little Python 3 script that leverages the [client library for Google's discovery based APIs](https://github.com/google/google-api-python-client) as well as [SQLAlchemy](http://www.sqlalchemy.org/) to retrieve and store Search Console data for archiving in a SQL DB, thus circumventing Google's 90-days history limit. This is meant to be run as a cron job.
Originally based on [ipullrank](https://github.com/ipullrank)'s [tool](https://github.com/ipullrank/gwt-scout).
