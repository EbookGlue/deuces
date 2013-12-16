import sqlalchemy as sa
import subprocess
import sys, os

base_postgres_url = os.environ.get('BASE_PG_URL')
engine = sa.create_engine("%s/postgres" % base_postgres_url)
conn = engine.connect()
conn.execute("commit")

db_name = sys.argv[1]

#conn.execute("create database %s;" % db_name)
result = conn.execute("select oid from pg_database where datname='%s';" % db_name)
oid = result.fetchone()[0]
conn.close()

cluster_base = '/var/lib/postgresql/9.1/main'
data_location = '%s/base/%s' % (cluster_base, oid)

DEVNULL = open(os.devnull, 'w')
pg_ctl = '/usr/lib/postgresql/9.1/bin/pg_ctl'

# Stop postgres server
print "Shutting down postgres server..."
stop_p = subprocess.Popen([pg_ctl, 'stop', '-D', cluster_base, '-m', 'fast'], stdout=DEVNULL, stderr=subprocess.STDOUT)
stop_p.wait()

# Clone backup into new database
print "Cloning backup from cloud..."
clone_p = subprocess.Popen(['envdir', '/etc/wal-e.d/env', 'wal-e', 'backup-fetch', data_location, 'LATEST'], stdout=DEVNULL, stderr=subprocess.STDOUT)
clone_p.wait()

# Start postgres server
print "Starting postgres server..."
start_p = subprocess.Popen([pg_ctl, 'start', '-D', cluster_base], stdout=DEVNULL, stderr=subprocess.STDOUT)
start_p.wait()

print "---"
print "Lastest backup has successfully been cloned into new database: %s" % db_name
print "---"
