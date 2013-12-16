import sqlalchemy as sa
import sys, os

base_postgres_url = os.environ.get('BASE_PG_URL')
engine = sa.create_engine("%s/postgres" % base_postgres_url, echo=True)
conn = engine.connect()
conn.execute("commit")

db_name = sys.argv[1]

conn.execute("create database %s;" % db_name)
result = conn.execute("select oid from pg_database where datname='%s';" % db_name)
oid = result.fetchone()[0]
conn.close()

print oid
