import sqlalchemy as sa

engine = sa.create_engine("postgresql://localhost/postgres")
conn = engine.connect()
conn.execute("commit")

db_name = 'deuces2'

conn.execute("create database %s;" % db_name)
result = conn.execute("select oid from pg_database where datname='%s';" % db_name)
oid = result.fetchone()[0]
conn.close()

import pdb; pdb.set_trace()