import psycopg2
import numpy as np
import pandas
import sys
import pandas as pd
import sqlalchemy


#### Generate test_1 and test_2 tables
## For test_1, make 4 columns:
def gen_test_table(engine, num_rows, table_name):
    col1 = pd.array(np.arange(num_rows))
    col2 = pd.array(np.random.randint(0, 10, num_rows), dtype="Int64")
    col3 = pd.array(np.random.randint(0, 100, num_rows), dtype="Int64")
    col4 = pd.array(np.random.randint(0, 1000, num_rows), dtype="Int64")
    ## Set random entries in col3 and col4 to NULL
    col3_nulls = np.random.choice(a=[True, False], size=num_rows, replace=True, p=[0.1, 0.9])
    col4_nulls = np.random.choice(a=[True, False], size=num_rows, replace=True, p=[0.3, 0.7])
    col3[tuple([col4_nulls])] = np.nan
    col4[tuple([col4_nulls])] = np.nan
    df = pd.DataFrame({
        "col1" : col1,
        "col2" : col2,
        "col3" : col3,
        "col4" : col4
    })
    df.to_sql(table_name, engine, index=False, if_exists='append')



#### Generate Postgres tables ####
def setup_postgres(conn, table_name):
    cur = conn.cursor()
    print("Setting up Postgres {}".format(table_name))
    # Delete previous entries
    print("Deleting previous entries")
    cur.execute("DELETE FROM {};".format(table_name))
    print("Deleted previous entries")

    # Insert the whole table
    print("Writing data from {}.tbl".format(table_name))
    with open("tpch-dbgen/{}.tbl".format(table_name), 'r') as f:
        cur.copy_from(f, table_name, sep='|')
    print("Wrote data")
    #print("Done Setting up Postgres Database")
    # Commit
    conn.commit()


def run(argv):
    if argv[1] == "tpch":
        conn = psycopg2.connect("host=localhost port=5432 dbname=tpl_test user=amlatyrpsql password=TRIvial$$159")
        setup_postgres(conn, "part")
        setup_postgres(conn, "supplier")
        setup_postgres(conn, "partsupp")
        setup_postgres(conn, "customer")
        setup_postgres(conn, "orders")
        setup_postgres(conn, "lineitem")
        setup_postgres(conn, "nation")
        setup_postgres(conn, "region")
    if argv[1] == "test_tables":
        engine = sqlalchemy.create_engine("postgresql://amlatyrpsql:TRIvial$$159@localhost:5432/tpl_test")
        print("AAAAA")
        gen_test_table(engine, 10000, "test_1")
        gen_test_table(engine, 100, "test_2")
        print("AAAAA")

run(sys.argv)
