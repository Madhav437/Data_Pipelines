import snowflake.connector
import configparser

parser = configparser.ConfigParser()
parser.read("pipeline.conf")
username = parser.get("snowflake_creds",
                      "username")
password = parser.get("snowflake_creds",
                      "password")
account_name = parser.get("snowflake_creds",
                          "account_name")

snow_conn = snowflake.connector.connect(
    user=username,
    password=password,
    account=account_name
)

# truncate the destination table
sql = "TRUNCATE madhav_test.public.export;"
cur = snow_conn.cursor()
cur.execute(sql)

cur.close()

sql = "use database madhav_test"
cur = snow_conn.cursor()
cur.execute(sql)

sql = """COPY INTO export
  FROM @my_s3_stage
  pattern='.*export_file.*.csv';"""

cur = snow_conn.cursor()
cur.execute(sql)
cur.close()
