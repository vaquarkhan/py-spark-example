import sqlalchemy

# Set the following variables depending on your specific
# connection name and root password from the earlier steps:
connection_name = "test"
db_password = "1234"
db_name = "testv"

db_user = "root"
driver_name = 'mysql+pymysql'
query_string = dict({"unix_socket": "/cloudsql/{}".format(connection_name)})

def hello_gcs(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file = event
    # print('Event ID: {}'.format(context.event_id))
    # print('Event type: {}'.format(context.event_type))
    # print('Bucket: {}'.format(event['bucket']))
    # print('File: {}'.format(event['name']))
    # print('Metageneration: {}'.format(event['metageneration']))
    # print('Created: {}'.format(event['timeCreated']))
    # print('Updated: {}'.format(event['updated']))

def insert(request):
    request_json = request.get_json()
    stmt = sqlalchemy.text('INSERT INTO entries (guestName, content) values ("third guest", "Also this one");')
    db = sqlalchemy.create_engine(
      sqlalchemy.engine.url.URL(
          drivername=driver_name,
          username=db_user,
          password=db_password,
          database=db_name,
          query=query_string,
      ),
      pool_size=5,
      max_overflow=2,
      pool_timeout=30,
      pool_recycle=1800
    )
    try:
        with db.connect() as conn:
            conn.execute(stmt)
    except Exception as e:
        return 'Error: {}'.format(str(e))
    return 'ok'