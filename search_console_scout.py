import argparse
import sys
import datetime
from datetime import timedelta
from googleapiclient import sample_tools
from sqlalchemy import Table, Column, Integer, Float, String, MetaData, create_engine, insert
from sqlalchemy.schema import CreateSchema
from sqlalchemy.engine import reflection

# Declare command-line flags.
argparser = argparse.ArgumentParser(add_help=False)
argparser.add_argument('property_uri', type=str,
                       help=('Site or app URI to query data for (including '
                             'trailing slash).'))
argparser.add_argument('days', type=int,
                       help=('How many days\' worth of data to retrieve and store?'))
argparser.add_argument('client_secrets_json_location', type=str,
                       help=('The folder where the client_secrets.json credential '
                             'file is located. (Leave a trailing / at the end!)'))

def main(argv):
  client_secrets_json = argparser.parse_args(argv[1:]).client_secrets_json_location

  service, flags = sample_tools.init(
      argv, 'webmasters', 'v3', __doc__, client_secrets_json, parents=[argparser],
      scope='https://www.googleapis.com/auth/webmasters.readonly')

  search_types = ['web', 'image', 'video']

  # DB connection
  engine = create_engine('postgresql+psycopg2://gscuser:Gsc@BT2015@172.16.190.19:5439/gsc')
  # engine = create_engine('postgresql+psycopg2://lmazou@localhost:5432/lmazou')
  conn = engine.connect()

  schema_name = flags.property_uri.replace(".", "_")

  # Schema check
  insp = reflection.Inspector.from_engine(engine)
  if (schema_name not in insp.get_schema_names()):
      # The schema name is the Search Console account name
      engine.execute(CreateSchema(schema_name))

  # Table creation
  metadata = MetaData(schema=schema_name)
  queries = Table('queries', metadata,
    Column('date', String(10)),
    Column('query', String(500)),
    Column('country', String(3)),
    Column('device', String(7)),
    Column('search_type', String(5)),
    Column('clicks', Float),
    Column('impressions', Float),
    Column('ctr', Float),
    Column('position', Float)
  )
  pages = Table('pages', metadata,
    Column('date', String(10)),
    Column('page', String(500)),
    Column('country', String(3)),
    Column('device', String(7)),
    Column('search_type', String(5)),
    Column('clicks', Float),
    Column('impressions', Float),
    Column('ctr', Float),
    Column('position', Float)
  )
  metadata.create_all(engine)

  today = datetime.date.today()
  two_days = timedelta(days=2)
  two_days_ago = today - two_days

  for i in range(flags.days, 0, -1):
    x_days = timedelta(days=i)
    x_days_ago = two_days_ago - x_days
    x_days_ago_string = x_days_ago.strftime("%Y-%m-%d")

    print('Processing data dated ' + x_days_ago_string)

    for search_type in search_types:
      # Queries API call
      request = {
          'startDate': x_days_ago_string,
          'endDate': x_days_ago_string,
          'dimensions': ['date', 'query', 'country', 'device'],
          'searchType': search_type
      }
      response = execute_request(service, flags.property_uri, request)
      # print_table(response, 'Queries')

      # Queries DB storage
      for row in response['rows']:
          date = row['keys'][0]
          query = row['keys'][1]
          country = row['keys'][2]
          device = row['keys'][3]
          clicks = row['clicks']
          impressions = row['impressions']
          ctr = row['ctr']
          position = row['position']
          ins = queries.insert().values(date=date, query=query, country=country, device=device, search_type=search_type, clicks=clicks, impressions=impressions, ctr=ctr, position=position)
          conn.execute(ins)

      # Pages API call
      request = {
          'startDate': x_days_ago_string,
          'endDate': x_days_ago_string,
          'dimensions': ['date', 'page', 'country', 'device'],
          'searchType': search_type
      }
      response = execute_request(service, flags.property_uri, request)
      # print_table(response, 'Pages')

      # Pages DB storage
      for row in response['rows']:
          date = row['keys'][0]
          page = row['keys'][1]
          country = row['keys'][2]
          device = row['keys'][3]
          clicks = row['clicks']
          impressions = row['impressions']
          ctr = row['ctr']
          position = row['position']
          ins = pages.insert().values(date=date, page=page, country=country, device=device, search_type=search_type, clicks=clicks, impressions=impressions, ctr=ctr, position=position)
          conn.execute(ins)

  print('Done!')

def execute_request(service, property_uri, request):
  """Executes a searchAnalytics.query request.

  Args:
    service: The webmasters service to use when executing the query.
    property_uri: The site or app URI to request data for.
    request: The request to be executed.

  Returns:
    An array of response rows.
  """
  return service.searchanalytics().query(
      siteUrl=property_uri, body=request).execute()


def print_table(response, title):
  """Prints out a response table.

  Each row contains key(s), clicks, impressions, CTR, and average position.

  Args:
    response: The server response to be printed as a table.
    title: The title of the table.
  """
  print(title + ':')

  if 'rows' not in response:
    print('Empty response')
    return

  rows = response['rows']
  row_format = '{:<20}' + '{:>20}' * 4
  print(row_format.format('Keys', 'Clicks', 'Impressions', 'CTR', 'Position'))
  for row in rows:
    keys = ''
    # Keys are returned only if one or more dimensions are requested.
    if 'keys' in row:
      keys = u','.join(row['keys']).encode('utf-8')
    print(row_format.format(
        keys, row['clicks'], row['impressions'], row['ctr'], row['position']))

if __name__ == '__main__':
  main(sys.argv)
