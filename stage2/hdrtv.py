import os
import psycopg2
import logging 
import urllib
import requests, json
from datetime import datetime as dt, timedelta, date
import pandas as pd



class aqms_api_class(object):
    """
    This code is modified from https://www.environment.nsw.gov.au/-/media/OEH/Corporate-Site/Documents/Air/air-quality-application-programming-interface-user-guide-210346.pdf
    This class defines and configures the API to query the Azure DataWarehouse 
    """
    def __init__(self, type):
        self.logger = logging.getLogger(type)
        self.url_api = "https://data.airquality.nsw.gov.au"
        self.headers = {'content-type': 'application/json', 'accept':'application/json'}
        self.type = type
        if self.type == 'site':
            self.type_url = '/api/Data/get_SiteDetails'
        elif self.type == 'param':
            self.type_url = 'api/Data/get_ParameterDetails'
        elif self.type == 'observ':
            self.type_url = 'api/Data/get_Observations'
        else:
            raise ValueError("Incorrect Data Type")
    
    def get_details(self, ):
        """
        Build a query to return all the details
        """
        query = urllib.parse.urljoin(self.url_api, self.type_url)
        response = requests.get(url=query, data='')
        return response
    
    def post_details(self, ObsRequest=None):
        """ 
        Build a query to extract current hourly observed 
        """
        assert self.type == 'observ'

        # retrieve current hourly observed air quality data by default
        query = urllib.parse.urljoin(self.url_api, self.type_url)
        if ObsRequest:
            response = requests.post(url=query, data=json.dumps(ObsRequest), headers=self.headers)
        else:
            response = requests.post(url=query, data='', headers=self.headers)
        return response
    
    def ObsRequest_init(self, 
                        parameters:list, 
                        sites:list, 
                        startdate:dt, 
                        enddate:dt, 
                        categories:list, 
                        subcategories:list):
        """ 
        Build a query to extract historical air quality data by pre-defined criteria
        :param parameters: e.g ['OZONE']
        :param sites: e.g [190, 221]
        :param startdate: e.g dt.Date(2018, 12, 1)
        :param enddate: e.g dt.Date(2019, 12, 1)
        :param categories: e.g ['Maximums']
        :param categories: e.g ['Daily']
        :param categories: e.g ['4h rolling average derived from 1h average']
        """
        ObsRequest = {}
        ObsRequest['Parameters'] = parameters
        ObsRequest['Sites'] = sites
        ObsRequest['StartDate'] = startdate.strftime('%Y-%m-%d')
        ObsRequest['EndDate'] = enddate.strftime('%Y-%m-%d')
        ObsRequest['Categories'] = categories
        ObsRequest['SubCategories'] = subcategories

        return ObsRequest


def get_sites_df():
    AQMS_S = aqms_api_class(type='site')
    AllSites = AQMS_S.get_details()
    site_data = json.loads(AllSites.text)
    site_df = pd.DataFrame(site_data)
    data_dir = "data"
    if not os.path.isdir(data_dir): 
        os.makedirs(os.path.join(os.getcwd(), data_dir))
    site_df.to_csv("data/site.csv")
    return site_df


def pgconnect(dbname='air_quality'):
    """ 
    Modified from COMP5310 week 4 tutorial slides
    utility function to connect to Postgresql database
    """
    YOUR_DBNAME = f'{dbname}'
    YOUR_USERNAME = 'postgres'
    YOUR_PW     = '2020352!'
    PORT = "2024"
    try: 
        conn = psycopg2.connect(host='localhost',
                                port = PORT,
                                database=YOUR_DBNAME,
                                user=YOUR_USERNAME, 
                                password=YOUR_PW)
        print('connected')
    except Exception as e:
        print("unable to connect to the database")
        print(e)
    return conn


def pgexec( conn, sqlcmd, args, msg, silent=False ):
   """ 
   utility function to execute some SQL statement can take optional arguments to fill in (dictionary) error and transaction handling built-in 
   """
   retval = False
   with conn:
      with conn.cursor() as cur:
         try:
            if args is None:
               cur.execute(sqlcmd)
            else:
               cur.execute(sqlcmd, args)
            if silent == False: 
                print("success: " + msg)
            retval = True
         except Exception as e:
            if silent == False: 
                print("db error: ")
                print(e)
   conn.commit()
   return retval


def pgquery( conn, sqlcmd, args, silent=False ):
   """ utility function to execute some SQL query statement
       can take optional arguments to fill in (dictionary)
       will print out on screen the result set of the query
       error and transaction handling built-in """
   retval = False
   res = []
   with conn:
      with conn.cursor() as cur:
         try:
            if args is None:
                print("sqlcmd: ", sqlcmd)
                cur.execute(sqlcmd)
            else:
                cur.execute(sqlcmd, args)
            if silent == False:
                for record in cur:
                    print(record)
                    res.append(record)
            retval = True
         except Exception as e:
            if silent == False:
                print("db read error: ")
                print(e)
   return retval, res


def retrive_histobs():
    """ 
    Task 1A: Retrieve all daily average PM2.5 data for all Sydney sites (i.e., in either Sydney
    East, Sydney South-west, or Sydney North-west) from 2023-01-01 to the day
    before the current date (inclusive).
    """
    # 1. define conditions
    parameters = ['PM2.5']
    regions = ['Sydney East', 'Sydney South-west', 'Sydney North-west'] 
    site_df = get_sites_df()
    sites = site_df.loc[site_df['Region'].isin(regions), 'Site_Id'].tolist() # Find out all regions_id
    startDate = date(2023, 1, 1) # start with 2023-01-01
    endDate = dt.now().date() - timedelta(1)  # end with the day before current day
    categories = ['Averages']
    SubCategories = ['Daily']

    # 2. retrieve historical observation data
    AQMS_O = aqms_api_class(type='observ')
    ObsRequest = AQMS_O.ObsRequest_init(parameters, sites, startDate, endDate, categories, SubCategories)
    Ozone_Obs = AQMS_O.post_details(ObsRequest)
    ozone_obs_data = json.loads(Ozone_Obs.text)
    obs_df = pd.DataFrame(ozone_obs_data)
    
    # 3. data preprocessing & transformation
    obs_df_ = pd.concat([obs_df, pd.json_normalize(obs_df['Parameter'])], axis=1)
    obs_df_.drop(columns=['Parameter'], inplace=True)

    # 4. delete record if missing PM2.5
    obs_df_.dropna(subset=['Value'], inplace=True)

    # 5. replace missing value with NA 
    obs_df_['DeterminingPollutant'] = obs_df_['DeterminingPollutant'].fillna('Unknown')
    obs_df_['AirQualityCategory'] = obs_df_['AirQualityCategory'].fillna('Unknown')
    
    return obs_df_


def write_db(obs_df):
    """ 
    Task 1B: Load the retrieved records into a Database (e.g., PostgreSQL). (Records without
    PM2.5 values can be ignored.)
    """
    data_obs = obs_df.to_dict(orient='records')
    conn = pgconnect(dbname='air_quality')
    pgexec (conn, "DROP TABLE IF EXISTS Observation CASCADE", None, "Reset Table Observation")
    obs_schema = """CREATE TABLE IF NOT EXISTS Observation (
                            observation_id SERIAL PRIMARY KEY,
                            site_id INTEGER NOT NULL,
                            date DATE NOT NULL, 
                            hour INTEGER,
                            hour_description VARCHAR(20),
                            value FLOAT,
                            air_quality_category VARCHAR(30), 
                            determining_pollutant VARCHAR(30),
                            parameter_code VARCHAR(30) NOT NULL, 
                            parameter_description VARCHAR(50), 
                            unit VARCHAR(20) NOT NULL, 
                            unit_description VARCHAR(30),
                            category VARCHAR(20) NOT NULL,
                            subcategory VARCHAR(10) NOT NULL,
                            frequency VARCHAR(50) NOT NULL,
                            CONSTRAINT site_idFK FOREIGN KEY (site_id)   REFERENCES Site (site_id)
                    )"""
    pgexec (conn, obs_schema, None, "Create Table Observation")
    insert_stmt = """INSERT INTO Observation(site_id, date, hour, hour_description, 
                    value, air_quality_category, determining_pollutant, parameter_code, 
                    parameter_description, unit, unit_description, category, subcategory, frequency) 
                    VALUES (%(Site_Id)s, %(Date)s, %(Hour)s, %(HourDescription)s, %(Value)s, 
                    %(AirQualityCategory)s, %(DeterminingPollutant)s, %(ParameterCode)s, %(ParameterDescription)s, %(Units)s, 
                    %(UnitsDescription)s, %(Category)s, %(SubCategory)s, %(Frequency)s)"""
    for i, row in enumerate(data_obs):
        pgexec (conn, insert_stmt, row, f"Row {i+1} inserted")


def task1AB():
    obs_df = retrive_histobs()
    write_db(obs_df)


def main():
    # Task 1A & Task 1B
    task1AB()


if __name__ == "__main__":
    main()