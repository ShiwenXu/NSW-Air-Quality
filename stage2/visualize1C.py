import os
import json
import streamlit as st
import pandas as pd
from hdrtv import pgconnect, get_sites_df


# @st.cache_data(ttl=600)
def run_query(query):
    # 1. connect to PostgreSQL database 
    conn = pgconnect()
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()


def task1C():
    """
    Task 1C: Use dashboard to visualise the daily average PM2.5 data for a chosen Site from a
    drop-down list, by using a line chart. Users should be able to choose any Sydney
    site, e.g., via a streamlit selectbox.
    """
    # 1. get conditioned data 
    try: 
        stmt_1 = """ SELECT * FROM Observation 
                WHERE parameter_code = 'PM2.5'
                and category = 'Averages' 
                and subcategory = 'Daily' """
        rows = run_query(stmt_1)
        column_names = ['Obs_Id', 'Site_Id', 'Date', 'Hour', 'HourDescription', 'Value', 
                        'AirQualityCategory', 'DeterminingPollutant', 'ParameterCode', 
                        'ParameterDescription', 'Units', 'UnitsDescription' , 'Category',
                        'SubCategory', 'Frequency']
        # check site.csv file for future use 
        data_dir = "data"
        if not os.path.isdir(data_dir): 
            os.makedirs(os.path.join(os.getcwd(), data_dir))
        else:
            if not os.path.exists('data/site.csv'):
                site_df = get_sites_df()
            site_df = pd.read_csv('data/site.csv')
        
        # merge two dataframes
        obs_data = pd.DataFrame(rows, columns=column_names)
        obs_data['Date'] = pd.to_datetime(obs_data['Date'])
        obs_data = pd.merge(obs_data, site_df, on='Site_Id', how='left')
        obs_data['SiteId-SiteName-Region'] = obs_data.apply(lambda row: f"{row['Site_Id']} - {row['SiteName']} - {row['Region']}", axis=1)
        obs_data.drop(['Site_Id', 'SiteName', 'Region'], axis=1, inplace=True)
        obs_data.drop(['HourDescription', 'ParameterDescription', 'UnitsDescription'], axis=1, inplace=True)

        # 2. plot dropdown list with the line-chart for selected site 
        sites = st.selectbox(
            "Choose a Site", list(obs_data['SiteId-SiteName-Region'])
        )
        if not sites:
            st.error("Please select one Site.")
        else:
            rtv_data = obs_data.loc[obs_data['SiteId-SiteName-Region'] == sites]
            st.write("### Daily Average PM2.5 Data ", rtv_data.sort_index())
            st.line_chart(rtv_data, x="Date", y="Value")

    except Exception as e:
        st.error(
            """
            **Something went wrong...**
            """
        )


def main():
    # task1AB() # get most up-to-date data
    st.set_page_config(layout="wide")
    st.title('Visualization Dashboard')
    task1C()


if __name__ == "__main__":
    main()