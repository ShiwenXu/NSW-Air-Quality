import os
import ast
import json
import streamlit as st
from streamlit_folium import st_folium
import folium
import paho.mqtt.client as mqtt
from sqlalchemy import create_engine
import pandas as pd
from hdrtv import get_sites_df


broker = "172.17.34.107"


def draw_map(CENTER_START, joined_df):
    """
    The folloing code is modified from https://folium.streamlit.app/dynamic_updates
    """

    new_lat, new_lon = joined_df.at[0, 'Latitude'], joined_df.at[0, 'Longitude']
    site_id, site_name, region = joined_df.at[0, 'Site_Id'], joined_df.at[0, 'SiteName'], joined_df.at[0, 'Region']
    qua_cat = joined_df.at[0, 'AirQualityCategory']
    pm_val = joined_df.at[0, 'Value']
    timestamp = joined_df.at[0, 'Date']
    html=f"""
        {site_id} - {site_name} - {region} 
        AirQualityCategory : {qua_cat} 
        PM2.5 Value: {pm_val} 
        Timestamp: {timestamp} 
        """
    new_marker = folium.Marker(
        location=[new_lat, new_lon],
        popup=html,
        parse_html=True, max_width="100%"
    )
    st.session_state["markers"].append(new_marker)
            
    m = folium.Map(location=CENTER_START, zoom_start=7.5)
    fg = folium.FeatureGroup(name="Markers")
    for marker in st.session_state["markers"]:
        fg.add_child(marker)
        
    try:
        st_folium(
            m,
            center=st.session_state["center"],
            zoom=st.session_state["zoom"],
            feature_group_to_add=fg,
            height=600,
            width=1000,
        )
    except:
        pass


def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe("shxu4542/comp5339-A2")


# Define what happens upon recieving a message from the server
def on_message(client, userdata, msg):
    print(f"Received message on topic {msg.topic}: {msg.payload}")
    # 1.1 Preprocess site_df 
    data_dir = "data"
    if not os.path.isdir(data_dir): 
        os.makedirs(os.path.join(os.getcwd(), data_dir))
    else:
        if not os.path.exists('data/site.csv'):
            site_df = get_sites_df()
        site_df = pd.read_csv('data/site.csv')
    site_df = site_df.loc[(site_df['Longitude'] != -999) & (site_df['Latitude'] != -999)]

    # 1.2 preprocess the received message into a dataframe
    message = msg.payload.decode('utf-8')
    message = message.replace('nan', 'None')
    record = ast.literal_eval(message)
    obs_df = pd.DataFrame.from_dict([record])

    # # 1.3 Join two dataframe on site_id 
    joined_df = pd.merge(obs_df, site_df, on='Site_Id', how='left')

    # 1.4 create session state
    mid_lon, mid_lat = site_df['Longitude'].mean(), site_df['Latitude'].mean()

    CENTER_START = [mid_lat, mid_lon]
    ZOOM_START = 8

    if "center" not in st.session_state:
        st.session_state["center"] = [mid_lat, mid_lon]
    if "zoom" not in st.session_state:
        st.session_state["zoom"] = ZOOM_START
    if "markers" not in st.session_state:
        st.session_state["markers"] = []
   
    draw_map(CENTER_START, joined_df)


def subscriber():
    client = mqtt.Client() 
    client.clean_session = True
    client.on_connect = on_connect
    client.on_message = on_message
    # Connect to the server using the IP address and connection port
    client.connect(broker, 1883, 60)
    client.loop_forever()


def task2D():
    """ 
    Task 2D: Subscribe to the messages that you published to MQTT, and for each message,
    dynamically add a marker on a map in a way similar to the left one of https://
    folium.streamlit.app/dynamic_map_vs_rerender. When a user clicks a marker, it
    should display the AirQualityCategory, the PM2.5 value, and the timestamp of the
    observation being made.
    """
    subscriber()


def main():
    st.title('Visualization Dashboard - PM2.5 in NSW')
    task2D()


if __name__ == "__main__":
    main()