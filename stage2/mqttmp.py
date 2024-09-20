import time 
import pandas as pd
import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish


# use instructor's server 
broker = "172.17.34.107"


def task2C():

    """
    Task 2C: For each retrieved hourly average PM2.5 record (read from the CSV file), publish
    the full record to a MQTT server, with a delay of 1 second between two publishing
    messages.
    """
    # 1. get conditioned record 
    rt_obs_df = pd.read_csv('data/real_time_data.csv')
    cond_df =  rt_obs_df.loc[(rt_obs_df['ParameterCode'] == 'PM2.5') & 
                         (rt_obs_df['Frequency'] == 'Hourly average')]
    
    # 2. publish to a MQTT server 
    for idx, row in cond_df.iterrows():
        msg = row.to_dict()
        publish.single(topic='shxu4542/comp5339-A2', payload=str(msg), hostname=broker, qos=0) 
        time.sleep(1)

def main():
    # Task 2C
    task2C()

if __name__ == "__main__":
    main()