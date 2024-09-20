
import os
import json
import pandas as pd
from hdrtv import aqms_api_class


def task2AB():
    """
    Task 2A: Retrieve the latest data (i.e., for the current date) for all available Site
    Task 2B: Store the retrieved records in a CSV file
    """
    # 1. read from API
    AQMS_O = aqms_api_class(type='observ')
    rt_obs = AQMS_O.post_details()
    rt_obs_data = json.loads(rt_obs.text)
    rt_obs_df = pd.DataFrame(rt_obs_data)

    # 2. transformation 
    rt_obs_df_ = pd.concat([rt_obs_df, pd.json_normalize(rt_obs_df['Parameter'])], axis=1)
    rt_obs_df_.drop(columns=['Parameter'], inplace=True)

    # 3. store to .csv file 
    data_dir = "data"
    if not os.path.isdir(data_dir): 
        os.makedirs(os.path.join(os.getcwd(), data_dir))
    rt_obs_df_.to_csv('data/real_time_data.csv', index=False)


def main():
    # Task 2A&B
    task2AB()


if __name__ == "__main__":
    main()