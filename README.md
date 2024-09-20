# NSW-Air-Quality

This project retrieves and processes both historical and real-time air quality data from the NSW government's API. The data is cleaned, preprocessed, and stored in a PostgreSQL database, with visualizations available through Streamlit. Additionally, a simulated real-time data stream is generated using MQTT to showcase dynamic data handling and visualization using Folium.


## Project Structure

```
nsw-air-quality/
├── Stage1/                         
│   ├── retrieve_data.ipynb       
│   └── report.pdf
│
├── Stage2/          
│   ├── requirements.txt             
│   ├── report.pdf                
│   ├── hdrtv.py                  # Historical data retriever      
│   ├── rdrtv.py                  # Real-time data retriever
│   ├── mqttmp.py                 # Mqtt message publisher
│   ├── visualization1C.py        # Historical data visualization
│   └── visualization2D.py        # Real-time data visulaization
└── README.md                     # Project overview and instructions
```