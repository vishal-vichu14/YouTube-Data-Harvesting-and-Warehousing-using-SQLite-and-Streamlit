# YouTube-Data-Harvesting-and-Warehousing-using-SQLite-and-Streamlit

This project designed to provide users with seamless access and analysis of data from multiple YouTube channels. This intuitive tool leverages the Google API to retrieve a comprehensive range of information, including channel details, video statistics, and viewer engagement metrics.

## Technology Stack Used
1. Python
2. SQLite
3. Streamlit
4. Google Client Library

## Packages to install 
1.pip install google-api-python-client
2.pip install streamlit
3.pip install pandas
4.pip install isodate
5.pip install streamlit-option-menu

## Approach

1. Create a .py python file and install the all the packages required for the project.
2. Establish a connection to the YouTube API V3, which allows us to retrieve channel and video data by utilizing the Google API client library for Python.
3. Create functions to get channel information, video ids, video information and comments information as a Data frame. 
4. Establish a connection to sqlite3 and create a cursor, which allows us to store our datas in cloud based database.
5. Transfer the Datas we collected from various channels using function we created.
6. Using SQL queries retrieved the data from sql database. 
7. Write the code for Streamlit application and using functions retrieved the data, retrieved the data is displayed within the Streamlit application.


## Problems faced during this project

1. API key Quota - Only limited units of datas can be retrieved from the Youtube per Day, So you have to wait till next day or should creat a new API key. Can also avoid this problem by avoiding using big youtube channels.
2. next_page_token - We can get all the video id information as output, only 5 information will be avilable. By using maxResults and pageToken, we can get all information.
3. Video duration perameter - Video duration perameter will be in isodate formate to convert it into time formate we should pip install isodate and from isodate import parse_duration and use it in a function to return time date formate,
4. Nested if buttons in Streamlit - It's not possible to create a nested if buttons in Streamlit besause when we choose second button the first button status will become False. Insterd of using nested if button we can use option-menu by pip install streamlit-option-menu.
