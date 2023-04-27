# Project Brief: Music Dataset Project using Apache Airflow

**Introduction:**

As a group we decided to look a dataset which is about music and we aim to create a data pipeline for a music streaming service using Apache Airflow. The datasets will contain information about songs, listener data, and lyric content. The primary objective is to develop a data pipeline that can efficiently deliver the data needed to optimize the music streaming service and provide new information to the streaming service about who is using their website and how. 

**Background:**

The music streaming service is one of the largest industries in the entertainment sector. Brands are always looking for new ways to improve their service to listeners and provide the best recommendations to the listener depending on the user’s history. It is a large market and data analysis is a key factor in making your service better than others. 

**Datasets:**

The datasets used in this project will be obtained from the Million Song Dataset (http://millionsongdataset.com/). This dataset contains information on over a million songs, including metadata, lyrics, and acoustic features. The datasets will be coordinated to extract and transform relevant data which can be used for a variety of things. 

**Pipeline:**

The data pipeline will be created using Apache Airflow. The pipeline will extract data from the million-song dataset and then it will be able to transform the data into a format that can be analysed and so that the data can be loaded into the into the service's database. The pipeline will be designed to handle large volumes of data and ensure data consistency and quality. Creating a pipe line which produces effienct and useable data is one of the most important factors when analysing data, and this is where most of the time for the project will go to. 

**Tasks:**

Design the data pipeline architecture

Identify and extract relevant data from the datasets

Transform the data to a useable format

Load the transformed data and analyse it

Create data validation checks to ensure data quality and consistency

Schedule the pipeline to run on a regular basis



**Timeline:**

The project has been divided into several stages to ensure smooth and efficient data delivery this is always updated on our GitHub so we can keep track of completed tasks. The first stage of the pipeline will involve extracting the data from the Million Song Dataset. The data will be obtained and the extracted data will be in a raw format that will require transformation to make it compatible and understandable. Only some categories of the dataset will be looked at for example: Looking at how many times a certain word appears in a song compared to other similar songs from the same genre. This will be useful as they can see weather the listener tends to like songs with certain words. 
The second stage of the pipeline will involve cleaning and transforming the data. This stage will involve in filtering out irrelevant data points. Data cleaning will involve removing duplicates, handling missing data, and removing any anomalies in the data. 
The final stage of the pipeline will involve validating the data to ensure data quality and consistency. Data validation checks will be created to detect any errors or inconsistencies in the data. If any errors are detected, the pipeline will be stopped, and the issue will be investigated and resolved.
Timeline:

**Week 1: Planning and data exploration**

During the first week, the team will plan and define the project scope. They will identify the relevant data from the Million Song Dataset, determine the data pipeline architecture, and set up the Apache Airflow environment.

**Week 2-3: Data Extraction and Transformation**

During the second and third weeks, the team will extract data from the Million Song Dataset using the API and transform the data into a format that can be used 

**Week 4: Data Loading**

During the fourth week, the team will load the transformed data and analyse it

**Week 5: Data Validation**

During the final week, the team will create data validation checks to ensure data quality and consistency. They will also test the pipeline and make any necessary changes to ensure its smooth and efficient operation.

The last week consisted of  Implementing the Spotify API and making sure all the dags are fully set up
