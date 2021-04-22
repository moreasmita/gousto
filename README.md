# gousto
## **Data Engineering Test** (Users activity reporting)

#### Scenario
At Gousto we use the open source event analytics platform ​Snowplow.​ It enables us to collect very granular data about our customers from various sources as a unified customer view. We use this data to analyse customers' behaviour and to train our machine learning algorithms for personalisation and to predict the right actions to engage customers as much as possible.

For daily reporting purposes we are interested in the following metrics:

- Number of active users (users who visited our website on day X and day X-1)
- Number of inactive users (users who didn't visit our website on day X and neither on day X-1)
- Number of churned users (users who visited our website on day X-1, but didn't visit on day X)
- Number of reactivated users (users who visited our website on day X, but didn't visit on day X-1)


Objectives
The data to be used for this test is available at https://s3-eu-west-1.amazonaws.com/gousto-hiring-test/data-engineer/events.gz.

It contains about 1.5 million events and for this test we included page-views only.
Each event (record) has the following 

## **Attributes: **
- event_id :  A UUID for each event	
- timestamp :  Timestamp event was recorded
- user_fingerprint : A user fingerprint generated by looking at the individual browser features
- domain_userid  : User ID set by Snowplow using 1st party cookie
- ​network_userid​ : User ID set by Snowplow using 3rd party cookie
- page : Referer page path

## Git Folder Structure :
```
1. SQL
    - bigquery.sql [SQL queries added for reference]
2. dags
    - gousto_de_test.py [Airflow Dag file]
3. data/output_data
    - Output of daily_report [Added for reference]
```
