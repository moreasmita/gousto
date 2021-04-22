# gousto
Data Engineering Test
users activity reporting
Scenario
At Gousto we use the open source event analytics platform ​Snowplow.​ It enables us to collect very granular data about our customers from various sources as a unified customer view. We use this data to analyse customers' behaviour and to train our machine learning algorithms for personalisation and to predict the right actions to engage customers as much as possible.
For daily reporting purposes we are interested in the following metrics:
● number of active users​ (users who visited our website on day X and day X-1)
● number of inactive users (users who didn't visit our website on day X and neither
on day X-1)
● number of churned users (users who visited our website on day X-1, but didn't visit
on day X)
● number of reactivated users (users who visited our website on day X, but didn't
visit on day X-1)
● [optional] ​number of sessions (we define session as a set of events per user with
gaps no more than 30 minutes)
Your objectives
The data to be used for this test is available at https://s3-eu-west-1.amazonaws.com/gousto-hiring-test/data-engineer/events.gz.​ It contains about 1.5 million events and for this test we included page-views only.
Each event (record) has the following attributes: ​event_id,​ ​timestamp,​ ​user_fingerprint​, domain_userid,​ ​network_userid​, p​ age.​
Requirements
