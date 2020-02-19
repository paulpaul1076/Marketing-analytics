Marketing analytics.

**How to build.**

Issue sbt assembly when within the project directory. I've used sbt version 0.13.8.

**How to run.**

I've ran this on an EMR cluster with this command:

spark-submit --driver-class-path MarketingAnalytics.jar --class com.company.MainNonSQL --jars hadoop-lzo-0.4.16.jar --master yarn MarketingAnalytics.jar -clickStreamPath hdfs:/user/clickStream.csv --purchaseStreamPath hdfs:/user/purchaseStream.csv

Sample clickStream.csv and purchaseStream.csv are available in src/test/scala/resources.

**My assumptions about the task.**

I assume that the application in question uses single sign on, meaning one user can't use the same website/phone app from multiple devices. Otherwise, it is impossible to generate sessionId's properly.

**How my code works.**
1) I use window functions to partition the table by userId, and order by eventTime. Then, I assign the number of encountered "app_open" events so far within 1 partition to the row that I am going over, thereby generating a sessionId per each row of the series of events in each session. <- I create a CTE for that.
2) After I generate the sessionId's, I create a "flattened" table, by eleminating all the events per each session, except "purchase" and "app_open" ones. I do this by joining the table with itself on sessionId, where the left side's eventType = "purchase" and the right side's eventType = "app_open". This allows my application to work even when there are multiple purchases within one session. Per each such joined row, I keep attributes of "app_open" and "purchase" events.
3) I select the necessary columns from the table obtained in the previous step, and use regular expressions to extract campaign_id, channel_id and purchase_id. I use a construct known as "look-behind", to find a position in the json, where "purchase_id:" occurs, for example, and consume everything that isn't a quote from then on. This allows me to properly extract the information that I need.
4) I then obtain the purchase attribution stream by joining the table from the previous step with the purchaseStream table.
5) In order by calculate top 10 campaigns I simply group by each campaign and sum the billingCost per each group. The I order by sum(billingCost) in descending order and limit the output by 10.
6) I calculate the most effective marketing channel by grouping by the channel_id, and per each group I count the distinct number of sessions. Then I order by count(distinct sessionId) in descending order and limit the output by 1. 
