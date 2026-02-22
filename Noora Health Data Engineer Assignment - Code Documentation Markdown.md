# **Noora Health – Data Engineer Assignment**

*This document describes the end-to-end ELT pipeline built to ingest, transform, validate, and analyze WhatsApp intervention data. The raw data consists of two append only tables (messages and statuses). The solution preserves full lifecycle history while enabling analytical use cases.*

1. ## **Data Ingestion (Airbyte → BigQuery)**

GitHub (messages.csv, statuses.csv)  
        ↓  
Airbyte Cloud (HTTPS Source)  
        ↓  
BigQuery Dataset: noora\_health\_raw  
        ↓  
Tables:  
  \- noora\_raw\_files (messages)  
  \- noora\_statuses\_file (statuses)

The raw CSV files were hosted on GitHub and ingested into BigQuery using Airbyte Cloud.  
Since both source tables are append-only, no transformation was performed at ingestion time.  
The raw layer remains immutable and serves as the source of truth.

2. ## **Transformation Logic**

**Deduplicate Messages (Append-Only Handling)**

WITH latest\_messages AS (  
  SELECT \*  
  FROM (  
    SELECT \*,  
           PARSE\_TIMESTAMP('%m/%d/%Y %H:%M:%S', inserted\_at) AS inserted\_at\_ts,  
           ROW\_NUMBER() OVER (  
             PARTITION BY id  
             ORDER BY PARSE\_TIMESTAMP('%m/%d/%Y %H:%M:%S', inserted\_at) DESC  
           ) AS rn  
    FROM \`noora\_health\_raw.noora\_raw\_files\`  
  )  
  WHERE rn \= 1  
)

The messages table is append-only. If a message changes, a new row is inserted instead of updating the existing record.  
To produce one row per logical message, the latest version was selected using ROW\_NUMBER() ordered by inserted\_at.  
This ensures:

* Only one record per message\_id  
* No loss of final message state

**Preserve Full Status Lifecycle**

status\_history AS (  
  SELECT  
    message\_id,  
    ARRAY\_AGG(  
      STRUCT(  
        status,  
        PARSE\_TIMESTAMP('%m/%d/%Y %H:%M:%S', timestamp) AS timestamp  
      )  
      ORDER BY PARSE\_TIMESTAMP('%m/%d/%Y %H:%M:%S', timestamp)  
    ) AS status\_events  
  FROM \`noora\_health\_raw.noora\_statuses\_file\`  
  GROUP BY message\_id  
)

The statuses table is also append-only and records lifecycle transitions (sent, delivered, read, failed).  
Instead of flattening or selecting only the latest status, all status events were aggregated into an ordered ARRAY.  
This preserves:

* Complete message lifecycle  
* Temporal ordering  
* Historical traceability

**Final Analytical Table**

CREATE OR REPLACE TABLE noora\_health\_analytics.messages\_enriched AS

WITH latest\_messages AS (  
  SELECT \*  
  FROM (  
    SELECT \*,  
           PARSE\_TIMESTAMP('%m/%d/%Y %H:%M:%S', inserted\_at) AS inserted\_at\_ts,  
           ROW\_NUMBER() OVER (  
             PARTITION BY id  
             ORDER BY PARSE\_TIMESTAMP('%m/%d/%Y %H:%M:%S', inserted\_at) DESC  
           ) AS rn  
    FROM \`noora\_health\_raw.noora\_raw\_files\`  
  )  
  WHERE rn \= 1  
),

status\_history AS (  
  SELECT  
    message\_id,  
    ARRAY\_AGG(  
      STRUCT(  
        status,  
        PARSE\_TIMESTAMP('%m/%d/%Y %H:%M:%S', timestamp) AS timestamp  
      )  
      ORDER BY PARSE\_TIMESTAMP('%m/%d/%Y %H:%M:%S', timestamp)  
    ) AS status\_events  
  FROM \`noora\_health\_raw.noora\_statuses\_file\`  
  GROUP BY message\_id  
)

SELECT  
  m.id AS message\_id,  
  m.masked\_author,  
  m.masked\_addressees,  
  m.content,  
  m.direction,  
  m.inserted\_at\_ts AS message\_inserted\_at,  
  s.status\_events  
FROM latest\_messages m  
LEFT JOIN status\_history s  
  ON m.id \= s.message\_id;

The final table contains one row per logical message.  
Design characteristics:

* Deduplicated message records  
* Full lifecycle preserved  
* User identifiers included for analytics  
* Proper TIMESTAMP typing for downstream calculations

3. ## **Data Validation**

**Duplicate Message Detection**

WITH duplicates AS (  
  SELECT  
    m1.message\_id AS msg1,  
    m2.message\_id AS msg2,  
    m1.content,  
    m1.message\_inserted\_at AS inserted\_at\_1,  
    m2.message\_inserted\_at AS inserted\_at\_2  
  FROM \`noorahealth-488108.noora\_health\_analytics.messages\_enriched\` m1  
  JOIN \`noorahealth-488108.noora\_health\_analytics.messages\_enriched\` m2  
    ON m1.content \= m2.content  
    AND m1.message\_id \< m2.message\_id  
    AND ABS(  
          TIMESTAMP\_DIFF(  
            m1.message\_inserted\_at,  
            m2.message\_inserted\_at,  
            SECOND  
          )  
        ) \<= 60  
)

SELECT \*  
FROM duplicates;

**Explanation**

Messages were considered potential duplicates if:

* Content is identical  
* Inserted within 60 seconds  
* Message IDs are different

This flags suspicious retries while avoiding self-matching rows.

**Referential Integrity Check**

SELECT s.message\_id  
FROM \`noorahealth-488108.noora\_health\_raw.noora\_statuses\_file\` s  
LEFT JOIN \`noorahealth-488108.noora\_health\_raw.noora\_raw\_files\` m  
  ON s.message\_id \= m.id  
WHERE m.id IS NULL;

**Explanation**

This query checks for orphan status records.

Result: No rows returned, confirming that all status events correspond to a valid message.

**Lifecycle Temporal Integrity**

WITH flattened AS (  
  SELECT  
    message\_id,  
    s.status,  
    s.timestamp  
  FROM \`noorahealth-488108.noora\_health\_analytics.messages\_enriched\`,  
  UNNEST(status\_events) s  
),

sent\_times AS (  
  SELECT  
    message\_id,  
    MIN(timestamp) AS sent\_time  
  FROM flattened  
  WHERE status \= 'sent'  
  GROUP BY message\_id  
)

SELECT f.\*  
FROM flattened f  
JOIN sent\_times s  
  ON f.message\_id \= s.message\_id  
WHERE f.status \= 'read'  
  AND f.timestamp \< s.sent\_time;

**Explanation**

This validation ensures lifecycle ordering:  
sent → delivered → read  
The query checks whether any "read" timestamp occurs before the earliest "sent" timestamp.  
Result: No temporal inconsistencies detected.

**Outbound Messages Without Status Events**

SELECT \*  
FROM \`noorahealth-488108.noora\_health\_analytics.messages\_enriched\`  
WHERE direction \= 'outbound'  
  AND (status\_events IS NULL OR ARRAY\_LENGTH(status\_events) \= 0);  
Outbound messages should typically have at least one lifecycle status (sent, delivered, read, or failed).

This query identifies outbound messages without any recorded status events.

4. ## **Data Visualization** 

**Dependencies** 

pip install google-cloud-bigquery pandas plotly 

pip install db-dtypes

import os

from google.cloud import bigquery

**Setting Up Environment**   
*os.environ\["GOOGLE\_APPLICATION\_CREDENTIALS"\] \= r"C:/Users/your\_json\_key"*

*client \= bigquery.Client()*

**Total Users vs Active Users Over Time**

Definitions:

Total users \= distinct users who sent OR received a message

Active users \= distinct users who sent inbound messages

query\_users \= """  
WITH base AS (  
  SELECT  
    DATE\_TRUNC(DATE(message\_inserted\_at), WEEK) AS week,  
    CASE  
      WHEN direction \= 'inbound' THEN masked\_author  
      WHEN direction \= 'outbound' THEN masked\_addressees  
    END AS user\_id,  
    direction  
  FROM \`noorahealth-488108.noora\_health\_analytics.messages\_enriched\`  
)

SELECT  
  week,  
  COUNT(DISTINCT user\_id) AS total\_users,  
  COUNT(DISTINCT IF(direction \= 'inbound', user\_id, NULL)) AS active\_users  
FROM base  
GROUP BY week  
ORDER BY week  
"""

df\_users \= client.query(query\_users).to\_dataframe()  
df\_users.head()

import plotly.graph\_objects as go

fig \= go.Figure()

fig.add\_trace(go.Scatter(

    x=df\_users\["week"\],

    y=df\_users\["total\_users"\],

    mode="lines+markers",

    name="Total Users"

))

fig.add\_trace(go.Scatter(

    x=df\_users\["week"\],

    y=df\_users\["active\_users"\],

    mode="lines+markers",

    name="Active Users"

))

fig.update\_layout(

    title="Total vs Active Users Over Time",

    xaxis\_title="Week",

    yaxis\_title="Number of Users",

    template="plotly\_white"

)

fig.show()

**Fraction of Non-Failed Outbound Messages That Were Read**

Definition:

Considered outbound messages only

Excluded messages with status "failed"

query\_read\_rate \= """

WITH flattened AS (

  SELECT

    message\_id,

    direction,

    s.status

  FROM \`noorahealth-488108.noora\_health\_analytics.messages\_enriched\`,

  UNNEST(status\_events) s

  WHERE direction \= 'outbound'

)

SELECT

  COUNT(DISTINCT IF(status \!= 'failed', message\_id, NULL)) AS non\_failed,

  COUNT(DISTINCT IF(status \= 'read', message\_id, NULL)) AS read\_messages

FROM flattened

"""

df\_read \= client.query(query\_read\_rate).to\_dataframe()

read\_rate \= df\_read\["read\_messages"\]\[0\] / df\_read\["non\_failed"\]\[0\]

read\_rate

print(f"Read Rate: {read\_rate:.2%}")

import plotly.graph\_objects as go

labels \= \["Read", "Not Read"\]

values \= \[df\_read\["read\_messages"\]\[0\], df\_read\["non\_failed"\]\[0\] \- df\_read\["read\_messages"\]\[0\]\]

fig \= go.Figure(go.Pie(

    labels=labels,

    values=values,

    hole=0.6,

    marker\_colors=\["\#2ecc71", "\#e0e0e0"\]

))

fig.update\_layout(

    title="Outbound Message Read Rate",

    annotations=\[dict(text=f"{values\[0\]/sum(values):.1%}", font\_size=24, showarrow=False)\]

)

fig.show()

**Distribution of Time Between Sent and Read**

read\_timestamp \- sent\_timestamp

query\_read\_time \= """  
WITH flattened AS (  
  SELECT  
    message\_id,  
    s.status,  
    s.timestamp  
  FROM \`noorahealth-488108.noora\_health\_analytics.messages\_enriched\`,  
  UNNEST(status\_events) s  
  WHERE direction \= 'outbound'  
),

sent\_times AS (  
  SELECT  
    message\_id,  
    MIN(timestamp) AS sent\_time  
  FROM flattened  
  WHERE status \= 'sent'  
  GROUP BY message\_id  
),

read\_times AS (  
  SELECT  
    message\_id,  
    MIN(timestamp) AS read\_time  
  FROM flattened  
  WHERE status \= 'read'  
  GROUP BY message\_id  
)

SELECT  
  TIMESTAMP\_DIFF(r.read\_time, s.sent\_time, SECOND) AS seconds\_to\_read  
FROM sent\_times s  
JOIN read\_times r  
  ON s.message\_id \= r.message\_id  
WHERE r.read\_time IS NOT NULL  
"""

df\_read\_time \= client.query(query\_read\_time).to\_dataframe()  
df\_read\_time.head()

import plotly.express as px

fig \= px.histogram(  
    df\_read\_time,  
    x="seconds\_to\_read",  
    nbins=50,  
    title="Distribution of Time to Read (Seconds)"  
)

fig.show()

**Outbound Messages in the Last Week by Status**

query\_last\_week \= """  
WITH flattened AS (  
  SELECT  
    DATE(s.timestamp) AS status\_date,  
    s.status  
  FROM \`noorahealth-488108.noora\_health\_analytics.messages\_enriched\`,  
  UNNEST(status\_events) s  
  WHERE direction \= 'outbound'  
),

max\_date AS (  
  SELECT MAX(status\_date) AS max\_status\_date  
  FROM flattened  
)

SELECT  
  f.status,  
  COUNT(\*) AS message\_count  
FROM flattened f  
CROSS JOIN max\_date m  
WHERE f.status\_date BETWEEN DATE\_SUB(m.max\_status\_date, INTERVAL 6 DAY)  
                         AND m.max\_status\_date  
GROUP BY f.status  
ORDER BY message\_count DESC  
"""

df\_last\_week \= client.query(query\_last\_week).to\_dataframe()  
df\_last\_week

import plotly.express as px

fig \= px.bar(  
    df\_last\_week,  
    x="status",  
    y="message\_count",  
    title="Outbound Messages (Last 7 Days in Dataset) by Status",  
    template="plotly\_white"  
)

fig.show()

