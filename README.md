# Data Modeling With Postgres
Project repo for the Udacity Data Engineering Program Project 1.

This README file includes a summary of the project, how to run the Python scripts, and an explanation of the files in the repository.

## Getting Started

1.  You will need to first connect and build the tables for the  Sparkify database.  To do this run the following command:

> python create_tables.py

2. To load data into the  Sparkify database run the following command:

>python etl.py


## Prerequisites
1.  Baseline configured PostgreSQL database.

2.  The following Python libraries need to be installed in the environment.
    * psycopg
    * pandas

## Purpose
The purpose of this database is to conduct ETL operations and store data from user activity from the Sparkify app.  
This data will be used by the Sparkify analytics team will use this data gain a greater understanding of user activity and songs being listened to.


## Database Schema
There are 5 tables in the database.  This design focuses on the songplay table which houses the most important information for the analytics team.  The supporting tables of time, users, songs, and artists help to provide context and additional details for the songplay table.
The **time table** which contains:

| Field        | Data Type          | Key  |
 |-------------  | ------------- | ------------- |
| start_time      | INT | Primary |
| hour      | INT      |    |
| day | INT      |     |
| week | INT      |     |
| month | INT      |     |
| year | INT      |     |
| weekday | INT      |     |

The **users table** which contains:

| Field        | Data Type          | Key  |
| ------------- | ------------- |  ------------- |
| user_id      | VARCHAR | Primary |
| first_name      | VARCHAR      |    |
| last_name | VARCHAR      |     |
| gender | VARCHAR      |     |
| level | VARCHAR     |     |

The **songs table** which contains:

| Field        | Data Type          | Key  |
| ------------- | ------------- |  ------------- |
| song_id      | VARCHAR | Primary |
| title      | VARCHAR      |    |
| artist_id | VARCHAR      |  Foreign Key   |
| year | INT      |     |
| duration | NUMERIC     |     |

The **artists table** which contains:

| Field        | Data Type          | Key  |
| ------------- | ------------- |  ------------- |
| artist_id      | VARCHAR | Primary |
| name      | VARCHAR      |    |
| location | VARCHAR      |  Foreign Key   |
| latitude | real      |     |
| longitude | real     |     |

The **songplay table** which contains:

| Field        | Data Type          | Key  |
| ------------- | ------------- |  ------------- |
| songplay_id      | INT | Primary |
| start_time      | INT      |  Foreign Key  |
| user_id | VARCHAR  |  Foreign Key   |
| song_id | VARCHAR      |  Foreign Key   |
| artist_id | VARCHAR     |  Foreign Key   |
| session_id | INT  |     |
| location | VARCHAR      |     |
| user_agent | VARCHAR     |     |

![ERD Diagram](./assets/images/erd.png)


## Example Queries
To get the top 5 users by session.

>SELECT users.last_name, users.first_name, COUNT(songplay.user_id)
FROM songplay
INNER JOIN users ON songplay.user_id=users.user_id
GROUP BY users.user_id
ORDER BY count DESC
LIMIT 5;

![top five](./assets/images/top_5.png)

To get the 10 locations with the least amount of user sessions

>SELECT songplay.location, count(songplay.location)
FROM songplay
where location is not null
GROUP BY songplay.location
ORDER BY count
limit 10;

![top five](./assets/images/bottom10.png)
