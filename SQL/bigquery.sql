------------------------------ Number of active users start ------------------------------------
DROP TABLE
  gousto.active_users;

CREATE TABLE
  gousto.active_users AS
SELECT
  DISTINCT(user_fingerprint) AS user_fingerprint,
  DATE(timestamp_) AS date_
FROM
  gousto.events
WHERE
  --  date(timestamp_) >= CURDATE()-1  and date(timestamp_) <= CURDATE();
  DATE(timestamp_) >= '2014-10-28'
  AND DATE(timestamp_) <= '2014-10-29'
  AND user_fingerprint != '';
 
------------------------------ Number of active users end ------------------------------------


------------------------------ Number of inactive users start ------------------------------------

DROP TABLE
  gousto.inactive_users;
  
CREATE TABLE
  gousto.inactive_users AS
SELECT
  i.user_fingerprint
FROM
  gousto.events i
LEFT JOIN
  gousto.active_users a
ON
  i.user_fingerprint = a.user_fingerprint
WHERE
  a.user_fingerprint IS NULL
  AND i.user_fingerprint != '';

-- Q: why there are blank values?
-- A: Need to remove blank value for anonymus users(user who visited website without identity)

------------------------------ Number of inactive users end -------------------------------------


------------------------------ Number of churned users start ------------------------------------
DROP TABLE
 gousto.churned_data;

CREATE TABLE
  gousto.churned_data AS
SELECT
    -- COUNT(DISTINCT(prev_day.user_fingerprint))
  DISTINCT(prev_day.user_fingerprint)
FROM
  gousto.active_users prev_day
LEFT JOIN
  gousto.active_users curr_day
ON
  curr_day.user_fingerprint = prev_day.user_fingerprint
  AND curr_day.date_ = DATE_ADD(prev_day.date_,INTERVAL 1 DAY)
WHERE
  curr_day.user_fingerprint IS NULL
  AND prev_day.date_ = '2014-10-28' ; -- CURDATE()-1


-- Testing Query
-- SELECT
--   *
-- FROM
--   gousto.events
-- WHERE
--   user_fingerprint IN ('1403450075',
--     '3560249421',
--     '	2690893774',
--     '2132358225')
--   AND DATE(timestamp_) >= '2014-10-28'
--   AND DATE(timestamp_) <= '2014-10-29';
------------------------------ Number of churned users end --------------------------------------

------------------------------ Number of Reactivate users start -----------------------------------

DROP TABLE
  gousto.fisrt_active;

CREATE TABLE
  gousto.fisrt_active AS
SELECT
  DISTINCT user_fingerprint,
  MIN(DATE(timestamp_)) AS date_
FROM
  gousto.events
-- WHERE
--   timestamp_ != '2014-10-28'
GROUP BY
  user_fingerprint;


DROP TABLE
  gousto.reactivate_users;

CREATE TABLE
  gousto.reactivate_users AS
SELECT
  --     COUNT(DISTINCT(prev_day.user_fingerprint)),
  DISTINCT(curr_day.user_fingerprint),
  curr_day.date_
FROM
  gousto.active_users curr_day
LEFT JOIN
  gousto.active_users prev_day
ON
  curr_day.user_fingerprint = prev_day.user_fingerprint
  AND curr_day.date_ = DATE_ADD(prev_day.date_,INTERVAL 1 DAY)
JOIN
  gousto.fisrt_active firs
ON
  curr_day.user_fingerprint= firs.user_fingerprint
  AND firs.date_ != curr_day.date_
WHERE
  prev_day.user_fingerprint IS NULL
  AND curr_day.date_ = '2014-10-29';

------------------------------ Number of Reactivate users end -----------------------------------