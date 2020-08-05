####################### MySQL SETUP #######################
# Creating the database
CREATE DATABASE FinalPJ;

# Selecting the database just created
USE FinalPJ;

####################### Data Processing ########################
################ Table User_purchase_history ################
# Data Loading 
CREATE TABLE IF NOT EXISTS User_purchase_history (
UserID INTEGER,
ItemID INTEGER,
Timestamp INTEGER
);

LOAD DATA
     INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/user_purchase_history.csv' 
INTO TABLE User_purchase_history
FIELDS 
     TERMINATED BY ','
lINES
	 TERMINATED BY '\n'        
	 STARTING BY '' 
     IGNORE 1 LINES
(@UserID, @ItemID, @Timestamp)
SET
     UserID = NULLIF(@UserID, ''),
     ItemID = NULLIF(@ItemID, ''),
     Timestamp = NULLIF(@Timestamp, '')
     ;

DESCRIBE User_purchase_history;

# Adjust the form of Timestamp
ALTER TABLE User_purchase_history 
ADD COLUMN datentime TIMESTAMP(0) NULL;

SET SQL_SAFE_UPDATES = 0;
UPDATE User_purchase_history
SET 
     datentime = FROM_UNIXTIME(Timestamp);

ALTER TABLE User_purchase_history 
ADD COLUMN dates CHAR(10) NULL;

SET SQL_SAFE_UPDATES = 0;
UPDATE User_purchase_history
SET
	 dates = SUBSTRING(datentime FROM 1 FOR 10);
     
ALTER TABLE User_purchase_history 
ADD COLUMN hours CHAR(10) NULL;

SET SQL_SAFE_UPDATES = 0;
UPDATE User_purchase_history
SET
     hours = SUBSTRING(datentime FROM 12 FOR 8);

# Time Outlier Processing - Only Saving Time from November 25 to December 03, 2017
SELECT MAX(Timestamp), MIN(Timestamp), MAX(datentime), MIN(datentime) 
     FROM User_purchase_history;

DELETE 
     FROM User_purchase_history 
	 WHERE datentime < '2017-11-25 00:00:00' 
	    OR datentime > '2017-12-04 00:00:00';

SELECT MAX(Timestamp), MIN(Timestamp), MAX(datentime), MIN(datentime) 
	 FROM User_purchase_history;

DESCRIBE User_purchase_history;

################ Table Item_category ################
# Data Loading 
CREATE TABLE IF NOT EXISTS Item_category(    
ItemID INTEGER,    
CategoryID INTEGER
);

LOAD DATA 
     INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/item_category.csv' 
INTO TABLE Item_category  
FIELDS               
     TERMINATED BY ','        
LINES               
     TERMINATED BY '\n'                
     STARTING BY ''               
     IGNORE 1 LINES     
(@ItemID, @CategoryID)    
SET     
     ItemID = NULLIF(@ItemID,''),     
     CategoryID = NULLIF(@CategoryID,'')
;

DESCRIBE Item_category;

################ Table User_behavior_history ################
# Data Loading 
CREATE TABLE IF NOT EXISTS User_behavior_history(    
Timestamp INTEGER,    
UserID INTEGER,    
Behavior_type varchar(20)
);

 LOAD DATA 
      INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/user_behavior_history.csv' 
 INTO TABLE User_behavior_history  
 FIELDS               
      TERMINATED BY ','        
 LINES               
      TERMINATED BY '\n'                
      STARTING BY ''               
      IGNORE 1 LINES     
 (@UserID, @Behavior_type, @Timestamp)    
 SET
      UserID = NULLIF(@UserID,''),
      Behavior_type = NULLIF(@Behavior_type,''),
      Timestamp = NULLIF(@Timestamp,'')
 ;

DESCRIBE User_behavior_history;

# Adjust the form of Timestamp
ALTER TABLE User_behavior_history 
ADD COLUMN datentime TIMESTAMP(0) NULL;

SET SQL_SAFE_UPDATES = 0;
UPDATE User_behavior_history
SET 
     datentime = FROM_UNIXTIME(Timestamp);

ALTER TABLE User_behavior_history 
ADD COLUMN dates CHAR(10) NULL;

SET SQL_SAFE_UPDATES = 0;
UPDATE User_behavior_history
SET
	 dates = SUBSTRING(datentime FROM 1 FOR 10);
     
ALTER TABLE User_behavior_history 
ADD COLUMN hours CHAR(10) NULL;

SET SQL_SAFE_UPDATES = 0;
UPDATE User_behavior_history
SET
     hours = SUBSTRING(datentime FROM 12 FOR 8);

DESCRIBE User_behavior_history;

# Time Outlier Processing - Only Saving Time from November 25 to December 03, 2017
SELECT MAX(Timestamp), MIN(Timestamp), MAX(datentime), MIN(datentime) 
     FROM User_behavior_history;

DELETE 
     FROM User_behavior_history 
	 WHERE datentime < '2017-11-25 00:00:00' 
	    OR datentime > '2017-12-04 00:00:00';

SELECT MAX(Timestamp), MIN(Timestamp), MAX(datentime), MIN(datentime) 
	 FROM User_behavior_history;
     
SHOW TABLES;

####################### User Behavior Analysis ########################
######### Flow #########
##### UV #####
SELECT COUNT(DISTINCT UserID)  AS 'UV' 
	 FROM User_behavior_history;
     
##### PV #####
SELECT COUNT(*) AS 'PV' 
	 FROM User_behavior_history 
	 WHERE Behavior_type = 'pv';

##### UV/PV #####
SELECT 
    (SELECT COUNT(*)
	FROM User_behavior_history
	WHERE Behavior_type = 'pv')/(COUNT(DISTINCT UserID) ) AS 'PV/UV'
    FROM User_behavior_history;

####################### RFM Model ########################
# RFM is a method used for analyzing customer value.
# RFM stands for the three dimensions: 
# Recency – How recently did the customer purchase?
# Frequency – How often do they purchase?
# Monetary Value – How much do they spend?

# Because the data source does not contain monetary value, we score customer value based on the R and F.

CREATE TABLE RFM
(
SELECT R.UserID,F.Frequency,R.RecentRank,F.FreqRank,
CONCAT(CASE WHEN RecentRank<=(19966)/2 THEN '0' 
            ELSE '1' END ,
       CASE WHEN FreqRank<=(19966)/2 THEN '0'
            ELSE '1' END) 
            AS UserValue
FROM 
(SELECT a.*,(@rank:=@rank+1) as RecentRank
FROM 
((SELECT UserID,DATEDIFF('2017-12-04',MAX(datentime)) AS Recent
FROM User_purchase_history
GROUP BY UserID
ORDER BY Recent) AS a ,(SELECT @rank:=0) AS b )) AS R,
(SELECT a.*,@rank1:=@rank1+1 AS FreqRank
FROM 
((SELECT UserID,COUNT(*) AS Frequency
FROM User_purchase_history
GROUP BY UserID
ORDER BY Frequency DESC) AS a ,(SELECT @rank1:=0) AS b)) AS F 
WHERE R.UserID=F.UserID)
;

SHOW TABLES;

SELECT *,
(CASE
WHEN UserValue='00' THEN 'Valued customer'
WHEN UserValue='10' THEN 'Important customers'  
WHEN UserValue='01' THEN 'Retained customers'
WHEN UserValue='11' THEN 'Potential customers'
END) AS Label
FROM RFM
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Label.csv'    
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n' 
;

# Item sales ranking
SELECT ItemBuytimes, COUNT(*) AS ItemTypecount
FROM
(SELECT COUNT(UserID) AS ItemBuytimes
FROM User_purchase_history
GROUP BY ItemID) AS ItemBuypool
GROUP BY ItemBuytimes
ORDER BY ItemBuytimes ASC
;

# Cumulative sales ranking of product categories
CREATE TABLE Item(
SELECT UserID,User_purchase_history.ItemID,CategoryID
FROM User_purchase_history  
LEFT JOIN Item_category
ON User_purchase_history.ItemID=Item_category.ItemID);

SELECT CateBuytimes, COUNT(*) AS CateTypecount
FROM
(
SELECT COUNT(UserID) AS CateBuytimes
FROM Item
GROUP BY CategoryID
) AS CateBuypool
GROUP BY CateBuytimes
ORDER BY CateBuytimes ASC;
