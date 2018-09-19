# jdbc Component Change log 


## [Unrelased]
### added 
### change 
### deprecated 
### removed 
### fixed 
### security 

## [V2.0] 2018-09-19 elastic.io 

### added 

triggers 
- SELECT 
- GET ROWS POLLING

actions 
- SELECT 
- LOOKUP BY PRIMARY KEY
- UPSERT BY PRIMARY KEY (for migration)
- DELETE BY PRIMARY KEY

### removed 
actions 
- CreateOrUpdateRecord

### fixed 
- fix issue in postgresql - getDate(null)
- fix null values as input for select

