# jdbc Component Change Log 


## [Unrelased]
### Added 
### Change 
### Deprecated 
### Removed 
### Fixed 
### Security 

## [V2.0] 2018-09-19 elastic.io 

### Added 

Triggers 
- SELECT 
- GET ROWS POLLING

Actions 
- SELECT 
- LOOKUP BY PRIMARY KEY
- UPSERT BY PRIMARY KEY (for migration)
- DELETE BY PRIMARY KEY

### Removed 
Actions 
- CreateOrUpdateRecord

### Fixed 
- fix issue in postgresql - getDate(null)
- fix null values as input for select

