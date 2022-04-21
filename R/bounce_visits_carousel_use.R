###do bounce visits use the carousel?
options(java.parameters = "-Xmx64g")
library(stringr)
library(RJDBC)
library(tidyverse)
library(lubridate)
library(httr)
library(ggplot2)
library(scales)
library(RColorBrewer)
library(wesanderson)
library(ggrepel)
theme_set(theme_classic())

##### functions ######
### round up/down to nearest value for the axes limits
round_any <- function(x, accuracy, func){func(x/ accuracy) * accuracy}

######### Get Redshift creds (local R) #########
driver <-JDBC("com.amazon.redshift.jdbc41.Driver","~/.redshiftTools/redshift-driver.jar",identifier.quote = "`")
my_aws_creds <-read.csv("~/Documents/Projects/DS/redshift_creds.csv",header = TRUE,stringsAsFactors = FALSE)
url <-paste0("jdbc:redshift://localhost:5439/redshiftdb?user=",my_aws_creds$user,"&password=",my_aws_creds$password)
conn <- dbConnect(driver, url)

# ######### Get Redshift creds MAP #########
# get_redshift_connection <- function() {
#   driver <-JDBC(driverClass = "com.amazon.redshift.jdbc.Driver",classPath = "/usr/lib/drivers/RedshiftJDBC42-no-awssdk-1.2.41.1065.jar",identifier.quote = "`")
#   url <-str_glue("jdbc:redshift://live-idl-prod-redshift-component-redshiftcluster-1q6vyltqf8lth.ctm1v7db0ubd.eu-west-1.redshift.amazonaws.com:5439/redshiftdb?user={Sys.getenv('REDSHIFT_USERNAME')}&password={Sys.getenv('REDSHIFT_PASSWORD')}")
#   conn <- dbConnect(driver, url)
#   return(conn)
# }
# conn <- get_redshift_connection()

# test that it works:
dbGetQuery(conn,"select distinct brand_title, series_title from prez.scv_vmb ORDER BY RANDOM() limit 10;")



dates<- dbGetQuery(conn, 'SELECT * FROM central_insights_sandbox.vb_dates_bounce ORDER BY 1;')
dates[1,]

for(date in 1:nrow(dates)){
  print(paste0(dates[date,], "        ", Sys.time()))

get_cookies <- paste0(
"DROP TABLE IF EXISTS central_insights_sandbox.vb_users_chrys;
CREATE TABLE central_insights_sandbox.vb_users_chrys AS
with visits as (
    SELECT DISTINCT dt::date, unique_visitor_cookie_id, visit_id,mobile_device_manufacturer
    FROM s3_audience.visits
    WHERE dt = REPLACE('",dates[date, ],"', '-', '')
    AND destination = 'PS_NEWS'
    AND is_signed_in = TRUE
    AND is_personalisation_on = TRUE
)
SELECT a.*,  unique_visitor_cookie_id, mobile_device_manufacturer
FROM central_insights_sandbox.vb_bounce_visits a
JOIN visits b on a.dt = b.dt  AND a.visit_id = b.visit_id
;")
dbSendUpdate(conn, get_cookies)

get_carousel <- paste0(
  "DROP TABLE IF EXISTS central_insights_sandbox.pub_data;
CREATE TABLE central_insights_sandbox.pub_data AS
SELECT DISTINCT dt::date,
                unique_visitor_cookie_id,
                visit_id,
                attribute
FROM s3_audience.publisher
WHERE dt = REPLACE('",dates[date, ],"', '-', '')
  AND destination = 'PS_NEWS'
  AND unique_visitor_cookie_id IN (SELECT DISTINCT unique_visitor_cookie_id FROM central_insights_sandbox.vb_users_chrys)
  AND placement = 'news.discovery.page'
  AND attribute = 'top-stories~carousel-scroll-start'
  AND publisher_clicks = 1"
)
dbSendUpdate(conn, get_carousel)


add_to_table <-
  "INSERT INTO central_insights_sandbox.vb_carousel_usage
SELECT a.dt,a.visit_id, a.app_type, b.unique_visitor_cookie_id, b.attribute
FROM central_insights_sandbox.vb_users_chrys a
LEFT JOIN central_insights_sandbox.pub_data b on a.dt = b.dt AND a.visit_id = b.visit_id
;"


dbSendUpdate(conn, add_to_table)

}


