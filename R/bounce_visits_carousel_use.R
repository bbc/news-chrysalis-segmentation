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
  "INSERT INTO central_insights_sandbox.vb_carousel_usage_2 
SELECT a.dt,a.visit_id, a.app_type, b.unique_visitor_cookie_id, b.attribute, mobile_device_manufacturer
FROM central_insights_sandbox.vb_users_chrys a
LEFT JOIN central_insights_sandbox.pub_data b on a.dt = b.dt AND a.visit_id = b.visit_id
;"


dbSendUpdate(conn, add_to_table)

}


carousel<- dbGetQuery(conn, "SELECT dt,
       CASE WHEN attribute ISNULL THEN 'no_scroll' ELSE 'scroll' END                   as carousel,
       CASE WHEN mobile_device_manufacturer = 'Apple' THEN 'iPhone' ELSE 'android' end as device_type
        ,
       count(*)                                                                        as visits
FROM central_insights_sandbox.vb_carousel_usage_2
GROUP BY 1, 2, 3
;")

carousel$dt<- ymd(carousel$dt)
carousel$visits <-as.numeric(carousel$visits)
carousel$carousel<-factor(carousel$carousel, levels = c("no_scroll","scroll"))
carousel %>% head()

carousel %>% 
  group_by(dt, device_type) %>% 
  mutate(perc = visits/sum(visits)) %>% 
  select(-visits) %>% 
  spread(key = carousel, value = perc)


plot_data<-carousel %>% 
  group_by(dt, device_type) %>% 
  mutate(perc = visits/sum(visits)) %>% 
  select(-visits)

plot_data %>% head()
line_data<-
plot_data  %>% 
  ungroup() %>% 
  group_by(device_type, carousel) %>% 
  mutate(mean = mean(perc)) %>% 
  filter(carousel == 'scroll')

x_axis_dates <- ymd(c(
  '2022-01-15',
  '2022-01-20',
  '2022-01-25',
  '2022-01-30',
  '2022-02-04',
  '2022-02-09',
  '2022-02-14',
  '2022-02-19',
  '2022-02-24',
  '2022-03-01',
  '2022-03-06',
  '2022-03-11',
  '2022-03-16',
  '2022-03-21',
  '2022-03-26',
  '2022-03-31'
))
ggplot(data = plot_data, aes(x = dt, y = perc, fill = carousel) )+
  geom_col(inherit.aes = TRUE,position = "stack", show.legend = TRUE) +
  geom_line(data =line_data, aes(x = dt,y = mean),linetype="dotted")+
  facet_wrap(~device_type , scales = "free_y", nrow = 2)+
  scale_y_continuous(breaks = c(0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0),
                     labels = scales::percent
                     )+
  ylab("percentage") +
  labs(title = "Percentage of visits using the Top Stories carousel")+
  scale_x_date(labels = date_format("%Y-%m-%d"),
               breaks = x_axis_dates)+
  geom_text_repel(data = line_data %>% filter(dt ==  ymd('2022-03-31')),
                  mapping = aes(x = ymd('2022-03-31'),
                                y = line_data$mean[line_data$dt == ymd('2022-03-31')], 
                                label = paste0("mean =", round(100*line_data$mean[line_data$dt == ymd('2022-03-31')],0),"%")
                  ),
                  hjust = "right",
                  colour = "black") +
  scale_fill_manual(values=c("light grey", "#999999"))
  






