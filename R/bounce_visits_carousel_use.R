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

######### find visits that bounce only on homepage ###########
# for(date in 1:nrow(dates)){
#   print(paste0(dates[date,], "        ", Sys.time()))
# pages_per_visit<- paste0(
#   "set search_path TO 'central_insights_sandbox';
#   DROP TABLE IF EXISTS vb_pages_per_visit;
# CREATE TABLE vb_pages_per_visit as
# with get_subsections as (
#     --- get the cleaned page name and the page section i.e World/politics
#     SELECT distinct REVERSE(SPLIT_PART(REVERSE(page_name), '::', 1)) AS page_name_cleaned,
#                     page_section
#     FROM s3_audience.audience_activity
#     WHERE destination = 'PS_NEWS'
#       AND dt = REPLACE('",dates[date,],"', '-', '')
#     AND page_section IS NOT NULL
#     GROUP BY 1, 2
# 
# ),
#      users as (
#          -- get users demographics and the pages they visit
#          SELECT DISTINCT date_of_event::date                                                                  as dt,
#                          audience_id,
#                          visit_id,
#                          date_of_event || '-' || visit_id                                                     as dist_visit,
#                          CASE
#                              WHEN app_name ILIKE '%chrysalis%' THEN 'chrysalis'
#                              WHEN app_type = 'responsive' OR app_type = 'web' OR app_type = 'amp' THEN 'web'
#                              WHEN app_type = 'mobile-app' THEN 'app'
#                              ELSE app_name END                                                                as app_type,
#                          CASE
#                              WHEN gender = 'male' THEN 'male'
#                              WHEN gender = 'female' THEN 'female'
#                              ELSE 'unknown' END                                                               as gender,
#                          CASE
#                              WHEN age_range IN ('16-19', '20-24') THEN '16-24'
#                              WHEN age_range IN ('25-29', '30-34') THEN '24-34'
#                              WHEN age_range IN ('35-39', '40-44') THEN '34-44'
#                              WHEN age_range IN ('45-49', '50-54') THEN '45-54'
#                              WHEN age_range IN ('55-59', '60-64', '65-70', '>70') THEN '55+'
#                              ELSE 'unknown' END                                                               as age_range,
#                          CASE
#                              WHEN acorn_category ISNULL THEN 'unknown'
#                              ELSE
#                                  LPAD(acorn_category::text, 2, '0') || '_' || acorn_category_description END  as acorn_cat,
#                          page_name,
#                          REVERSE(SPLIT_PART(REVERSE(page_name), '::', 1))                                     AS page_name_cleaned
#          FROM audience.audience_activity_daily_summary_enriched
#          WHERE destination = 'PS_NEWS'
#            AND date_of_event = '",dates[date,],"'
#            AND geo_country_site_visited = 'United Kingdom'
#            AND is_personalisation_on = TRUE
#            AND age_range NOT IN ('0-5', '6-10', '11-15')
#            AND app_type IS NOT NULL
#          ORDER BY 1, 2, 3
#      )
# 
# SELECT a.*, b.page_section
# FROM users a
#          LEFT JOIN get_subsections b on a.page_name_cleaned = b.page_name_cleaned
# ;
#   "
# )
# 
# 
# bounce_ids<- paste0("
# --identify the bounce visits
# set search_path TO 'central_insights_sandbox';
# DROP TABLE IF EXISTS vb_bounce_visit_ids;
# CREATE TABLE vb_bounce_visit_ids AS
# SELECT distinct dt, audience_id, visit_id, count(distinct page_name) as num_pages, dt||'-'|| visit_id as dist_visit
# FROM vb_pages_per_visit
# GROUP BY 1,2,3
# HAVING num_pages =1
# ORDER BY 1,2,3;")
# 
# 
# get_bounce_visits<- paste0("
# -- select the bounce visits
# set search_path TO 'central_insights_sandbox';
# DROP TABLE IF EXISTS vb_bounce_visits;
# CREATE TABLE vb_bounce_visits as
# SELECT *
# FROM vb_pages_per_visit
# WHERE dist_visit IN (SELECT dist_visit FROM vb_bounce_visit_ids);
# ")
# 
# top_bounce_pages<- paste0("
# set search_path TO 'central_insights_sandbox';
# INSERT INTO vb_bounce_summary
# with total_visits as (SELECT dt, app_type, count(dist_visit) as total_visits FROM vb_bounce_visits GROUP BY 1,2),
#      page_visits as (
#          SELECT dt, app_type,
#                 page_name_cleaned,
#                 count(distinct dist_visit)                                     as visits,
#                 row_number() over (partition by app_type order by visits DESC) as rank
#          FROM vb_bounce_visits
#          GROUP BY 1, 2,3
#          )
# SELECT a.*, round(100*a.visits::double precision/b.total_visits::double precision,1) as perc
# FROM page_visits a
#          LEFT JOIN total_visits b on a.app_type = b.app_type AND a.dt = b.dt
# WHERE rank <=5;")
# 
# homepage_bounce<-paste0("  
# set search_path TO 'central_insights_sandbox';
# INSERT INTO  vb_homepage_bounce
#     SELECT * FROM vb_bounce_visits WHERE page_name_cleaned IN ('news.page','news.discovery.page');")
# 
# dbSendUpdate(conn, pages_per_visit)
# dbSendUpdate(conn, bounce_ids)
# dbSendUpdate(conn, get_bounce_visits)
# dbSendUpdate(conn, top_bounce_pages)
# dbSendUpdate(conn, homepage_bounce)
# 
# }


homepage_bounce <-
  dbGetQuery(
    conn,
    "SELECT * FROM central_insights_sandbox.vb_bounce_summary 
    WHERE page_name_cleaned IN ('news.page','news.discovery.page')"
  )

homepage_bounce$dt<-ymd(homepage_bounce$dt)
homepage_bounce$visits<-as.numeric(homepage_bounce$visits)
homepage_bounce$app_type<-factor(homepage_bounce$app_type, levels =c('web','app', 'chrysalis'))
homepage_bounce %>% head() 

## data summary
homepage_bounce %>% 
  group_by(app_type) %>% 
  summarise(mean_perc = mean(perc))


war_label <- data.frame(
  label = c("Russia invades Ukraine", "", ""),
  app_type   = factor(c('web','app', 'chrysalis'), levels =c('web','app', 'chrysalis'))
)

### graph for percentage of bounce visits to each platform
ggplot(data = homepage_bounce, aes(x = dt, y = perc/100, colour = app_type) )+
  geom_line()+
  geom_point()+
  scale_x_date(labels = date_format("%Y-%m-%d"),
               breaks = x_axis_dates)+
  scale_y_continuous(breaks = c(0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0),
                     labels = scales::percent,
                     limits = c(0,1)
  )+
  ylab("percentage") +
  labs(title = "Percentage of bounce visits that were on homepage")+
  geom_vline(xintercept = ymd('2022-02-24'), linetype="dashed",
             color = "black")+
  geom_text( data = war_label,
             mapping = aes(x = ymd('2022-02-24'), y = Inf, label = label),
             hjust   = -0.1,
             vjust   = 1.2,
             colour = "black")+
  theme(axis.text.x=element_text(angle=45,hjust=1))

 ## raw number of bounce visits to each platform
ggplot(data = homepage_bounce, aes(x = dt, y = visits, colour = app_type) )+
  geom_line()+
  geom_point()+
  scale_x_date(labels = date_format("%Y-%m-%d"),
               breaks = x_axis_dates)+
  scale_y_continuous(label = comma,
                     n.breaks = 10)+
  ylab("percentage") +
  labs(title = "Number of bounce visits that were on homepage")+
  geom_vline(xintercept = ymd('2022-02-24'), linetype="dashed",
             color = "black")+
  geom_text( data = war_label,
             mapping = aes(x = ymd('2022-02-24'), y = Inf, label = label),
             hjust   = -0.1,
             vjust   = 1.2,
             colour = "black")+
facet_wrap(~app_type , scales = "free_y", nrow = 3)+
  theme(axis.text.x=element_text(angle=45,hjust=1))

### web bounce percentage vs visits
ggplot(data = homepage_bounce %>% 
         filter(app_type == 'web') %>% 
         select(dt, visits, perc) %>% 
         gather(key = measure, value = value, visits:perc), 
       aes(x = dt, y = value, colour = measure))+
  geom_point()+
  geom_line()+
  scale_x_date(labels = date_format("%Y-%m-%d"),
               breaks = x_axis_dates)+
  labs(title = "Number of bounce visits that were on homepage")+
  geom_vline(xintercept = ymd('2022-02-24'), linetype="dashed",
             color = "black")+
  geom_text( data = war_label,
             mapping = aes(x = ymd('2022-02-24'), y = Inf, label = label),
             hjust   = -0.1,
             vjust   = 1.2,
             colour = "black")+
  facet_wrap(~measure , scales = "free_y", nrow = 2)+
  theme(axis.text.x=element_text(angle=45,hjust=1))

homepage_bounce %>% 
  filter(app_type == 'web') %>% 
  head() %>% 
  select(dt, visits, perc) %>% 
  gather(key = measure, value = value, visits:perc)

### total boucnce visits
total_bounces<- homepage_bounce %>% 
  mutate(total_visits = round(100*visits/perc,0))
total_bounces %>% head()

total_bounces$dt<- ymd(total_bounces$dt)
total_bounces$visits<- as.numeric(total_bounces$visits)
total_bounces$app_type<-factor(total_bounces$app_type, levels =c('web','app', 'chrysalis'))


ggplot(data = total_bounces, aes(x= dt, y = total_visits, colour = app_type) )+
  geom_line()+
  geom_point()+
  scale_x_date(labels = date_format("%Y-%m-%d"),
               breaks = x_axis_dates)+
  scale_y_continuous(label = comma,
                     n.breaks = 8)+
  labs(title = "Total number of bounce visits")+
  geom_vline(xintercept = ymd('2022-02-24'), linetype="dashed",
             color = "black")+
  geom_text( data = war_label,
             mapping = aes(x = ymd('2022-02-24'), y = Inf, label = label),
             hjust   = -0.1,
             vjust   = 1.2,
             colour = "black")+
  facet_wrap(~app_type , scales = "free_y", nrow = 3)+
  theme(axis.text.x=element_text(angle=45,hjust=1))


## bounce visits vs total visits
total_bounces %>% head() %>% 
  select(dt, app_type, visits, total_visits) %>% 
  gather(key = measure, value = value , visits:total_visits)

ggplot(data = total_bounces %>%  
         select(dt, app_type, visits, total_visits) %>% 
         rename(homepage_bounce = visits, total_bounces = total_visits
                ) %>% 
         gather(key = measure, value = value , homepage_bounce:total_bounces), 
       aes(x= dt, y = value, colour = measure) )+
  geom_line()+
  geom_point()+
  scale_x_date(labels = date_format("%Y-%m-%d"),
               breaks = x_axis_dates)+
  scale_y_continuous(label = comma,
                     n.breaks = 8)+
  labs(title = "Total number of bounce visits")+
  geom_vline(xintercept = ymd('2022-02-24'), linetype="dashed",
             color = "black")+
  geom_text( data = war_label,
             mapping = aes(x = ymd('2022-02-24'), y = Inf, label = label),
             hjust   = -0.1,
             vjust   = 1.2,
             colour = "black")+
  facet_wrap(~app_type , scales = "free_y", nrow = 3)+
  theme(axis.text.x=element_text(angle=45,hjust=1))+
  scale_color_manual(values=c( "#E69F00", "#56B4E9"))


## summary of bounce visits
total_bounces %>%  #head() %>% 
  select(dt, app_type, visits, total_visits) %>% 
  rename(homepage_bounce = visits, total_bounces = total_visits) %>% 
  group_by(app_type) %>% 
  summarise(mean_homepage_bounce = mean(homepage_bounce),
            median_homepage_bounce = median(homepage_bounce),
            mean_total_bounces = mean(total_bounces),
            median_total_bounces = median(total_bounces),
            ) %>% 
  gather(key = measure, value = value , 2:5) %>% 
  mutate(value = signif(value,digits = 3)) %>% 
  spread(key = app_type, value  = value)


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
  






