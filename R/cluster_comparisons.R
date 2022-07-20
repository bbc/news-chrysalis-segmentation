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


######### get the original cluster information ########

##set the original cluster names 
cluster_names<- data.frame(
  order= c(0,1,2,3,4,5,6),
  description= c(
    "Evenly spread - higher engagement",
    "Evenly spread - lower engagement",
    "Health and UK",
    "Human interest - social/ents",
    "Human interest - health & education",
    "Politics & reality check",
    "Tech, science & business"
  ),
  cluster =c(6,4,2,0,1,9,5)
)
cluster_names

## bring in data used for initial clusters###
comparison_data <-
  dbGetQuery(
    conn,
    "SELECT cluster,
         CASE
           WHEN age_range IN ('16-24', '25-34') THEN 'under_35'
           WHEN age_range = 'unknown' THEN 'unknown'
           ELSE 'over_35' END as age_range,
  gender, acorn_category, nation, sum(users) as users
FROM central_insights_sandbox.vb_cluster_demo
GROUP BY 1, 2, 3, 4, 5;
    "
  ) %>% replace(is.na(.), 'unknown')
comparison_data %>% head()


############ Functions to collate the data ################
#### bring in new cluster data and aggregate
get_demographic_data <- function(sql_table) {
  comparison_data <<-dbGetQuery(conn,
      paste0(
        "with get_data as (
           SELECT a.*,
           CASE
               WHEN b.age_range in ('16-19', '20-24','25-29', '30-34') THEN 'under_35'
               WHEN b.age_range ISNULL THEN 'unknown'
               ELSE 'over_35' END                                  as age_range,
           CASE
               WHEN gender = 'male' THEN 'male'
               WHEN gender = 'female' THEN 'female'
               ELSE 'unknown' END                              as gender
            ,
           acorn_category || '-' || acorn_category_description as acorn_category,
           nation
          FROM ",sql_table," a
          LEFT JOIN prez.profile_extension b on a.audience_id = b.bbc_hid3
          )
          SELECT segment as cluster, age_range, gender, acorn_category, nation, count(audience_id) as users
          FROM get_data
          GROUP BY 1, 2, 3, 4, 5;"
      )) %>% replace(is.na(.), 'unknown')
    
  print(comparison_data %>% head())
}

#### set cluster number
set_cluster_number<-function(cluster_numbers){
  cluster_names<<- data.frame(
    order= c(0,1,2,3,4,5,6),
    description= c(
      "Evenly spread - higher engagement",
      "Evenly spread - lower engagement",
      "Health and UK",
      "Human interest - social/ents",
      "Human interest - health & education",
      "Politics & reality check",
      "Tech, science & business"
    ),
    cluster = cluster_numbers
  )
  print(cluster_names)
}
#set_cluster_number(cluster_numbers= c(6,4,2,0,1,9,5) )

### get % of users in each cluster
get_cluster_totals <- function(df){
  cluster_totals <-
    cluster_names %>%
    inner_join(
      df %>%
        group_by(cluster) %>%
        summarise(users = sum(users)) %>%
        mutate(perc_of_users = paste0(round(
          100 * users / sum(users), 0
        ), '%')) %>%
        select(-users),
      by  = "cluster"
    )
  
  return(cluster_totals)}

##splits
group_data <- function(df, measure) {
  grouped_data <-
    cluster_names %>%
    inner_join(
      df %>%
        group_by(cluster, !!sym(measure)) %>%
        summarise(users = sum(users)) %>%
        filter(!!sym(measure) != 'unknown') %>%
        ungroup() %>%
        group_by(cluster) %>%
        mutate(perc = paste0(round(
          100 * users / sum(users), 0
        ), '%')) %>%
        select(-users) %>%
        spread(key = !!sym(measure), value = perc),
      by  = "cluster"
    )
  
  return(grouped_data)
}

#### to analyse the initial data #####
set_cluster_number(cluster_numbers= c(6,4,2,0,1,9,5) )
## this is in a different structure to the comparison data
comparison_data <-
  dbGetQuery(
    conn,
    "SELECT cluster,
         CASE
           WHEN age_range IN ('16-24', '25-34') THEN 'under_35'
           WHEN age_range = 'unknown' THEN 'unknown'
           ELSE 'over_35' END as age_range,
  gender, acorn_category, nation, sum(users) as users
FROM central_insights_sandbox.vb_cluster_demo
GROUP BY 1, 2, 3, 4, 5;
    "
  ) %>% replace(is.na(.), 'unknown')
cluster_summary<-
  get_cluster_totals(comparison_data) %>% 
  left_join(group_data(comparison_data, 'gender')) %>% 
  left_join(group_data(comparison_data, 'age_range'))

cluster_summary
write.csv(cluster_summary %>% select(-order), "original_clusters.csv", row.names = FALSE)


######### to analyse data #########
#1. set the cluster numbers as found using the python heatmap
#2. bring in the data
#3. summarise and group

#### for the april 3 months
set_cluster_number(cluster_numbers= c(5,6,1,3,4,2,0) )
get_demographic_data(sql_table = "central_insights_sandbox.taste_segmentation_training_segments_april_3_month")
cluster_summary<-
  get_cluster_totals(comparison_data) %>% 
  left_join(group_data(comparison_data, 'gender')) %>% 
  left_join(group_data(comparison_data, 'age_range'))

cluster_summary
write.csv(cluster_summary %>% select(-order), "clusters_april_3_month.csv", row.names = FALSE)

#### for the feb 1 month
set_cluster_number(cluster_numbers= c(6, 1, 2, 3, 4, 5, 0) )
get_demographic_data(sql_table = "central_insights_sandbox.taste_segmentation_training_segments_feb_1_month")
cluster_summary<-
  get_cluster_totals(comparison_data) %>% 
  left_join(group_data(comparison_data, 'gender')) %>% 
  left_join(group_data(comparison_data, 'age_range'))

cluster_summary
write.csv(cluster_summary %>% select(-order), "clusters_feb_1_month.csv", row.names = FALSE)



