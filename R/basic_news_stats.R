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
dbGetQuery(conn,"select distinct brand_title, series_title  from prez.scv_vmb limit 10;")

#### Basic Demographics
data<-dbGetQuery(conn, "SELECT * FROM central_insights_sandbox.vb_news_basics;")
data$users<-as.numeric(data$users)
data$app_type<-factor(data$app_type,levels = c('mobile-chrysalis','app','web') )
data %>% head()



### age_gender
plot_data<-data %>%
  group_by(app_type,age_range, gender) %>%
  summarise(users=sum(users)) %>% 
  filter(age_range !='Unknown')
plot_data

perc_labs<-plot_data %>%ungroup() %>%group_by(app_type,age_range)%>%mutate(perc = round(100*users/sum(users),0))
perc_labs
age_perc<-plot_data %>%ungroup() %>%group_by(app_type,age_range)%>%summarise(users = sum(users)) %>%mutate(perc = round(100*users/sum(users),0))
age_perc

age_perc_labs <- perc_labs %>%
  select(app_type, age_range, gender) %>%
  left_join(age_perc, by = c('age_range', 'app_type')) %>%
  replace(is.na(.), 0) %>%
  left_join(
    plot_data %>%
      summarise(total = sum(users) / 1000000) %>%
      group_by(app_type) %>%
      summarise(max_value = max(total))
  )




ggplot(data = plot_data ,
       aes(x = age_range,y = users/1000000, group = gender, fill = gender)) +
  geom_col(inherit.aes = TRUE,
           position = "stack",
           show.legend = TRUE)+
  #scale_y_continuous(limits = c(0, max_value$total*1.1), breaks = seq(0, max_value$total*1.1, by = 1))+
  xlab("Age Range") +
  ylab("Users (millions)") +
  labs(title = paste0('Age & gender distribution of News users \n(2022-01-15 to 2022-03-31)'))+
  geom_text(aes(label=paste0(perc_labs$perc ,"%")),
            position=position_stack(vjust = 0.5),
            colour="black")+
  geom_label(data = age_perc_labs,
             aes(label=paste0(age_perc_labs$perc ,"%")),
             y = age_perc_labs$max_value,
             colour="black",
             fill = "white")+
  scale_fill_manual(name = "Gender",values=wes_palette(n=3, name="GrandBudapest1"))+
  theme(legend.position = "bottom")+
  facet_wrap(~app_type, scales = "free_y")



