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

######## Basic Demographics ########
data<-dbGetQuery(conn, "SELECT * FROM central_insights_sandbox.vb_news_basics;")
data$users<-as.numeric(data$users)
data$app_type<-factor(data$app_type,levels = c('web','app', 'chrysalis') )
data %>% head()

# Example data structure
# app_type    gender age_range                  acorn_cat  users
#        web unknown     45-54   04_Financially Stretched  25274
#        web  female     45-54       02_Rising Prosperity 100060
#        web  female     16-24         05_Urban Adversity  33170
#  chrysalis  female     45-54 03_Comfortable Communities    342
#  chrysalis  female       55+       02_Rising Prosperity    428
#        app    male     45-54                    unknown   3564

##### function to make the graphs #####

make_graph <- function(df, #the input data
                       plotted_measure = 'users',
                       comparison_measure, #the value to facet wrap (create comparable bars) by e.g app type
                       field_to_plot, # the data field you wish to split by (str)
                       graph_title = NULL, # the title (str)
                       palette_family = 0, ##e.g "brewer","wes_anderson"- these two families can be used or leave blank for a default
                       colour_palette = NULL ,  # the palette name e.g "Set1" (str) or "GrandBudapest1"
                       n_colours = NULL ##the number of colours required 
                       ) {
  grouping_fields<- comparison_measure %>% append(field_to_plot)

  plot_data <-
    df %>%
      group_by_at({{grouping_fields}}) %>%
      summarise(measure = sum(!!sym(plotted_measure))) %>%
      mutate(perc = round(100 * measure / sum(measure), 0)) %>%
      mutate(dummy = 1) %>%
      left_join(df %>% group_by(!!sym(comparison_measure)) %>% summarise(total = signif(sum(!!sym(plotted_measure)), 3)))
    print(plot_data)
    
    
    
    graph <- ggplot(data = plot_data ,
                    aes( x = dummy, y = measure, group = !!sym(field_to_plot),fill = !!sym(field_to_plot))) +
      geom_col(inherit.aes = TRUE,position = "stack", show.legend = TRUE) +
      ylab("Users") +
      labs(title = graph_title) +
      geom_text(aes(label = paste0(plot_data$perc , "%")),
                position = position_stack(vjust = 0.5),
                colour = "black") +
      geom_label(
        data = plot_data,
        aes(label = paste0(scales::comma(total), " ",plotted_measure)),
        y = plot_data$total*1.025,
        colour = "black",
        fill = "white") +
      {if(palette_family == 'brewer')scale_fill_manual(name = field_to_plot,
                        values = brewer.pal(n_colours, name = colour_palette)
                        )} +
      {if(palette_family == 'wes_anderson')scale_fill_manual(name = field_to_plot,
                        values = wes_palette(n_colours, name = colour_palette)
      )} +
      scale_y_continuous(label = comma,
                         n.breaks = 6
                         ) +
      theme(legend.position = "bottom",
        axis.title.x = element_blank(),
        axis.text.x = element_blank(),
        axis.ticks.x = element_blank()) +
      facet_wrap(comparison_measure , scales = "free_y", nrow = 1)
    

    return(graph)
  }
age_graph <- make_graph(
  df = data,
  comparison_measure = 'app_type',
  field_to_plot = "age_range",
  graph_title = "Age split for BBC News (2022-01-15 to 2022-03-31)",
  colour_palette = "Darjeeling1",
  palette_family = 'wes_anderson',
  n_colours = 5
)
age_graph

gender_graph <- make_graph(
  df = data,
  comparison_measure = "app_type",
  field_to_plot = "gender",
  graph_title = "Gender split for BBC News (2022-01-15 to 2022-03-31)",
  colour_palette = "GrandBudapest1",
  palette_family = 'wes_anderson',
  n_colours = 3
)
gender_graph

acorn_graph <- make_graph(
  df = data,
  comparison_measure = "app_type",
  field_to_plot = "acorn_cat",
  graph_title = "Acorn split for BBC News (2022-01-15 to 2022-03-31)",
  colour_palette = "Set1",
  palette_family = 'brewer',
  n_colours = 7
)
acorn_graph


######## Daily traffic ########
data<-dbGetQuery(conn, "SELECT * FROM central_insights_sandbox.vb_news_daily;")
data$users<-as.numeric(data$users)
data$visits<-as.numeric(data$visits)
data<-data %>% rename(dt = date_of_event)
data$app_type<-factor(data$app_type,levels = c('web','app', 'chrysalis') )
data %>% head()


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
war_label <- data.frame(
  label = c("Russia invades Ukraine", "", ""),
  app_type   = factor(c('web','app', 'chrysalis'), levels =c('web','app', 'chrysalis'))
)


### users and visits
plot_data <- data %>% 
  group_by(dt,app_type) %>% 
  summarise(users = sum(users), visits = sum(visits)) %>% 
  gather(key = measure, value = value, users:visits) %>% 
  group_by(app_type, measure) %>% 
  mutate(mean = signif(mean(value),2))
plot_data$app_type<-factor(plot_data$app_type,levels = c('web','app', 'chrysalis') )
plot_data 

### users and visits
ggplot(data= plot_data, aes(x = dt, colour = measure) )+
  geom_line(aes(y = value))+
  geom_line(aes(y = mean),linetype="dotted")+
  ylab("Users")+
  xlab("Date")+
  labs(title = "User and Visits per day to BBC News (2022-01-15 to 2022-03-31)") +
  scale_y_continuous(label = comma,
                     n.breaks = 6) +
  scale_x_date(labels = date_format("%Y-%m-%d"),
               breaks = x_axis_dates)+
  geom_vline(xintercept = ymd('2022-02-24'), linetype="dashed",
             color = "black")+
  geom_text( data = war_label,
             mapping = aes(x = ymd('2022-02-24'), y = Inf, label = label),
            hjust   = -0.1,
             vjust   = 1.2,
            colour = "black") +
  geom_text( data = plot_data,
             mapping = aes(x = ymd('2022-03-31'), y = plot_data$mean, label = comma(plot_data$mean)),
             #hjust   = -0.1,
             vjust   = -1,
             colour = "black") +
  theme(axis.text.x=element_text(angle=60, hjust=1))+
  facet_wrap(~app_type, scales = "free_y", ncol = 1)


########## old vs new app ##########

old_vs_new<-data %>% 
  group_by(dt, app_type) %>% 
  filter(app_type != 'web') %>% 
  summarise(measure = sum(visits)) %>%
  mutate(perc = round(100*measure/sum(measure),2)) %>% 
  select(-measure) %>% 
  spread(key = app_type, value = perc)

old_vs_new %>% head()
old_vs_new$chrysalis %>% max()
old_vs_new$chrysalis %>% min()
old_vs_new$chrysalis %>% sd()
old_vs_new$chrysalis %>% mean()
old_vs_new$chrysalis %>% median()

ggplot(data = data %>% 
         group_by(dt, app_type) %>% 
         filter(app_type != 'web') %>% 
         summarise(measure = sum(visits)) %>%
         mutate(perc = round(100*measure/sum(measure),2)), 
       aes(x = dt, y = perc, fill = app_type))+
  geom_col(inherit.aes = TRUE,position = "stack", show.legend = TRUE) +
  scale_y_continuous(limits = c(0,1.5))
  


##### Daily traffic split by demographics #####

make_line_graph <- function(df, #the input data
                            plotted_measure =c("users","visits") , ## the value to be plotted on the y axis
                            comparison_measure, #the value to facet wrap (create comparable bars) by e.g app type
                            field_to_split_by,# the data field you wish to split by (str)
                            graph_title = NULL,# the title (str)
                            palette_family = 0, ##e.g "brewer","wes_anderson"- these two families can be used or leave blank for a default
                            colour_palette = NULL , # the palette name e.g "Set1" (str) or "GrandBudapest1"
                            n_colours = NULL ##the number of colours required 
                            ) {
                            
  grouping_fields<- comparison_measure %>% append(field_to_split_by)


  plot_data <- 
    df %>%  
    group_by_at({{c("dt") %>% append(grouping_fields)}}) %>% 
    summarise(measure = sum(!!sym(plotted_measure))) %>% 
    group_by_at({{grouping_fields}}) %>% 
    mutate(mean = signif(mean(measure),2))
  
  perc<-
    plot_data %>% 
    ungroup() %>% 
    select({{grouping_fields}}, mean) %>% 
    unique() %>% 
    group_by_at({{comparison_measure}}) %>% 
    mutate(mean_perc =paste0(round(100*mean/sum(mean),0),"%") )
  
  plot_data<- plot_data %>% left_join(perc, by = c({{grouping_fields}}, "mean"))

  if(comparison_measure =='app_type'){
    plot_data$app_type<-factor(plot_data$app_type,levels = c('web','app', 'chrysalis') )
    }
  print(plot_data %>% head())
  plot_data<<-plot_data

ggplot(data= plot_data, aes(x = dt, colour = !!sym(field_to_split_by)) )+
  geom_line(aes(y = measure))+
  geom_line(aes(y = mean),linetype="dotted")+
  ylab(plotted_measure)+
  xlab("Date")+
  labs(title = graph_title) +
  scale_y_continuous(label = comma,
                     n.breaks = 6) +
  scale_x_date(labels = date_format("%Y-%m-%d"),
               breaks = x_axis_dates,
               limits = c(data$dt %>% min(), data$dt %>% max()+6)
               )+
  geom_vline(xintercept = ymd('2022-02-24'), linetype="dashed",
             color = "black")+
  geom_text( data = war_label,
             mapping = aes(x = ymd('2022-02-24'), y = Inf, label = label),
             hjust   = -0.1,
             vjust   = 1.2,
             colour = "black") +
  geom_text_repel(data = plot_data %>% filter(dt ==  ymd('2022-03-31')),
             mapping = aes(x = ymd('2022-03-31'),
                           y = plot_data$mean[plot_data$dt == ymd('2022-03-31')], 
                           label = paste0(comma(plot_data$mean[plot_data$dt == ymd('2022-03-31')])," (", plot_data$mean_perc[plot_data$dt == ymd('2022-03-31')])
                                          ),
             hjust = "right",
             colour = "black") +
  {if(palette_family == 'brewer')scale_colour_manual(name = field_to_split_by,
                                                   values = brewer.pal(n_colours, name = colour_palette)
  )} +
  {if(palette_family == 'wes_anderson')scale_colour_manual(name = field_to_split_by,
                                                         values = wes_palette(n_colours, name = colour_palette)
  )} +
  theme(axis.text.x=element_text(angle=60, hjust=1))+
  facet_wrap(~app_type, scales = "free_y", ncol = 1)

}

age_users<-
  make_line_graph(  df = data,
                    plotted_measure = "users",
                    comparison_measure = "app_type",
                    field_to_split_by = "age_range",
                    graph_title = "Users split by age range for BBC News (2022-01-15 to 2022-03-31)",
                    colour_palette = "Darjeeling1",
                    palette_family = 'wes_anderson',
                    n_colours = 5
  )
age_users

gender_users<-
  make_line_graph(  df = data,
                  plotted_measure = "users",
                  comparison_measure = "app_type",
                  field_to_split_by = "gender",
                  graph_title = "Users split by gender for BBC News (2022-01-15 to 2022-03-31)",
                  colour_palette = "GrandBudapest1",
                  palette_family = 'wes_anderson',
                  n_colours = 3
)
gender_users


acorn_users<-
  make_line_graph(  df = data,
                    plotted_measure = "users",
                    comparison_measure = "app_type",
                    field_to_split_by = "acorn_cat",
                    graph_title = "Users split by acorn category for BBC News (2022-01-15 to 2022-03-31)",
                    colour_palette = "Set1",
                    palette_family = 'brewer',
                    n_colours = 7
  )
acorn_users

####### pages ######
field<-"acorn_cat"
data <-
  dbGetQuery(
    conn,paste0(
    "SELECT ",field,", 
    CASE WHEN pages < 5 then pages::varchar ELSE '5+' END as pages,
    sum(visits) as visits 
    FROM central_insights_sandbox.vb_news_pages_summary 
    WHERE pages !=0
    GROUP BY 1,2;")
  )
data$visits <- as.numeric(data$visits )
data$app_type<-factor(data$app_type,levels = c('web','app', 'chrysalis') )
data$pages<-factor(data$pages,levels = c('5+','4','3','2','1') )
data

make_graph(
  df = data,
  plotted_measure = 'visits',
  comparison_measure = field,
  field_to_plot = "pages",
  graph_title = "Visits containing X number of pages on BBC News (2022-01-15 to 2022-03-31)",
  #colour_palette = "Darjeeling1",
  #palette_family = 'wes_anderson',
  #n_colours = 5
)



######### Frequency segments ###########

## for all users listed
data <- dbGetQuery(
  conn,
"SELECT date_of_segmentation as dt,
segvalue,
frequency_group as freq_group,
count as users
FROM central_insights_sandbox.vb_news_freq_seg
ORDER BY segvalue;"
)


data %>% head()
data %>% summary()
data$dt <- ymd(data$dt)
data$users<- as.numeric(data$users)
data$freq_group<-factor(data$freq_group,
                        levels = c(
                          '13 weeks+',
                          'less than monthly',
                          'monthly',
                          'fortnightly',
                          'weekly',
                          'daily'
                          ))

data$segvalue <- factor(
  data$segvalue,
  levels = c(
    'I. dormant',
    'H. last seen 26-52 weeks ago',
    'G. last seen 13-26 weeks ago',
    'F. less than monthly',
    'E. monthly',
    'D. fortnightly',
    'C. 1-2 days per week',
    'B. 2-5 days per week',
    'A. daily (5+ days per week)'
  )
)

data %>% 
  arrange(dt, segvalue) %>% 
  group_by(segvalue) %>% 
  summarise(avg_users = mean(users)) %>% 
  mutate(perc = avg_users/sum(avg_users),
         dummy = 1)


# ggplot(data = data %>%
#          arrange(dt, segvalue) %>%
#          group_by(segvalue) %>%
#          summarise(avg_users = mean(users)) %>%
#          mutate(perc = round(100*avg_users/sum(avg_users),0),
#                 dummy = 1),
#        aes(x = dummy, y = avg_users/1000000, fill = segvalue)
#        )+
#   geom_col(inherit.aes = TRUE,position = "stack", show.legend = TRUE)+
#   ylab("Average users (million)") +
#   labs(fill="Segment")+
#   labs(title = "BBC News accounts in each frequency group averaged over 11 weeks \n (w/c 2022-01-17 to 2022-03-28) ") +
#   geom_text(aes(label = paste0(comma(signif(avg_users/1000000,2))," mil (", perc , "%)")),
#             position = position_stack(vjust = 0.5),
#             colour = "black")+
#   scale_y_continuous(label = comma,
#                      n.breaks = 20
#   ) +
#   scale_fill_brewer(palette="Blues")+
#   theme(
#         axis.title.x = element_blank(),
#         axis.text.x = element_blank(),
#         axis.ticks.x = element_blank())+
# guides(fill = guide_legend(reverse=T))

ggplot(data = data %>%
         arrange(dt, freq_group) %>%
         group_by(dt,freq_group) %>%
         summarise(users = sum(users)) %>%
         group_by(freq_group) %>%
         summarise(avg_users = mean(users)) %>%
         mutate(perc = round(100*avg_users/sum(avg_users),0),
                dummy = 1),
       aes(x = dummy, y = avg_users/1000000, fill = freq_group)
)+
  geom_col(inherit.aes = TRUE,position = "stack", show.legend = TRUE)+
  ylab("Average users (million)") +
  labs(fill="Frequency Group")+
  labs(title = "BBC News accounts in each frequency group averaged over 11 weeks \n (w/c 2022-01-17 to 2022-03-28) ") +
  geom_text(aes(label = paste0(comma(signif(avg_users/1000000,2))," mil (", perc , "%)")),
            position = position_stack(vjust = 0.5),
            colour = "black")+
  scale_y_continuous(label = comma,
                     n.breaks = 20
  ) +
  scale_fill_brewer(palette="Reds")+
  theme(
    axis.title.x = element_blank(),
    axis.text.x = element_blank(),
    axis.ticks.x = element_blank())+
  guides(fill = guide_legend(reverse=T))


####### for only active users
data <- dbGetQuery(
  conn,
  "SELECT app_type,
  week_commencing,
       freq_group,
       count(distinct audience_id) as users
FROM central_insights_sandbox.vb_news_segs
GROUP BY 1,2,3
ORDER BY 1 ,2;"
)


data %>% head()
data %>% summary()
data$users<- as.numeric(data$users)
data$freq_group<-factor(data$freq_group,
                        levels = c("new",
                          '13 weeks+',
                          'less than monthly',
                          'monthly',
                          'fortnightly',
                          'weekly',
                          'daily'
                        ))
data$app_type<-factor(data$app_type,levels = c('web','app', 'chrysalis') )


plot_data <- data %>%
  arrange(app_type, week_commencing, freq_group) %>%
  group_by(app_type, freq_group) %>%
  summarise(avg_users = mean(users)) %>%
  mutate(perc = round(100 * avg_users / sum(avg_users), 1),
         dummy = 1)

### Graph to show the % of users (averaged weekly) that are in each frequency group

ggplot(data = plot_data,
       aes(x = dummy, y = signif(avg_users,2), fill = app_type, 
           
           )
)+
  geom_col( inherit.aes = TRUE,
            aes(alpha = freq_group),
           position = "stack", 
           show.legend = TRUE)+
  ylab("Average users") +
  labs(alpha="Frequency Group")+
  labs(title = "Frequency of BBC News (signed in) users averaged over 11 weeks \n (w/c 2022-01-17 to 2022-03-28) ") +
  geom_text(aes(#alpha  = NULL,
                #fill= freq_group,
    group = freq_group,
                label = ifelse(plot_data$perc>1, paste0(comma(signif(plot_data$avg_users,2)) ," (", 
                                      plot_data$perc,
                                      "%)"),""
                )
                ),
            position = position_stack(vjust = 0.5),
            colour = "black"
            )+
  scale_y_continuous(label = comma,
                     n.breaks = 20
  ) +
  #scale_fill_brewer(palette="rgb")+
  theme(
    axis.title.x = element_blank(),
    axis.text.x = element_blank(),
    axis.ticks.x = element_blank())+
  guides(fill = "none",
         alpha= guide_legend(reverse=T)
         )+
  facet_wrap(~app_type,scales = "free_y" )



plot_data$avg_users[plot_data$perc>1]

if(plot_data$perc > 5.0) {
  paste0(comma(signif(plot_data$avg_users, 2)) , " (",
         plot_data$perc,
         "%)")
}else{
  ""
}






