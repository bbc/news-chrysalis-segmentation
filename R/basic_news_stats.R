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
dbGetQuery(conn,"select distinct brand_title, series_title from prez.scv_vmb ORDER BY RANDOM() limit 10;")

######## Basic Demographics ########
data<-dbGetQuery(conn, "SELECT * FROM central_insights_sandbox.vb_news_basics;")
data$users<-as.numeric(data$users)
data$app_type<-factor(data$app_type,levels = c('web','app', 'chrysalis') )
data %>% head()


##### function to make the graphs #####

make_graph <- function(df, #the input data
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
      summarise(users = sum(users)) %>%
      mutate(perc = round(100 * users / sum(users), 0)) %>%
      mutate(dummy = 1) %>%
      left_join(df %>% group_by(!!sym(comparison_measure)) %>% summarise(total_users = signif(sum(users), 3)))
    print(plot_data)
    
    
    graph <- ggplot(data = plot_data ,
                    aes( x = dummy, y = users, group = !!sym(field_to_plot),fill = !!sym(field_to_plot))) +
      geom_col(inherit.aes = TRUE,position = "stack", show.legend = TRUE) +
      ylab("Users") +
      labs(title = graph_title) +
      geom_text(aes(label = paste0(plot_data$perc , "%")),
                position = position_stack(vjust = 0.5),
                colour = "black") +
      geom_label(
        data = plot_data,
        aes(label = paste0(scales::comma(total_users), " users")),
        y = plot_data$total_users*1.025,
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
      facet_wrap(comparison_measure , scales = "free_y")
    

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



