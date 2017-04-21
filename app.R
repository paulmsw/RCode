
library(shinydashboard)
library(shinysky)
library(shiny)

my_autocomplete_list <- c("smallbusiness.chron.com","stevecollings.co.uk","thecollegeinvestor.com","time.com","timesofindia.indiatimes.com","www.1st-loan.co.uk", "www.abc.net.au",
"www.accountingweb.co.uk", "www.argos.co.uk", "www.automoneytitleloans.com")


ui <- dashboardPage(skin = "black",
                    
                    dashboardHeader(title = "Dynamic sidebar", 
                                    dropdownMenu(type = "messages",
                                                 messageItem(
                                                   from = "Sales Dept",
                                                   message = "Sales are steady this month."
                                                 ),
                                                 messageItem(
                                                   from = "New User",
                                                   message = "How do I register?",
                                                   icon = icon("question"),
                                                   time = "13:45"
                                                 ),
                                                 messageItem(
                                                   from = "Support",
                                                   message = "The new server is ready.",
                                                   icon = icon("life-ring"),
                                                   time = "2014-12-01"
                                                 )
                                    ),
                                    dropdownMenu(type = "notifications",
                                                 notificationItem(
                                                   text = "5 new users today",
                                                   icon("users")
                                                 ),
                                                 notificationItem(
                                                   text = "12 items delivered",
                                                   icon("truck"),
                                                   status = "success"
                                                 ),
                                                 notificationItem(
                                                   text = "Server load at 86%",
                                                   icon = icon("exclamation-triangle"),
                                                   status = "warning"
                                                 )
                                    ),
                                    dropdownMenu(type = "tasks", badgeStatus = "success",
                                                 taskItem(value = 90, color = "green",
                                                          "Documentation"
                                                 ),
                                                 taskItem(value = 17, color = "aqua",
                                                          "Project X"
                                                 ),
                                                 taskItem(value = 75, color = "yellow",
                                                          "Server deployment"
                                                 ),
                                                 taskItem(value = 80, color = "red",
                                                          "Overall project"
                                                 )
                                    )),
                    dashboardSidebar(
          
                      dateRangeInput("daterange3", "Date range:",
                                     start  = "2001-01-01",
                                     end    = "2010-12-31",
                                     min    = "2001-01-01",
                                     max    = "2012-12-21",
                                     format = "mm/dd/yy",
                                     separator = " - "),
                      selectInput("variable", "Variable:",
                                  c("Cylinders" = "cyl",
                                    "Transmission" = "am",
                                    "Gears" = "gear")),
                      
                      
                      
                      
                      
                      sidebarMenu(
                        
                        menuItem("Dashboard", tabName = "dashboard", icon = icon("dashboard")),
                        menuItem("Views", icon = icon("th"), tabName = "widgets",
                                 badgeLabel = "new", badgeColor = "green"),
                        menuItem("Charts", icon = icon("bar-chart"), tabName = "widgets",
                                 badgeLabel = "new", badgeColor = "green"),
                        menuItem("Tasks", icon = icon("tasks"), tabName = "widgets",
                                 badgeLabel = "new", badgeColor = "green")
                      ),
                      # Custom CSS to hide the default logout panel
                      tags$head(tags$style(HTML('.shiny-server-account { display: none; }'))),
                      
                      
                      
                      
                      # The dynamically-generated user panel
                      uiOutput("userpanel")
                    ),
                    dashboardBody(

            
                        box(plotOutput("plot1")),
                        
                        box("Box content here", br(), "More box content",
                          sliderInput("slider", "Slider input:", 1, 100, 50),
                          textInput("text", "Text input:")),
                        
                      
                      box(width = 6, status = "info", title = "Search domains...",
                          textInput.typeahead(id="search",
                                              placeholder="www.example.com",
                                              local=data.frame(name=c(my_autocomplete_list)),
                                              valueKey = "name",
                                              tokens=c(1:length(my_autocomplete_list)),
                                              template = HTML("<p class='repo-language'>{{info}}</p> <p class='repo-name'>{{name}}</p>"))
                          ),
                      box(width = 6, status = "primary", title = "test"),
                      
                      
                      box(width = 6, status = "info", title = 
                            fluidRow(
                              tags$style(".box-title {width: 100%;}"),
                              column(6, "Trend - Usage of space",br(),
                                     div(downloadLink("downloadspacetrend", "Download this chart"), style = "font-size:70%", align = "left")),
                              column(6,
                                     div(checkboxInput(inputId = "spacetrendcheck", "Add to reports", value = FALSE),style = "font-size:70%",float = "right", align = "right", direction = "rtl"))
                            ),
                          div(plotOutput("spacetrend"), width = "100%", height = "400px", style = "font-size:90%;"),
                          uiOutput("spacetrendcomment")
                      ),
                      box(width = 6, status = "info", title = "test",
                          fluidRow(
                            tags$style(type="text/css",".shiny-output-error { visibility: hidden; }",".shiny-output-error:before { visibility: hidden; }"),
                            tags$style(type="text/css","#search { top: 50% !important;left: 50% !important;margin-top: -100px !important;margin-left: -250px 
                       !important; color: blue;font-size: 20px;font-style: italic;}"
                                       
                                       
                                       )

                            
                            
                          
                          )
                      ),
                    
                      box(
                        title = "First tabBox", width = 6, solidHeader = TRUE,  collapsible = TRUE,
                        # The id lets us use input$tabset1 on the server to find the current tab
                        
                        sliderInput("x", "Range:",
                                    min = 1, max = 100, value = c(0,100)),
                        sliderInput("y", "Range:",
                                    min = 1, max = 100, value = c(0,100))
                      ),
                      
                      box(
                        title = "First tabBox", width = 6, solidHeader = TRUE, collapsible = TRUE,
                        # The id lets us use input$tabset1 on the server to find the current tab
                        
                        sliderInput("x", "Range:",
                                    min = 1, max = 100, value = c(0,100)),
                        sliderInput("y", "Range:",
                                    min = 1, max = 100, value = c(0,100))
                      )
                      
                    )
)


server <- function(input, output, session) {
  
  
  output$userpanel <- renderUI({
    # session$user is non-NULL only in authenticated sessions
    if (!is.null(session$user)) {
      sidebarUserPanel(
        menuItem("Menu item", icon = icon("calendar")),
        span("Logged in as ", session$user),
        subtitle = a(icon("sign-out"), "Logout", href="__logout__"))
    }
  
    
    
    })
  
  
}

shinyApp(ui, server)

