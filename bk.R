

ui <- dashboardPage(skin = "black",
                    dashboardHeader(title = "Dynamic sidebar"),
                    
                    
                    
                    dashboardSidebar(
                      
                      sidebarSearchForm(textId = "searchText", buttonId = "searchButton",
                                        label = "Search..."),
                      selectInput("variable", "Variable:",
                                  c("CreditCards" = "CreditCards",
                                    "Loans" = "Loans",
                                    "Gears" = "gear")),
                      
                      sidebarMenu(
                        
                        menuItem("Dashboard", tabName = "dashboard", icon = icon("dashboard")),
                        menuItem("Views", icon = icon("th"), tabName = "widgets",
                                 badgeLabel = "new", badgeColor = "green"),
                        menuItem("Charts", icon = icon("bar-chart"), tabName = "widgets"),
                        menuItem("Tasks", icon = icon("tasks"), tabName = "widgets")
                      ),
                      # Custom CSS to hide the default logout panel
                      tags$head(tags$style(HTML('.shiny-server-account { display: none; }'))),
                      
                      
                      
                      
                      # The dynamically-generated user panel
                      uiOutput("userpanel")
                    ),
                    dashboardBody( 
                      fluidRow(
                        box(
                          title = NULL, footer = NULL, status = NULL,
                          solidHeader = FALSE, background = NULL, width = 12, height = "450px",
                          collapsible = FALSE, collapsed = FALSE),
                        box(
                          title = "First tabBox",
                          # The id lets us use input$tabset1 on the server to find the current tab
                          id = "tabset1",
                          tabPanel("Metrics", 
                                   selectInput("variable", "Product:",
                                               c("Casino" = "Casino",
                                                 "Bingo" = "Bingo",
                                                 "Poker" = "Poker",
                                                 "Sports" = "Sports",
                                                 "Lotto" = "Lotto")),
                                   sliderInput("CF", "Citation Flow:", 
                                               min = 0, max = 100, value = c(25,50)),
                                   sliderInput("TF", "Trust Flow:",
                                               min = 0, max = 100, value = c(25,50)),
                                   sliderInput("DA", "Domain Authority:",
                                               min = 0, max = 100, value = c(30,60))
                          )
                        ),
                        Fluid
                        tabBox(
                          side = "left", height = "300px",
                          selected = "Tab1",
                          tabPanel("Tab1", dateRangeInput("inDateRange", "Date range input:")),
                          checkboxGroupInput("icons", "Choose icons:",
                                             choiceNames =
                                               list(icon("calendar"), icon("bed"),
                                                    icon("cog"), icon("bug")),
                                             choiceValues =
                                               list("calendar", "bed", "cog", "bug")
                          ),
                          
                          tabPanel("Tab2", "Tab content 2"),
                          tabPanel("Tab3", "Note that when side=right, the tab order is reversed.")
                        )
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
        subtitle = a(icon("sign-out"), "Logout", href="__logout__")
      )
    }
  })
}

shinyApp(ui, server)