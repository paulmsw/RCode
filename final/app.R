
library(shinysky)
library(shiny)




setwd("~/Data/RandD/AppData/")
smon <- readRDS("smon.rds")

domainlevels <- levels(smon$Domain)

my_autocomplete_list <- levels(smon$Domain)


ui <- shinyUI(
  fluidPage(
    tags$style(type="text/css",".shiny-output-error { visibility: hidden; }",".shiny-output-error:before { visibility: hidden; }"),
            tags$style(type="text/css","#search { top: 50% !important;left: 50% !important;margin-top: -100px !important;margin-left: -250px 
                       !important; color: blue;font-size: 20px;font-style: italic;}"
                       
                       
                       
                       ),         
            
            mainPanel(  
              # one way of doing it
              textInput.typeahead(id="search",
                                  placeholder="Type your name please",
                                  local=data.frame(name=c(my_autocomplete_list)),
                                  valueKey = "name",
                                  tokens=c(1:length(my_autocomplete_list)),
                                  template = HTML("<p class='repo-language'>{{info}}</p> <p class='repo-name'>{{name}}</p>")
              )
        )
  )
)



server <- function(input, output, session) {
  
  var <- reactive({
    if(input$go==0){return()}
    isolate({
      input$go
      cbind("a","c")
    })
  })  
  output$text <- renderText({var()})
  
  domain <- reactive({
    
  })
  
}

shinyApp(ui = ui, server = server)



