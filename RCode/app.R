library(shinythemes)
# A simple Shiny demo written by Dieter Menne

options(rgl.useNULL = TRUE)
library(shiny)
library(rgl)

app = shinyApp(
  ui = bootstrapPage(
    checkboxInput("rescale", "Rescale"),
    rglwidgetOutput("rglPlot")
  ),
  server = function(input, output) {
    output$rglPlot <- renderRglwidget({
    
      rglwidget()
    })
  })
