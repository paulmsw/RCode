# server.R

shinyServer(function(input, output) {
  
  output$text1 <- renderText({ 
   paste( "You have selected this", input$var, sep = " ")
  })
  
}
)
