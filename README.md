# Data Processing with Incanter
Basic data processing operations in clojure are presented using incanter.

* row selection
* column selection
* create "views" of your dataset
* apply functions by row
* apply functions by column
* apply functions to groups of rows
* reshape your data (similar to reshape in R) with deshape and shape

## Usage
drop into a repl

`lein repl`

bring in the namespace

`(use 'incanter-demo.core)`

and play around

`(head (to-dataset user-info))`
