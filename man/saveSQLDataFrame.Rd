% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/saveSQLDataFrame.R
\name{saveSQLDataFrame}
\alias{saveSQLDataFrame}
\title{Save SQLDataFrame object as a new database table.}
\usage{
saveSQLDataFrame(
  x,
  dbname = tempfile(fileext = ".db"),
  dbtable = deparse(substitute(x)),
  localConn = dbcon(x),
  overwrite = FALSE,
  index = TRUE,
  ...
)
}
\arguments{
\item{x}{The \code{SQLDataFrame} object to be saved.}

\item{dbname}{A character string of the file path of to be saved
SQLite database file. Will only be used when the input
SQLDataFrame represents a SQLite database table. Default to
save in a temporary file.}

\item{dbtable}{A character string for the to be saved database
table name. Default is the name of the input
\code{SQLDataFrame}.}

\item{localConn}{A MySQL connection with write permission. Will be
used only when the input SQLDataFrame objects connects to MySQL
connections without write permission. A new MySQL table will be
written in the the database this argument provides.}

\item{overwrite}{Whether to overwrite the \code{dbtable} if already
exists. Only applies to the saving of SQLDataFrame objects
representing SQLite database tables. Default is FALSE.}

\item{index}{Whether to create the database index. Default is TRUE.}

\item{...}{other parameters passed to methods.}
}
\value{
A \code{SQLDataFrame} object.
}
\description{
The function to save \code{SQLDataFrame} object as a
    database table with a supplied path to database. It also
    returns a \code{SQLDataFrame} object constructed from the
    user-supplied \code{dbname}, \code{dbtable}, and \code{dbkey}.
}
\details{
For SQLDataFrame from \code{union} or \code{rbind}, if
    representation of MySQL tables, the data will be sorted by key
    columns, and saved as MySQL table in the connection with write
    permission.
}
\examples{
test.db <- system.file("extdata/test.db", package = "SQLDataFrame")
conn <- DBI::dbConnect(DBI::dbDriver("SQLite"), dbname = test.db)
obj <- SQLDataFrame(conn = conn, dbtable = "state", dbkey = "state")
obj1 <- obj[1:10, 2:3]
obj1 <- saveSQLDataFrame(obj1, dbtable = "obj_subset")
dbcon(obj1)
dbtable(obj1)
}
