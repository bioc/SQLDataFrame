#' Save SQLDataFrame object as a new database table.
#' @rdname saveSQLDataFrame
#' @description The function to save \code{SQLDataFrame} object as a
#'     database table with a supplied path to database. It also
#'     returns a \code{SQLDataFrame} object constructed from the
#'     user-supplied \code{dbname}, \code{dbtable}, and \code{dbkey}.
#' @param x The \code{SQLDataFrame} object to be saved.
#' @param localConn A MySQL connection with write permission. Will be
#'     used only when the input SQLDataFrame objects connects to MySQL
#'     connections without write permission. A new MySQL table will be
#'     written in the the database this argument provides.
#' @param dbname A character string of the file path of to be saved
#'     SQLite database file. Will only be used when the input
#'     SQLDataFrame represents a SQLite database table. Default to
#'     save in a temporary file.
#' @param dbtable A character string for the to be saved database
#'     table name. Default is the name of the input
#'     \code{SQLDataFrame}.
#' @param overwrite Whether to overwrite the \code{dbtable} if already
#'     exists. Only applies to the saving of SQLDataFrame objects
#'     representing SQLite database tables. Default is FALSE.
#' @param index Whether to create the database index. Default is TRUE.
#' @param ... other parameters passed to methods.
#' @details For SQLDataFrame from \code{union} or \code{rbind}, if
#'     representation of MySQL tables, the data will be sorted by key
#'     columns, and saved as MySQL table in the connection with write
#'     permission.
#' @return A \code{SQLDataFrame} object.
#' @import DBI
#' @import dbplyr
#' @rawNamespace import(dplyr, except = c("first", "rename",
#'     "setequal", "setdiff", "intersect", "union", "ident", "sql"))
#' @examples
#' test.db <- system.file("extdata/test.db", package = "SQLDataFrame")
#' conn <- DBI::dbConnect(DBI::dbDriver("SQLite"), dbname = test.db)
#' obj <- SQLDataFrame(conn = conn, dbtable = "state", dbkey = "state")
#' obj1 <- obj[1:10, 2:3]
#' obj1 <- saveSQLDataFrame(obj1, dbtable = "obj_subset")
#' dbcon(obj1)
#' dbtable(obj1)
#' @export


saveSQLDataFrame <- function(x,
                             dbname = tempfile(fileext = ".db"),  ## only used for SQLiteConnection
                             dbtable = deparse(substitute(x)), 
                             localConn = dbcon(x),  ## only used for MySQLConnection
                             overwrite = FALSE,  ## only used for SQLiteConnection
                             index = TRUE, ...)
{
    if (is(dbcon(x), "MySQLConnection")) {
        con <- dbcon(x)
        if (identical(con, localConn)) {
            if (!.mysql_has_write_perm(con))
                stop("Please provide a MySQL connection ",
                     "with write permission in argument: localConn")
            tbl <- .extract_tbl_from_SQLDataFrame_indexes(tblData(x), x)
            sql_cmd <- build_sql("CREATE TABLE ", sql(dbtable), " AS ",
                                 db_sql_render(con, tbl), con = con) 
            dbExecute(con, sql_cmd)
        } else {
            if (is.null(remote_name(tblData(u1))))
                ## means non-"lazy_base_remote_query", from
                ## join/union/rbind, etc.
                stop("Saving SQLDataFrame with lazy join / union queries ",
                     "from same non-writable MySQL database is not supported!")
            if (!.mysql_has_write_perm(localConn))
                stop("Please provide a MySQL connection ",
                     "with write permission in argument: localConn")
            fedtable <- dplyr:::random_table_name()
            ## a temporary federated table will be generated, and then
            ## removed if "create table" is successful. 
            tbl <- .createFedTable_and_reopen_tbl(x,
                                                  localConn,
                                                  fedtable,
                                                  remotePswd = .get_mysql_var(con))
            con <- localConn
            sql_cmd <- build_sql("CREATE TABLE ", sql(dbtable)," AS ",
                                 db_sql_render(con, tbl), con = con) 
            trycreate <- try(dbExecute(con, sql_cmd))
            if (!is(trycreate, "try-error")) {
                sql_drop <- build_sql("DROP TABLE ", sql(fedtable), con = con)
                dbExecute(con, sql_drop)
            }
        }
    } else if(is(dbcon(x), "SQLiteConnection")) { 
        if (file.exists(dbname)) {
            dbname <- file_path_as_absolute(dbname)
            if (overwrite == FALSE)
                stop("The 'dbname' already exists! Please provide a new value ",
                     "OR change 'overwrite = TRUE'. ")
        }
        con <- dbcon(x)
        tbl <- .extract_tbl_from_SQLDataFrame_indexes(tblData(x), x)
        }
        sql_cmd <- build_sql("CREATE TABLE ", sql(dbtable), " AS ",
                             db_sql_render(con, tbl), con = con)
        dbExecute(con, sql_cmd)
    if (index)
        dbplyr:::db_create_index.DBIConnection(con, dbtable,
                                               columns = dbkey(x),
                                               unique = TRUE)
    ## FIXME: implement "overwrite" argument here for the index
    ## doesn't work.
    ## https://www.w3schools.com/sql/sql_create_index.asp DROP INDEX
    ## table_name.index_name; see also: dbRemoveTable()
    
    ## This chunk applies to an input SQLDataFrame from "rbind"
    file.copy(dbcon(x)@dbname, dbname, overwrite = overwrite)
    sql_drop <- build_sql("DROP TABLE ", sql(dbtable), con = con)
    dbExecute(con, sql_drop)
    con <- DBI::dbConnect(dbDriver("SQLite"), dbname = dbname)
    if (!is.null(ridx(x))) {
        dbWriteTable(con, paste0(dbtable, "_ridx"),
                     value = data.frame(ridx = ridx(x)),
                     overwrite = overwrite)
    }
    .msg_saveSQLDataFrame(x, con, dbtable)
    suppressMessages(res <- SQLDataFrame(conn = con, dbtable = dbtable, dbkey = dbkey(x)))
    invisible(res)
}

.write_ridx_sqlite <- function(x, dbtable, con) {
}

.msg_saveSQLDataFrame <- function(x, con, dbtable)
{
    type <- class(con)
    switch(type,
           MySQLConnection = .msg_save_mysql(x, con, dbtable),
           SQLiteConnection = .msg_save_sqlite(x, con, dbtable)
           ) 
}

.msg_save_mysql <- function(x, con, dbtable) {
    info <- dbGetInfo(con)
    databaseLine <- paste0("mysql ", info$serverVersion, " [",
                           .mysql_info(con),
                           ":/", info$dbname, "] \n")  ## legacy format from lazy_tbl
    msg <- paste0("## A new database table is saved! \n",
                  "## Source: table<", dbtable, "> [",
                  paste(dim(x), collapse = " X "), "] \n",
                  "## Database: ", databaseLine, 
                  "## Use the following command to reload into R: \n",
                  "## sdf <- SQLDataFrame(\n",
                  "##   host = '", info$host, "',\n",
                  "##   user = '", info$user, "',\n",
                  "##   password = '',", "   ## Only if required!", "\n",
                  "##   type = 'MySQL',\n",
                  "##   dbname = '", info$dbname, "',\n",
                  "##   dbtable = '", dbtable, "',\n",
                  "##   dbkey = ", ifelse(length(dbkey(x)) == 1, "", "c("),
                  paste(paste0("'", dbkey(x), "'"), collapse=", "),
                  ifelse(length(dbkey(x)) == 1, "", ")"), ")", "\n")
    message(msg)
}

.msg_save_sqlite <- function(x, con, dbtable) {
    databaseLine <- paste0("sqlite ", dbplyr:::sqlite_version(),
                           " [", con@dbname, "] \n")
    msg <- paste0("## A new database table is saved! \n",
                  "## Source: table<", dbtable, "> [",
                  paste(dim(x), collapse = " X "), "] \n",
                  "## Database: ", databaseLine, 
                  "## Use the following command to reload into R: \n",
                  "## sdf <- SQLDataFrame(\n",
                  "##   dbname = '", con@dbname, "',\n",
                  "##   type = 'SQLite',\n",
                  "##   dbtable = '", dbtable, "',\n",
                  "##   dbkey = ", ifelse(length(dbkey(x)) == 1, "", "c("),
                  paste(paste0("'", dbkey(x), "'"), collapse=", "),
                  ifelse(length(dbkey(x)) == 1, "", ")"), ")", "\n")
    message(msg)
}

