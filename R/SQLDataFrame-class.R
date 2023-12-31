#' SQLDataFrame class
#' @name SQLDataFrame
#' @aliases class:SQLDataFrame SQLDataFrame-class
#' @rdname SQLDataFrame-class
#' @description NULL
#' @exportClass SQLDataFrame
#' @importFrom methods setOldClass new
## add other connections. 
setOldClass(c("tbl_MySQLConnection", "tbl_SQLiteConnection",
              "tbl_BigQueryConnection",
              "tbl_dbi", "tbl_sql", "tbl_lazy", "tbl"))
.SQLDataFrame <- setClass(
    "SQLDataFrame",
    contains = "DataFrame",
    slots = c(
        dbkey = "character",
        dbnrows = "integer",  
        tblData = "tbl_dbi",
        indexes = "list",
        dbconcatKey = "ANY" 
    )
)

### Constructor
#' @rdname SQLDataFrame-class
#' @description \code{SQLDataFrame} constructor, slot getters, show
#'     method and coercion methods to \code{DataFrame} and
#'     \code{data.frame} objects.
#' @param conn a valid \code{DBIConnection} from \code{SQLite},
#'     \code{MySQL} or \code{BigQuery}. If provided, arguments of
#'     `user`, `host`, `dbname`, `password` will be ignored.
#' @param host host name for MySQL database or project name for
#'     BigQuery.
#' @param user user name for SQL database.
#' @param dbname database name for SQL connection.
#' @param password password for SQL database connection.
#' @param billing the Google Cloud project name with authorized
#'     billing information.
#' @param type The SQL database type, supports "SQLite", "MySQL" and
#'     "BigQuery".
#' @param dbtable A character string for the table name in that
#'     database. If not provided and there is only one table
#'     available, it will be read in by default.
#' @param dbkey A character vector for the name of key columns that
#'     could uniquely identify each row of the database table. Will be
#'     ignored for \code{BigQueryConnection}.
#' @param col.names A character vector specifying the column names you
#'     want to read into the \code{SQLDataFrame}.
#' @return A \code{SQLDataFrame} object.
#' @export
#' @importFrom tools file_path_as_absolute
#' @import RSQLite
#' @import dbplyr
#' @examples
#' 
#' ## SQLDataFrame construction
#' dbname <- system.file("extdata/test.db", package = "SQLDataFrame")
#' conn <- DBI::dbConnect(DBI::dbDriver("SQLite"), dbname = dbname)
#' obj <- SQLDataFrame(conn = conn, dbtable = "state",
#'                     dbkey = "state")
#' obj1 <- SQLDataFrame(conn = conn, dbtable = "state",
#'                      dbkey = c("region", "population"))
#' obj1
#'
#' ### construction from database credentials
#' obj2 <- SQLDataFrame(dbname = dbname, type = "SQLite",
#'                      dbtable = "state", dbkey = "state")
#' all.equal(obj, obj2)  ## [1] TRUE
#' 
#' ## slot accessors
#' dbcon(obj)
#' dbtable(obj)
#' dbkey(obj)
#' dbkey(obj1)
#'
#' dbconcatKey(obj)
#' dbconcatKey(obj1)
#'
#' ## slot accessors (for internal use only)
#' tblData(obj)
#' dbnrows(obj)
#' 
#' ## ROWNAMES
#' ROWNAMES(obj[sample(10, 5), ])
#' ROWNAMES(obj1[sample(10, 5), ])
#'
#' ## coercion
#' as.data.frame(obj)
#' as(obj, "DataFrame")
#' 
#' 
#' ## dbkey replacement
#' dbkey(obj) <- c("region", "population")
#' obj
#'
#' ## construction from MySQL 
#' \dontrun{
#' mysqlConn <- DBI::dbConnect(dbDriver("MySQL"),
#'                             host = "",
#'                             user = "",
#'                             password = "",  ## required if need further
#'                                             ## aggregation operations such as
#'                                             ## join, union, rbind, etc. 
#'                             dbname = "")
#' sdf <- SQLDataFrame(conn = mysqlConn, dbtable = "", dbkey = "")
#'
#' ## Or pass credentials directly into constructor
#' objensb <- SQLDataFrame(user = "genome",
#'                         host = "genome-mysql.soe.ucsc.edu",
#'                         dbname = "xenTro9",
#'                         type = "MySQL",
#'                         dbtable = "xenoRefGene",
#'                         dbkey = c("name", "txStart"))
#' 
#' }
#' 
#' ## construction from BigQuery
#' \dontrun{
#' con <- DBI::dbConnect(bigquery(),  ## equivalent dbDriver("bigquery")
#'                       project = "bigquery-public-data",
#'                       dataset = "human_variant_annotation",
#'                       billing = "")  ## your project name that linked to
#'                                      ## Google Cloud with billing information.
#' sdf <- SQLDataFrame(conn = con, dbtable = "ncbi_clinvar_hg38_20180701")
#'
#' ## Or pass credentials directly into constructor
#' sdf1 <- SQLDataFrame(host = "bigquery-public-data",
#'                      dbname = "human_variant_annotation",
#'                      billing = "",
#'                      type = "BigQuery",
#'                      dbtable = "ncbi_clinvar_hg38_20180701")
#'
#' }

SQLDataFrame <- function(conn,
                         host, user, dbname,
                         password = NULL, ## required for certain MySQL connection.
                         billing = character(0),  ## BigQuery connection.
                         type = c("SQLite", "MySQL", "BigQuery"),
                         dbtable = character(0), ## could be NULL if
                                                 ## only 1 table exists!
                         dbkey = character(0),
                         col.names = NULL
                         ){

    type <- match.arg(type)
    if (missing(conn)) {
        ## FIXME: Error in .local(drv, ...) : Cannot allocate a new
        ## connection: 16 connections already opened
        conn <- switch(type,
                       MySQL = DBI::dbConnect(dbDriver("MySQL"),
                                              host = host,
                                              user = user,
                                              password = password,
                                              dbname = dbname),
                       SQLite = DBI::dbConnect(dbDriver("SQLite"),
                                               dbname = dbname),
                       BigQuery = DBI::dbConnect(dbDriver("bigquery"),
                                                 project = host,
                                                 dataset = dbname,
                                                 billing = billing)
                       )
    } else {
        ifcred <- c(host = !missing(host), user = !missing(user),
                    dbname = !missing(dbname), password = !missing(password),
                    billing = !missing(billing))
        if (any(ifcred))
            message("These arguments are ignored: ",
                    paste(names(ifcred[ifcred]), collapse = ", "))
    }
    ## check dbtable
    tbls <- DBI::dbListTables(conn)
    if (missing(dbtable) || !dbExistsTable(conn, dbtable)) {
        if (length(tbls) == 1) {
            dbtable <- tbls
            message(paste0("Using the only table: '", tbls,
                           "' that is available in the connection"))
        } else {
            stop("Please specify the 'dbtable' argument, ",
                 "which must be one of: '",
                 paste(tbls, collapse = ", "), "'")
        }
    }
    ## check dbkey
    if (is(conn, "BigQueryConnection")) {
        if (!missing(dbkey))
            cat("The argument: '", dbkey, "' is ignored for BigQueryConnection.")        
    } else {
        flds <- dbListFields(conn, dbtable)
        if (!all(dbkey %in% flds)){
            stop("Please specify the 'dbkey' argument, ",
                 "which must be one of: '",
                 paste(flds, collapse = ", "), "'")
        }
    }
    
    if (is(conn, "MySQLConnection")) {
        .set_mysql_var(conn, password)
    }        
    
    ## construction
    ### ROW_NUMBER() only works here for "BigQueryConnection", not for
    ### SQLiteConnection, and MySQLConnection
    if (is(conn, "BigQueryConnection")) {
        tbl <- tbl(conn, sql(paste0("SELECT ROW_NUMBER() OVER() AS SurrogateKey, * FROM ", dbtable)))
        dbkey <- "SurrogateKey"
    } else {
        tbl <- conn %>% tbl(dbtable)   ## ERROR if "dbtable" does not exist!
    }
    ## dbnrows <- tbl %>% summarize(n = n()) %>% pull(n) %>% as.integer  ## FIXME 
    dbnrows <- tbl %>% transmute(cons = 1.0) %>% count(cons) %>% pull(n) %>% as.integer
    ## col.names
    cns <- colnames(tbl)
    if (is.null(col.names)) {
        col.names <- cns
        cidx <- NULL
    } else {
        idx <- col.names %in% cns
        wmsg <- paste0(
            "The 'col.names' of '",
            paste(col.names[!idx], collapse = ", "),
            "' does not exist!")
        if (!any(idx)) {
            warning(
                wmsg, " Will use 'col.names = colnames(dbtable)'",
                " as default.")
            col.names <- cns
            cidx <- NULL
        } else {
            warning(wmsg, " Only '",
                    paste(col.names[idx], collapse = ", "),
                    "' will be used.")
            col.names <- col.names[idx]
            cidx <- match(col.names, cns)
        }
    }
    
    ## ridx
    ridx <- NULL
    ridxTableName <- paste0(dbtable, "_ridx")
    if (ridxTableName %in% tbls) {
        ridx <- dbReadTable(conn, ridxTableName)$ridx
    }
    
    ## concatKey
    if (is(conn, "BigQueryConnection")) {
        concatKey <- seq_len(dbnrows)
    } else {
        concatKey <- tbl %>%
            mutate(concatKey = paste(!!!syms(dbkey), sep=":")) %>%
            pull(concatKey)
    }
    .SQLDataFrame(
        dbkey = dbkey,
        dbnrows = dbnrows,
        tblData = tbl,
        indexes = list(ridx, cidx),
        dbconcatKey = concatKey
    )
}

.validity_SQLDataFrame <- function(object)
{
    idx <- object@indexes
    if (length(idx) != 2) {
        stop("The indexes for SQLDataFrame should have 'length == 2'")
    }
    if (any(duplicated(dbconcatKey(object)))) {
        stop("The 'dbkey' column of SQLDataFrame '", dbkey(object),
             "' must have unique values!")
    }
}
setValidity("SQLDataFrame", .validity_SQLDataFrame)

###-------------
## accessor
###-------------

setGeneric("tblData", signature = "x", function(x)
    standardGeneric("tblData"))

#' @rdname SQLDataFrame-class
#' @aliases tblData tblData,SQLDataFrame-method
#' @export

setMethod("tblData", "SQLDataFrame", function(x)
{
    x@tblData
})

setGeneric("dbtable", signature = "x", function(x)
    standardGeneric("dbtable"))

#' @rdname SQLDataFrame-class
#' @aliases dbtable dbtable,SQLDataFrame-method
#' @export

setMethod("dbtable", "SQLDataFrame", function(x)
{
    res <- dbplyr::remote_name(tblData(x))
    if (is.null(res))
        warning(.msg_dbtable)
    as.character(res)

    ## } else {
    ##     warning(.msg_dbtable)
    ## }
})
.msg_dbtable <- paste0("## not available for SQLDataFrame with lazy queries ",
                       "of 'union', 'join', or 'rbind'. \n",
                       "## call 'saveSQLDataFrame()' to save the data as ",
                       "database table and call 'dbtable()' again! \n")

setGeneric("dbkey", signature = "x", function(x)
    standardGeneric("dbkey"))

#' @rdname SQLDataFrame-class
#' @aliases dbkey dbkey,SQLDataFrame-method
#' @export
setMethod("dbkey", "SQLDataFrame", function(x) x@dbkey )

setGeneric(
    "dbkey<-",
    function(x, value) standardGeneric("dbkey<-"),
    signature="x")

## Works like constructing a new SQLDF and calculate the dbconcatKey
## which is must.
#' @name "dbkey<-"
#' @rdname SQLDataFrame-class
#' @aliases dbkey<- dbkey<-,SQLDataFrame-method
#' @param value The column name to be used as \code{dbkey(x)}
#' @rawNamespace import(BiocGenerics, except=c("combine"))
#' @export
setReplaceMethod( "dbkey", "SQLDataFrame", function(x, value) {
    if (is(dbcon(x), "BigQueryConnection"))
        stop("Redefining of 'dbkey' for BigQueryConnection is not supported!")
    if (!all(value %in% colnames(tblData(x))))
        stop("Please choose 'dbkey' from the following: ",
             paste(colnames(tblData(x)), collapse = ", "))
    concatKey <- tblData(x) %>%
        mutate(concatKey = paste(!!!syms(value), sep=":")) %>%
        pull(concatKey)
    BiocGenerics:::replaceSlots(x, dbkey = value,
                                dbconcatKey = concatKey,
                                check=TRUE)
})

setGeneric("dbconcatKey", signature = "x", function(x)
    standardGeneric("dbconcatKey"))

#' @rdname SQLDataFrame-class
#' @aliases dbconcatKey dbconcatKey,SQLDataFrame-method
#' @export
setMethod("dbconcatKey", "SQLDataFrame", function(x)
{
    x@dbconcatKey
})

setGeneric("dbnrows", signature = "x", function(x)
    standardGeneric("dbnrows"))

#' @rdname SQLDataFrame-class
#' @aliases dbnrows dbnrows,SQLDataFrame-method
#' @export
setMethod("dbnrows", "SQLDataFrame", function(x)
{
    x@dbnrows
})

#' @rdname SQLDataFrame-class
#' @aliases ROWNAMES ROWNAMES,SQLDataFrame-method
#' @export
setMethod("ROWNAMES", "SQLDataFrame", function(x)
{
    ridx <- ridx(x)
    res <- dbconcatKey(x)
    if (!is.null(ridx))
        res <- dbconcatKey(x)[ridx]
    return(res)
})

###--------------
### show method
###--------------

## input "tbl_dbi" and output "tbl_dbi".

## filter() makes sure it returns table with unique rows, no duplicate rows allowed...
.extract_tbl_rows_by_key <- function(x, key, concatKey, i)  ## "concatKey" must correspond to "x"
{
    ## always require a dbkey(), and accommodate with multiple key columns. 
    i <- sort(unique(i))
    if (is(x$src$con, "BigQueryConnection")) {
        out <- x %>% filter(SurrogateKey %in% i)
    } else {
        x <- x %>% mutate(concatKeys = paste(!!!syms(key), sep=":"))
        ## FIXME: possible to remove the ".0" trailing after numeric values?
        ## see: https://github.com/tidyverse/dplyr/issues/3230 (deliberate...)
        out <- x %>% filter(concatKeys %in% !!(concatKey[i])) %>% select(-concatKeys)
    }
    ## returns "tbl_dbi" object, no realization.
    return(out)
}

## Nothing special, just queried the ridx, and ordered tbl by "key+otherCols"
.extract_tbl_from_SQLDataFrame_indexes <- function(tbl, sdf, collect = FALSE)
{
    ridx <- ridx(sdf)
    if (!is.null(ridx))
        tbl <- .extract_tbl_rows_by_key(tbl, dbkey(sdf),
                                        dbconcatKey(sdf), ridx)
    if (collect)
        tbl <- collect(tbl)
    tbl.out <- tbl %>% select(dbkey(sdf), colnames(sdf))
    ## columns ordered by "key + otherCols"
    return(tbl.out)
}

## .printROWS realize all ridx(x), so be careful here to only use small x.
.printROWS <- function(x, index){
    tbl <- .extract_tbl_from_SQLDataFrame_indexes(tblData(x), x, collect = TRUE)
    ## already ordered by "key + otherCols".
    out.tbl <- collect(tbl)
    ridx <- normalizeRowIndex(x)
    i <- match(index, sort(unique(ridx))) 
    
    out <- as.matrix(unname(cbind(
        out.tbl[i, seq_along(dbkey(x))],
        rep("|", length(i)),
        out.tbl[i, -seq_along(dbkey(x))])))
    return(out)
}

#' @rdname SQLDataFrame-class
#' @param object An \code{SQLDataFrame} object.
#' @importFrom lazyeval interp
#' @import S4Vectors
#' @export

setMethod("show", "SQLDataFrame", function (object) 
{
    nhead <- get_showHeadLines()
    ntail <- get_showTailLines()
    nr <- nrow(object)
    nc <- ncol(object)
    cat(class(object), " with ", nr, ifelse(nr == 1, " row and ", 
        " rows and "), nc, ifelse(nc == 1, " column\n", " columns\n"), 
        sep = "")
    if (nr > 0 && nc >= 0) {
        if (nr <= (nhead + ntail + 1L)) {
            out <- .printROWS(object, normalizeRowIndex(object))
        }
        else {
            sdf.head <- object[seq_len(nhead), , drop=FALSE]
            sdf.tail <- object[tail(seq_len(nrow(object)), ntail), , drop=FALSE]
            out <- rbind(
                .printROWS(sdf.head, ridx(sdf.head)),
                c(rep.int("...", length(dbkey(object))),".", rep.int("...", nc)),
                .printROWS(sdf.tail, ridx(sdf.tail)))
        }
        classinfoFun <- function(tbl, colnames) {
            matrix(unlist(lapply(
            as.data.frame(head(tbl %>% select(colnames))),  ## added a layer on lazy tbl. 
            function(x)
            { paste0("<", classNameForDisplay(x)[1], ">") }),
            use.names = FALSE), nrow = 1,
            dimnames = list("", colnames))}
        classinfo_key <- classinfoFun(tblData(object), dbkey(object))
        classinfo <- matrix(c(classinfo_key, "|"), nrow = 1,
                            dimnames = list("", c(dbkey(object), "|")))
        if (nc > 0) {
            classinfo_other <- classinfoFun(tblData(object), colnames(object))
            classinfo <- cbind(classinfo, classinfo_other)
        }
        out <- rbind(classinfo, out)
        print(out, quote = FALSE, right = TRUE)
    }
})

###--------------
### coercion
###--------------

#' @rdname SQLDataFrame-class
#' @name coerce
#' @aliases coerce,SQLDataFrame,data.frame-method
#' @export

setAs("SQLDataFrame", "data.frame", function(from)
{
    as.data.frame(from, optional = TRUE)
})

## refer to: getAnywhere(as.data.frame.tbl_sql) defined in dbplyr
.as.data.frame.SQLDataFrame <- function(x, row.names = NULL,
                                        optional = NULL, ...)
{
    tbl <- .extract_tbl_from_SQLDataFrame_indexes(tblData(x), x)
    ridx <- normalizeRowIndex(x)
    i <- match(ridx, sort(unique(ridx))) 
    as.data.frame(tbl, row.names = row.names, optional = optional)[i, ]
}

#' @name coerce
#' @rdname SQLDataFrame-class
#' @aliases as.data.frame,SQLDataFrame-method
#' @param x An \code{SQLDataFrame} object
#' @param row.names \code{NULL} or a character vector giving the row
#'     names for the data frame. Only including this argument for the
#'     \code{as.data.frame} generic. Does not apply to
#'     \code{SQLDataFrame}. See \code{base::as.data.frame} for
#'     details.
#' @param optional logical. If \code{TRUE}, setting row names and
#'     converting column names is optional. Only including this
#'     argument for the \code{as.data.frame} generic. Does not apply
#'     to \code{SQLDataFrame}. See \code{base::as.data.frame} for
#'     details.
#' @param ... additional arguments to be passed.
#' @export
setMethod("as.data.frame", signature = "SQLDataFrame", .as.data.frame.SQLDataFrame)

#' @name coerce
#' @rdname SQLDataFrame-class
#' @aliases coerce,SQLDataFrame,DataFrame-method
#' @export
#' 
setAs("SQLDataFrame", "DataFrame", function(from)
{
    as(as.data.frame(from), "DataFrame")
})

#' @name coerce
#' @rdname SQLDataFrame-class
#' @aliases coerce,data.frame,SQLDataFrame-method
#' @export
#' 
setAs("data.frame", "SQLDataFrame", function(from)
{
    ## check if unique for columns (from left to right)
    for (i in seq_len(ncol(from))) {
        ifunique <- !any(duplicated(from[,i]))
        if (ifunique) break
    }
    dbkey <- colnames(from)[i]
    msg <- paste0("## Using the '", dbkey, "' as 'dbkey' column. \n",
                  "## Otherwise, construct a 'SQLDataFrame' by: \n",
                  "## 'makeSQLDataFrame(filename, dbkey = '')' \n")
    message(msg)
    makeSQLDataFrame(from, dbkey = colnames(from)[i])
})

#' @name coerce
#' @rdname SQLDataFrame-class
#' @aliases coerce,DataFrame,SQLDataFrame-method
#' @export
#' 
setAs("DataFrame", "SQLDataFrame", function(from)
{
    as(as.data.frame(from), "SQLDataFrame")
})

#' @name coerce
#' @rdname SQLDataFrame-class
#' @aliases coerce,ANY,SQLDataFrame-method
#' @export
#' 
setAs("ANY", "SQLDataFrame", function(from)
{
    from <- as.data.frame(from)
    if (ncol(from) == 1)
        stop("There should be more than 1 columns available ",
             "to construct a 'SQLDataFrame' object")
    if (anyDuplicated(tolower(colnames(from))))
        stop("Please use distinct colnames (case-unsensitive)!")
    as(as.data.frame(from), "SQLDataFrame")
})

