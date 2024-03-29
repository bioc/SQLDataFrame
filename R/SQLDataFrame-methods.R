###---------------------------
### Basic methods
###--------------------------- 

#' SQLDataFrame methods
#' @name SQLDataFrame-methods
#' @description \code{head, tail}: Retrieve the first / last n rows of
#'     the \code{SQLDataFrame} object. See \code{?S4Vectors::head} for
#'     more details.
#' @param x An \code{SQLDataFrame} object.
#' @param n Number of rows.
#' @rdname SQLDataFrame-methods
#' @aliases head head,SQLDataFrame-method
#' @return \code{head, tail}: An \code{SQLDataFrame} object with
#'     certain rows.
#' @export
#' 
setMethod("head", "SQLDataFrame", function(x, n=6L)
{
    stopifnot(length(n) == 1L)
    n <- if (n < 0L) 
             max(nrow(x) + n, 0L)
         else min(n, nrow(x))
    x[seq_len(n), , drop = FALSE]
})

#' @rdname SQLDataFrame-methods
#' @aliases tail tail,SQLDataFrame-method
#' @export
#' 
## mostly copied from "tail,DataTable"
setMethod("tail", "SQLDataFrame", function(x, n=6L)
{
    stopifnot(length(n) == 1L)
    nrx <- nrow(x)
    n <- if (n < 0L) 
             max(nrx + n, 0L)
         else min(n, nrx)
    sel <- as.integer(seq.int(to = nrx, length.out = n))
    ans <- x[sel, , drop = FALSE]
    ans    
})

#' @description \code{dim, nrow, ncol, length, dimnames, rownames,
#'     colnames, names}: Retrieve the dimension and dimension names of
#'     SQLDataFrame object.
#' @export
#' @examples
#' 
#' ##################
#' ## basic methods
#' ##################
#' 
#' test.db <- system.file("extdata/test.db", package = "SQLDataFrame")
#' conn <- DBI::dbConnect(DBI::dbDriver("SQLite"), dbname = test.db)
#' obj <- SQLDataFrame(conn = conn, dbtable = "state", dbkey = "state")
#' dim(obj)
#' nrow(obj)
#' ncol(obj)
#' dimnames(obj)
#' rownames(obj)
#' names(obj)
#' colnames(obj)
#' length(obj)

#' @rdname SQLDataFrame-methods
#' @aliases nrow nrow,SQLDataFrame-method
#' @aliases dim dim,SQLDataFrame-method
#' @return \code{dim}: interger vector showing number of rows and cols.  
#' @return \code{nrow}: An integer showing number of rows.
#' @export
setMethod("nrow", "SQLDataFrame", function(x) length(normalizeRowIndex(x)))

#' @rdname SQLDataFrame-methods
#' @aliases names names,SQLDataFrame-method
#' @return \code{names}: A character vector.
#' @export

## used inside "[[, normalizeDoubleBracketSubscript(i, x)" 
setMethod("names", "SQLDataFrame", function(x) {
    cns <- colnames(tblData(x))[-.wheredbkey(x)]
    cidx <- x@indexes[[2]]
    if (!is.null(cidx))
        cns <- cns[cidx]
    cns
})

#' @rdname SQLDataFrame-methods
#' @aliases names<- names<-,SQLDataFrame
#' @return \code{names<-}: No change. 
#' @export

setReplaceMethod("names", "SQLDataFrame", function(x, value) x)

#' @rdname SQLDataFrame-methods
#' @param do.NULL logical. If ‘FALSE’ and names are ‘NULL’, names are
#'     created. See \code{base::colnames} for details.
#' @param prefix for created names when \code{do.NULL} is FALSE.
#' @aliases colnames colnames,SQLDataFrame-method
#' @return \code{colnames}: A character vector.
#' @export

setMethod("colnames", "SQLDataFrame", function (x, do.NULL = TRUE, prefix = "col") {
    if (!(identical(do.NULL, TRUE) && identical(prefix, "col"))) 
        stop(wmsg("argument 'do.NULL' and 'prefix' are not supported"))
    names(x)
})

#' @rdname SQLDataFrame-methods
#' @param value a valid value for component of \code{rownames},
#'     \code{colnames}, \code{dimnames}. NOTE, it doesn't make any
#'     change when applied to these replacement methods for
#'     \code{SQLDataFrame}.
#' @aliases colnames<- colnames<-,SQLDataFrame-method
#' @return \code{colnames<-}: No change.
#' @export

setReplaceMethod("colnames", "SQLDataFrame", function(x, value) x)

#' @rdname SQLDataFrame-methods
#' @aliases rownames rownames,SQLDataFrame-method
#' @return \code{rownames}: NULL or vector if "do.NULL = FALSE".
#' @export

setMethod("rownames", "SQLDataFrame", function(x, do.NULL=TRUE, prefix="row") {
    rn <- NULL
    if (is.null(rn) && !do.NULL) {
        nr <- NROW(x)
        if (nr > 0L) 
            rn <- paste(prefix, seq_len(nr), sep = "")
        else rn <- character(0L)
    }
    rn
})

#' @rdname SQLDataFrame-methods
#' @aliases rownames<- rownames<-,SQLDataFrame-method
#' @return \code{rownames<-}: No change. 
#' @export

## referred rownames<-,DFrame, but should always return x with NULL rownames
setReplaceMethod("rownames", "SQLDataFrame", function(x, value) x)
        
#' @rdname SQLDataFrame-methods
#' @aliases length length,SQLDataFrame-method
#' @return \code{length}: An integer same as \code{ncol}.
#' @export

setMethod("length", "SQLDataFrame", function(x) length(colnames(x)))

#' @rdname SQLDataFrame-methods
#' @aliases dimnames dimnames,SQLDataFrame-method
#' @return \code{dimnames}: A list. 
#' @export

setMethod("dimnames", "SQLDataFrame", function(x)
{
    ans <- list(rownames(x), colnames(x))
    DelayedArray:::simplify_NULL_dimnames(ans)
})

#' @rdname SQLDataFrame-methods
#' @aliases dimnames<- dimnames<-,SQLDataFrame-method
#' @return \code{dimnames<-}: No change. 
#' @export

setReplaceMethod("dimnames", "SQLDataFrame", function(x, value)
{
    if (!(is.list(value) && length(value) == 2L)) 
        stop(wmsg("dimnames replacement value must be a list of length 2"))
    callNextMethod()
})


###--------------------
### "[,SQLDataFrame"
###-------------------- 
.extractROWS_SQLDataFrame <- function(x, i)
{
    i <- normalizeSingleBracketSubscript(i, x)
    ridx <- x@indexes[[1]]
    if (is.null(ridx)) {
        if (! identical(i, seq_len(x@dbnrows)))
            x@indexes[[1]] <- i
    } else {
        x@indexes[[1]] <- x@indexes[[1]][i]
    }
    return(x)
}
setMethod("extractROWS", "SQLDataFrame", .extractROWS_SQLDataFrame)

#' @importFrom stats setNames
.extractCOLS_SQLDataFrame <- function(x, j)
{
    xstub <- setNames(seq_along(x), names(x))
    if (is.character(j) & any(j %in% dbkey(x)))
        j <- setdiff(j, dbkey(x))
    j <- normalizeSingleBracketSubscript(j, xstub)
    cidx <- x@indexes[[2]]
    if (is.null(cidx)) {
        if (!identical(j, seq_along(colnames(x))))
            x@indexes[[2]] <- j
    } else {
            x@indexes[[2]] <- x@indexes[[2]][j]
    }
    return(x)
}

#' @description \code{[i, j]} supports subsetting by \code{i} (for
#'     row) and \code{j} (for column) and respects ‘drop=FALSE’.
#' @rdname SQLDataFrame-methods
#' @param i Row subscript. Could be numeric / character / logical
#'     values, a named list of key values, and \code{SQLDataFrame},
#'     \code{data.frame}, \code{tibble} objects.
#' @param j Column subscript.
#' @param drop Whether to drop with reduced dimension. Default is
#'     TRUE.
#' @return \code{[i, j]}: A \code{SQLDataFrame} object or vector with
#'     realized column values (with single column subsetting and
#'     default \code{drop=TRUE}. )
#' @aliases [,SQLDataFrame,ANY-method
#' @importFrom tibble tibble
#' @export
#' @examples
#'
#' obj1 <- SQLDataFrame(conn = conn, dbtable = "state",
#'                      dbkey = c("region", "population"))

#' ###############
#' ## subsetting
#' ###############
#'
#' obj[1]
#' obj["region"]
#' obj$region
#' obj[]
#' obj[,]
#' obj[NULL, ]
#' obj[, NULL]
#'
#' ## by numeric / logical / character vectors
#' obj[1:5, 2:3]
#' obj[c(TRUE, FALSE), c(TRUE, FALSE)]
#' obj[c("Alabama", "South Dakota"), ]
#' obj1[c("South:3615.0", "West:3559.0"), ]
#' ### Remeber to add `.0` trailing for numeric values. If not sure,
#' ### check `ROWNAMES()`.
#'
#' ## by SQLDataFrame
#' obj_sub <- obj[sample(10), ]
#' obj[obj_sub, ]
#'
#' ## by a named list of key column values (or equivalently data.frame /
#' ## tibble)
#' obj[data.frame(state = c("Colorado", "Arizona")), ]
#' obj[tibble::tibble(state = c("Colorado", "Arizona")), ]
#' obj[list(state = c("Colorado", "Arizona")), ]
#' obj1[list(region = c("South", "West"),
#'           population = c("3615.0", "365.0")), ]
#' ### remember to add the '.0' trailing for numeric values. If not sure,
#' ### check `ROWNAMES()`.
#'
#' ## Subsetting with key columns
#'
#' obj["state"] ## list style subsetting, return a SQLDataFrame object with col = 0.
#' obj[c("state", "division")]  ## list style subsetting, return a SQLDataFrame object with col = 1.
#' obj[, "state"] ## realize specific key column value.
#' obj[, c("state", "division")] ## col = 1, but do not realize.
#' 


setMethod("[", "SQLDataFrame", function(x, i, j, ..., drop = TRUE)
{
    if (!isTRUEorFALSE(drop)) 
        stop("'drop' must be TRUE or FALSE")
    if (length(list(...)) > 0L) 
        warning("parameters in '...' not supported")
    list_style_subsetting <- (nargs() - !missing(drop)) < 3L
    if (list_style_subsetting || !missing(j)) {
        if (list_style_subsetting) {
            if (!missing(drop)) 
                warning("'drop' argument ignored by list-style subsetting")
            if (missing(i)) 
                return(x)  ## x[] 
            j <- i  ## x[i]
        }
        if (!is(j, "IntegerRanges")) {
            x <- .extractCOLS_SQLDataFrame(x, j) ## x["key"] returns
                                                 ## SQLSataFrame with
                                                 ## 0 cols.
        }
        if (list_style_subsetting) 
            return(x)
    }
    if (!missing(i)) { 
        x <- extractROWS(x, i)
    }
    if (missing(drop)) 
        drop <- nrow(x) & ncol(x) %in% c(0L, 1L) ## if nrow(x)==0,
                                                 ## return the
                                                 ## SQLDataFrame with
                                                 ## 0 rows and 1
                                                 ## column(s)
    if (drop) {
        if (ncol(x) == 1L & length(j) == 1) ## x[, "col"] realize.
                                            ## x[,c("key", "other")]
                                            ## do not realize.
            return(x[[1L]])
        if (ncol(x) == 0 & !is.null(j) & length(j) == 1)
            return(x[[j]]) ## x[,"key"] returns realized value of that
                           ## key column.
        if (nrow(x) == 1L) 
            return(as(x, "list"))
    }
    x
})

#' @rdname SQLDataFrame-methods
#' @importFrom methods is as callNextMethod
#' @aliases [,SQLDataFrame,SQLDataFrame-method 
#' @export
setMethod("[", signature = c("SQLDataFrame", "SQLDataFrame", "ANY"),
          function(x, i, j, ..., drop = TRUE)
{
    if (!identical(dbkey(x), dbkey(i)))
        stop("The dbkey() must be same between '", deparse(substitute(x)),
             "' and '", deparse(substitute(i)), "'.", "\n")
    i <- ROWNAMES(i)
    callNextMethod()
})

#' @rdname SQLDataFrame-methods
#' @aliases [,SQLDataFrame,list-method 
#' @export
setMethod("[", signature = c("SQLDataFrame", "list", "ANY"),
          function(x, i, j, ..., drop = TRUE)
{
    if (!identical(dbkey(x), union(dbkey(x), names(i))))
        stop("Please use: '", paste(dbkey(x), collapse=", "),
             "' as the query list name(s).")
    i <- do.call(paste, c(i[dbkey(x)], sep=":"))
    callNextMethod()
})

###--------------------
### "[[,SQLDataFrame" (do realization for single column only)
###--------------------

#' @rdname SQLDataFrame-methods
#' @export
setMethod("[[", "SQLDataFrame", function(x, i, j, ...)
{
    dotArgs <- list(...)
    if (length(dotArgs) > 0L) 
        dotArgs <- dotArgs[names(dotArgs) != "exact"]
    if (!missing(j) || length(dotArgs) > 0L) 
        stop("incorrect number of subscripts")
    ## extracting key col value 
    if (is.character(i) && length(i) == 1 && i %in% dbkey(x)) {
        res <- .extract_tbl_from_SQLDataFrame_indexes(tblData(x), x) %>% select(i) %>% pull()
        return(res)
    }
    i2 <- normalizeDoubleBracketSubscript(
        i, x,
        exact = TRUE,  ## default
        allow.NA = TRUE,
        allow.nomatch = TRUE)
    ## "allow.NA" and "allow.nomatch" is consistent with
    ## selectMethod("getListElement", "list") <- "simpleList"
    if (is.na(i2))
        return(NULL)
    tblData <- .extract_tbl_from_SQLDataFrame_indexes(tblData(x), x) %>% select(- !!dbkey(x))
    res <- tblData %>% pull(i2)
    return(res)
})

#' @rdname SQLDataFrame-methods
#' @param name column name to be extracted by \code{$}.
#' @export
setMethod("$", "SQLDataFrame", function(x, name) x[[name]] )

#############################
### select, filter & mutate
#############################

#' @description Use \code{select()} function to select certain
#'     columns.
#' @rdname SQLDataFrame-methods
#' @aliases select select,SQLDataFrame-methods
#' @return \code{select}: always returns a SQLDataFrame object no
#'     matter how may columns are selected. If only key column(s)
#'     is(are) selected, it will return a \code{SQLDataFrame} object
#'     with 0 col (only key columns are shown).
#' @param .data A \code{SQLDataFrame} object.
#' @param ... additional arguments to be passed.
#' \itemize{
#' \item \code{select()}: One or more unquoted expressions separated
#'     by commas. You can treat variable names like they are
#'     positions, so you can use expressions like ‘x:y’ to select
#'     ranges of variables. Positive values select variables; negative
#'     values drop variables. See \code{?dplyr::select} for more
#'     details.
#' \item \code{filter()}: Logical predicates defined in terms of the
#'     variables in ‘.data’. Multiple conditions are combined with
#'     ‘&’. Only rows where the condition evaluates to ‘TRUE’ are
#'     kept. See \code{?dplyr::filter} for more details.
#' \item \code{mutate()}: Name-value pairs of expressions, each with
#'     length 1 or the same length as the number of rows in the group
#'     (if using ‘group_by()’) or in the entire input (if not using
#'     groups). The name of each argument will be the name of a new
#'     variable, and the value will be its corresponding value.
#'      New variables
#'     overwrite existing variables of the same name. NOTE that the new
#'     value could only be of length 1 or the operation of existing columns.
#'     If a new vector of values are given, error will return. This is due
#'     to the internal method of 'mutate.tbl_lazy' not being able to take
#'     new arbitrary values. 
#' }
#' @export
#' @examples
#' 
#' ###################
#' ## select, filter, mutate
#' ###################
#' library(dplyr)
#' obj %>% select(division)  ## equivalent to obj["division"], or obj[, "division", drop = FALSE]
#' obj %>% select(region:size)
#' 
#' obj %>% filter(region == "West" & size == "medium")
#' obj1 %>% filter(region == "West" & population > 10000)
#' 
#' ## obj %>% mutate(p1 = population / 10)
#' ## obj %>% mutate(s1 = size)
#'
#' obj %>% select(region, size, population) %>% 
#'     filter(population > 10000) ## %>% 
#'     ## mutate(pK = population/1000)
#' obj1 %>% select(region, size, population) %>% 
#'     filter(population > 10000) ## %>% 
#'     ## mutate(pK = population/1000)  

select.SQLDataFrame <- function(.data, ...)
{
    tbl <- .extract_tbl_from_SQLDataFrame_indexes(tblData(.data), .data)
    dots <- quos(...)
    old_vars <- op_vars(tbl$lazy_query)
    new_vars <- tidyselect::vars_select(old_vars, !!!dots, .include = op_grps(tbl$lazy_query))
    .extractCOLS_SQLDataFrame(.data, new_vars)
}

#' @description Use \code{filter()} to choose rows/cases where
#'     conditions are true.
#' @rdname SQLDataFrame-methods
#' @aliases filter filter,SQLDataFrame-method
#' @return \code{filter}: A \code{SQLDataFrame} object with subset
#'     rows of the input SQLDataFrame object matching conditions.
#' @export

filter.SQLDataFrame <- function(.data, ...)
{
    tbl <- .extract_tbl_from_SQLDataFrame_indexes(tblData(.data), .data)
    temp <- dplyr::filter(tbl, ...)

    if (is(tbl$src$con, "BigQueryConnection")) {
        rnms <- temp %>% pull(SurrogateKey)
    } else {
        rnms <- temp %>%
            transmute(concat = paste(!!!syms(dbkey(.data)), sep = ":")) %>%
            pull(concat)
    }
    idx <- match(rnms, ROWNAMES(.data))

    if (!identical(idx, normalizeRowIndex(.data))) {
        if (!is.null(ridx(.data))) {
            .data@indexes[[1]] <- ridx(.data)[idx]
        } else {
            .data@indexes[[1]] <- idx
        }
    }
    return(.data)
}

#' @description \code{mutate()} adds new columns and preserves
#'     existing ones; It also preserves the number of rows of the
#'     input. New variables overwrite existing variables of the same
#'     name.
#' @rdname SQLDataFrame-methods
#' @aliases mutate mutate,SQLDataFrame-methods
#' @return \code{mutate}: A SQLDataFrame object.
#' @export
#' 
mutate.SQLDataFrame <- function(.data, ...)
{
    if (is(dbcon(.data), "MySQLConnection")) {
        con <- dbcon(.data)
        tbl <- tblData(.data)
    } ## FIXME: generalize and remove duplicate code, check for SQLite
      ## cases, any chance to avoid creating new local connections?
    else {
        if (is(tblData(.data)$ops, "op_double") | is(tblData(.data)$ops, "op_single")) {
            con <- dbcon(.data)
            tbl <- tblData(.data)
        } else {
            dbname <- tempfile(fileext = ".db")
            con <- DBI::dbConnect(RSQLite::SQLite(), dbname = dbname)
            aux <- .attach_database(con, dbcon(.data)@dbname)
            auxSchema <- in_schema(aux, ident(dbtable(.data)))
        tbl <- tbl(con, auxSchema)
        }
    }
    tbl_out <- dplyr::mutate(tbl, ...) ## FIXME: use mutate(xx = NULL) to remove column?
    ## once done, add to @param ...: Use ‘NULL’ value in ‘mutate’ to drop a variable.
    out <- BiocGenerics:::replaceSlots(.data, tblData = tbl_out)

    ## check if not-null for the existing @indexes, and update for mutate.
    cidx <- .data@indexes[[2]]
    if (!is.null(cidx)) {
        cidx <- c(cidx,
                  setdiff(seq_len(ncol(tbl_out)), seq_len(ncol(tbl))) - length(dbkey(.data)))
        out <- BiocGenerics:::replaceSlots(out, indexes = list(ridx(.data), cidx))
    }
    return(out)
}

#' @description \code{dbcon} returns the connection of a
#'     SQLDataFrame object.
#' @rdname SQLDataFrame-methods
#' @export
#' @examples
#' 
#' ###################
#' ## connection info
#' ###################
#'
#' dbcon(obj)
dbcon <- function(x)
{
    dbplyr::remote_con(tblData(x))
}
