use crate::parser::SqlGenError;
use core::fmt::Debug;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct Dialect {
    /// The starting quote if any. Valid quote characters are the single quote,
    /// double quote, backtick, and opening square bracket.
    pub quote_style: Option<char>,
    pub quote_functions: bool,
    pub functions: HashSet<String>,
    pub function_transforms: HashMap<String, Arc<dyn FunctionTransform>>,
}

pub trait FunctionTransform: Debug + Send + Sync {
    fn transform(&self, name: &str, args: &[String]) -> String;
}

impl Dialect {
    pub fn datafusion() -> Self {
        Self {
            quote_style: Some('"'),
            quote_functions: false,
            functions: vec![
                "abs",
                "acos",
                "asin",
                "atan",
                "atan2",
                "ceil",
                "coalesce",
                "cos",
                "digest",
                "exp",
                "floor",
                "ln",
                "log",
                "log10",
                "log2",
                "power",
                "round",
                "signum",
                "sin",
                "sqrt",
                "tan",
                "trunc",
                "array",
                "ascii",
                "bit_length",
                "btrim",
                "character_length",
                "chr",
                "concat",
                "concat_ws",
                "date_part",
                "date_trunc",
                "date_bin",
                "initcap",
                "left",
                "lpad",
                "lower",
                "ltrim",
                "md5",
                "nullif",
                "octet_length",
                "random",
                "regexp_replace",
                "repeat",
                "replace",
                "reverse",
                "right",
                "rpad",
                "rtrim",
                "sha224",
                "sha256",
                "sha384",
                "Sha512",
                "split_part",
                "starts_with",
                "strpos",
                "substr",
                "to_hex",
                "to_timestamp",
                "to_timestamp_millis",
                "to_timestamp_micros",
                "to_timestamp_seconds",
                "from_unixtime",
                "now",
                "translate",
                "trim",
                "upper",
                "regexp_match",
                "struct",
                // Aggregate functions
                "min",
                "max",
                "count",
                "avg",
                "mean",
                "sum",
                "median",
                "approx_distinct",
                "array_agg",
                "var",
                "var_samp",
                "var_pop",
                "stddev",
                "stddev_samp",
                "stddev_pop",
                "covar",
                "covar_samp",
                "covar_pop",
                "corr",
                "approx_percentile_cont",
                "approx_percentile_cont_with_weight",
                "approx_median",
                "grouping",
                // Window functions
                "row_number",
                "rank",
                "dense_rank",
                "percent_rank",
                "cume_dist",
                "ntile",
                "lag",
                "lead",
                "first_value",
                "last_value",
                "nth_value",
            ]
            .iter()
            .map(|name| name.to_string())
            .collect(),
            function_transforms: Default::default(),
        }
    }

    pub fn sqlite() -> Self {
        let mut function_transforms: HashMap<String, Arc<dyn FunctionTransform>> =
            Default::default();
        function_transforms.insert("floor".to_string(), Arc::new(SqLiteFloorTransform));
        function_transforms.insert("ceil".to_string(), Arc::new(SqLiteCeilTransform));
        function_transforms.insert("isfinite".to_string(), Arc::new(SqLiteIsFiniteTransform));
        function_transforms.insert("isnan".to_string(), Arc::new(SqLiteIsNanTransform));

        Self {
            quote_style: Some('"'),
            quote_functions: true,
            functions: vec![
                "abs",
                "changes",
                "char",
                "coalesce",
                "format",
                "glob",
                "hex",
                "ifnull",
                "iif",
                "instr",
                "last_insert_rowid",
                "length",
                "like",
                "likelihood",
                "likely",
                "load_extension",
                "lower",
                "ltrim",
                "max",
                "min",
                "nullif",
                "printf",
                "quote",
                "random",
                "randomblob",
                "replace",
                "round",
                "rtrim",
                "sign",
                "soundex",
                "sqlite_compileoption_get",
                "sqlite_compileoption_used",
                "sqlite_offset",
                "sqlite_source_id",
                "sqlite_version",
                "substr",
                "substring",
                "total_changes",
                "trim",
                "typeof",
                "unicode",
                "unlikely",
                "upper",
                "zeroblob",
                // Aggregation
                "avg",
                "count",
                "group_concat",
                "max",
                "min",
                "sum",
                "total",
                // Window functions
                "row_number",
                "rank",
                "dense_rank",
                "percent_rank",
                "cume_dist",
                "ntile",
                "lag",
                "lead",
                "first_value",
                "last_value",
                "nth_value",
            ]
            .iter()
            .map(|name| name.to_string())
            .collect(),
            function_transforms,
        }
    }
}

#[derive(Clone, Debug)]
struct SqLiteFloorTransform;
impl FunctionTransform for SqLiteFloorTransform {
    fn transform(&self, _name: &str, args: &[String]) -> String {
        format!("round({} - 0.5)", &args[0])
    }
}

#[derive(Clone, Debug)]
struct SqLiteCeilTransform;
impl FunctionTransform for SqLiteCeilTransform {
    fn transform(&self, _name: &str, args: &[String]) -> String {
        format!("round({} + 0.5)", &args[0])
    }
}

#[derive(Clone, Debug)]
struct SqLiteIsFiniteTransform;
impl FunctionTransform for SqLiteIsFiniteTransform {
    fn transform(&self, _name: &str, args: &[String]) -> String {
        format!("{arg} NOT IN ('NaN', '-Inf', 'Inf')", arg = &args[0])
    }
}

#[derive(Clone, Debug)]
struct SqLiteIsNanTransform;
impl FunctionTransform for SqLiteIsNanTransform {
    fn transform(&self, _name: &str, args: &[String]) -> String {
        format!("{arg} = 'NaN'", arg = &args[0])
    }
}

impl Default for Dialect {
    fn default() -> Self {
        Self {
            quote_style: None,
            quote_functions: false,
            functions: Default::default(),
            function_transforms: Default::default(),
        }
    }
}

pub trait DialectDisplay {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> Result<(), SqlGenError>;

    fn sql(&self, dialect: &Dialect) -> Result<String, SqlGenError>
    where
        Self: Sized,
    {
        let mut repr = String::new();
        DialectDisplay::fmt(self, &mut repr, dialect)?;
        Ok(repr)
    }
}
