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
    pub function_transforms: HashMap<String, Arc<dyn FunctionTransform>>
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
            ].iter().map(|name| name.to_string()).collect(),
            function_transforms: Default::default(),
        }
    }

    pub fn sqlite() -> Self {
        let mut function_transforms: HashMap<String, Arc<dyn FunctionTransform>> = Default::default();
        function_transforms.insert("floor".to_string(), Arc::new(SqLiteFloorTransform));
        function_transforms.insert("ceil".to_string(), Arc::new(SqLiteCeilTransform));

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
                "zeroblob"
            ].iter().map(|name| name.to_string()).collect(),
            function_transforms
        }
    }
}

#[derive(Clone, Debug)]
struct SqLiteFloorTransform;
impl FunctionTransform for SqLiteFloorTransform {
    fn transform(&self, name: &str, args: &[String]) -> String {
        format!("round({} - 0.5)", &args[0])
    }
}

#[derive(Clone, Debug)]
struct SqLiteCeilTransform;
impl FunctionTransform for SqLiteCeilTransform {
    fn transform(&self, name: &str, args: &[String]) -> String {
        format!("round({} + 0.5)", &args[0])
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
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> fmt::Result;

    fn sql(&self, dialect: &Dialect) -> Result<String, fmt::Error>
    where
        Self: Sized,
    {
        let mut repr = String::new();
        DialectDisplay::fmt(self, &mut repr, dialect)?;
        Ok(repr)
    }
}
