// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[cfg(not(feature = "std"))]
use alloc::boxed::Box;
#[cfg(not(feature = "std"))]
use alloc::string::String;
use core::fmt;

use crate::dialect::{Dialect, DialectDisplay};
use crate::parser::SqlGenError;
#[cfg(feature = "bigdecimal")]
use bigdecimal::BigDecimal;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use super::Expr;

/// Primitive SQL values such as number and string
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Value {
    /// Numeric literal
    #[cfg(not(feature = "bigdecimal"))]
    Number(String, bool),
    #[cfg(feature = "bigdecimal")]
    Number(BigDecimal, bool),
    /// 'string value'
    SingleQuotedString(String),
    /// e'string value' (postgres extension)
    /// <https://www.postgresql.org/docs/8.3/sql-syntax-lexical.html#SQL-SYNTAX-STRINGS
    EscapedStringLiteral(String),
    /// N'string value'
    NationalStringLiteral(String),
    /// X'hex value'
    HexStringLiteral(String),

    DoubleQuotedString(String),
    /// Boolean value true or false
    Boolean(bool),
    /// INTERVAL literals, roughly in the following format:
    /// `INTERVAL '<value>' [ <leading_field> [ (<leading_precision>) ] ]
    /// [ TO <last_field> [ (<fractional_seconds_precision>) ] ]`,
    /// e.g. `INTERVAL '123:45.67' MINUTE(3) TO SECOND(2)`.
    ///
    /// The parser does not validate the `<value>`, nor does it ensure
    /// that the `<leading_field>` units >= the units in `<last_field>`,
    /// so the user will have to reject intervals like `HOUR TO YEAR`.
    Interval {
        value: Box<Expr>,
        leading_field: Option<DateTimeField>,
        leading_precision: Option<u64>,
        last_field: Option<DateTimeField>,
        /// The seconds precision can be specified in SQL source as
        /// `INTERVAL '__' SECOND(_, x)` (in which case the `leading_field`
        /// will be `Second` and the `last_field` will be `None`),
        /// or as `__ TO SECOND(x)`.
        fractional_seconds_precision: Option<u64>,
    },
    /// `NULL` value
    Null,
    /// `?` or `$` Prepared statement arg placeholder
    Placeholder(String),
}

impl DialectDisplay for Value {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> Result<(), SqlGenError> {
        Ok(match self {
            Value::Number(v, l) => write!(f, "{}{long}", v, long = if *l { "L" } else { "" }),
            Value::DoubleQuotedString(v) => write!(f, "\"{}\"", v),
            Value::SingleQuotedString(v) => {
                write!(f, "'{}'", escape_single_quote_string(v).sql(dialect)?)
            }
            Value::EscapedStringLiteral(v) => {
                write!(f, "E'{}'", escape_escaped_string(v).sql(dialect)?)
            }
            Value::NationalStringLiteral(v) => write!(f, "N'{}'", v),
            Value::HexStringLiteral(v) => write!(f, "X'{}'", v),
            Value::Boolean(v) => write!(f, "{}", v),
            Value::Interval {
                value,
                leading_field: Some(DateTimeField::Second),
                leading_precision: Some(leading_precision),
                last_field,
                fractional_seconds_precision: Some(fractional_seconds_precision),
            } => {
                // When the leading field is SECOND, the parser guarantees that
                // the last field is None.
                assert!(last_field.is_none());
                write!(
                    f,
                    "INTERVAL {} SECOND ({}, {})",
                    value.sql(dialect)?,
                    leading_precision,
                    fractional_seconds_precision
                )
            }
            Value::Interval {
                value,
                leading_field,
                leading_precision,
                last_field,
                fractional_seconds_precision,
            } => {
                write!(f, "INTERVAL {}", value.sql(dialect)?)?;
                if let Some(leading_field) = leading_field {
                    write!(f, " {}", leading_field.sql(dialect)?)?;
                }
                if let Some(leading_precision) = leading_precision {
                    write!(f, " ({})", leading_precision)?;
                }
                if let Some(last_field) = last_field {
                    write!(f, " TO {}", last_field.sql(dialect)?)?;
                }
                if let Some(fractional_seconds_precision) = fractional_seconds_precision {
                    write!(f, " ({})", fractional_seconds_precision)?;
                }
                Ok(())
            }
            Value::Null => write!(f, "NULL"),
            Value::Placeholder(v) => write!(f, "{}", v),
        }?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum DateTimeField {
    Year,
    Month,
    Week,
    Day,
    Hour,
    Minute,
    Second,
    Century,
    Decade,
    Dow,
    Doy,
    Epoch,
    Isodow,
    Isoyear,
    Julian,
    Microseconds,
    Millenium,
    Milliseconds,
    Quarter,
    Timezone,
    TimezoneHour,
    TimezoneMinute,
}

impl DialectDisplay for DateTimeField {
    fn fmt(&self, f: &mut (dyn fmt::Write), _dialect: &Dialect) -> Result<(), SqlGenError> {
        Ok(f.write_str(match self {
            DateTimeField::Year => "YEAR",
            DateTimeField::Month => "MONTH",
            DateTimeField::Week => "WEEK",
            DateTimeField::Day => "DAY",
            DateTimeField::Hour => "HOUR",
            DateTimeField::Minute => "MINUTE",
            DateTimeField::Second => "SECOND",
            DateTimeField::Century => "CENTURY",
            DateTimeField::Decade => "DECADE",
            DateTimeField::Dow => "DOW",
            DateTimeField::Doy => "DOY",
            DateTimeField::Epoch => "EPOCH",
            DateTimeField::Isodow => "ISODOW",
            DateTimeField::Isoyear => "ISOYEAR",
            DateTimeField::Julian => "JULIAN",
            DateTimeField::Microseconds => "MICROSECONDS",
            DateTimeField::Millenium => "MILLENIUM",
            DateTimeField::Milliseconds => "MILLISECONDS",
            DateTimeField::Quarter => "QUARTER",
            DateTimeField::Timezone => "TIMEZONE",
            DateTimeField::TimezoneHour => "TIMEZONE_HOUR",
            DateTimeField::TimezoneMinute => "TIMEZONE_MINUTE",
        })?)
    }
}

pub struct EscapeQuotedString<'a> {
    string: &'a str,
    quote: char,
}

impl<'a> DialectDisplay for EscapeQuotedString<'a> {
    fn fmt(&self, f: &mut (dyn fmt::Write), _dialect: &Dialect) -> Result<(), SqlGenError> {
        for c in self.string.chars() {
            if c == self.quote {
                write!(f, "{q}{q}", q = self.quote)?;
            } else {
                write!(f, "{}", c)?;
            }
        }
        Ok(())
    }
}

pub fn escape_quoted_string(string: &str, quote: char) -> EscapeQuotedString<'_> {
    EscapeQuotedString { string, quote }
}

pub fn escape_single_quote_string(s: &str) -> EscapeQuotedString<'_> {
    escape_quoted_string(s, '\'')
}

pub struct EscapeEscapedStringLiteral<'a>(&'a str);

impl<'a> DialectDisplay for EscapeEscapedStringLiteral<'a> {
    fn fmt(&self, f: &mut (dyn fmt::Write), _dialect: &Dialect) -> Result<(), SqlGenError> {
        for c in self.0.chars() {
            match c {
                '\'' => {
                    write!(f, r#"\'"#)?;
                }
                '\\' => {
                    write!(f, r#"\\"#)?;
                }
                '\n' => {
                    write!(f, r#"\n"#)?;
                }
                '\t' => {
                    write!(f, r#"\t"#)?;
                }
                '\r' => {
                    write!(f, r#"\r"#)?;
                }
                _ => {
                    write!(f, "{}", c)?;
                }
            }
        }
        Ok(())
    }
}

pub fn escape_escaped_string(s: &str) -> EscapeEscapedStringLiteral<'_> {
    EscapeEscapedStringLiteral(s)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum TrimWhereField {
    Both,
    Leading,
    Trailing,
}

impl DialectDisplay for TrimWhereField {
    fn fmt(&self, f: &mut (dyn fmt::Write), _dialect: &Dialect) -> Result<(), SqlGenError> {
        use TrimWhereField::*;
        Ok(f.write_str(match self {
            Both => "BOTH",
            Leading => "LEADING",
            Trailing => "TRAILING",
        })?)
    }
}
