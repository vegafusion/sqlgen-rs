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
use alloc::{boxed::Box, vec::Vec};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::fmt::Write;

use crate::ast::*;
use crate::dialect::{Dialect, DialectDisplay};

/// The most complete variant of a `SELECT` query expression, optionally
/// including `WITH`, `UNION` / other set operations, and `ORDER BY`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Query {
    /// WITH (common table expressions, or CTEs)
    pub with: Option<With>,
    /// SELECT or UNION / EXCEPT / INTERSECT
    pub body: Box<SetExpr>,
    /// ORDER BY
    pub order_by: Vec<OrderByExpr>,
    /// `LIMIT { <N> | ALL }`
    pub limit: Option<Expr>,
    /// `OFFSET <N> [ { ROW | ROWS } ]`
    pub offset: Option<Offset>,
    /// `FETCH { FIRST | NEXT } <N> [ PERCENT ] { ROW | ROWS } | { ONLY | WITH TIES }`
    pub fetch: Option<Fetch>,
    /// `FOR { UPDATE | SHARE }`
    pub lock: Option<LockType>,
}

impl DialectDisplay for Query {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> Result<(), SqlGenError> {
        if let Some(ref with) = self.with {
            write!(f, "{} ", with.sql(dialect)?)?;
        }
        write!(f, "{}", self.body.sql(dialect)?)?;
        if !self.order_by.is_empty() {
            write!(
                f,
                " ORDER BY {}",
                display_comma_separated(&self.order_by).sql(dialect)?
            )?;
        }
        if let Some(ref limit) = self.limit {
            write!(f, " LIMIT {}", limit.sql(dialect)?)?;
        }
        if let Some(ref offset) = self.offset {
            write!(f, " {}", offset.sql(dialect)?)?;
        }
        if let Some(ref fetch) = self.fetch {
            write!(f, " {}", fetch.sql(dialect)?)?;
        }
        if let Some(ref lock) = self.lock {
            write!(f, " {}", lock.sql(dialect)?)?;
        }
        Ok(())
    }
}

/// A node in a tree, representing a "query body" expression, roughly:
/// `SELECT ... [ {UNION|EXCEPT|INTERSECT} SELECT ...]`
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum SetExpr {
    /// Restricted SELECT .. FROM .. HAVING (no ORDER BY or set operations)
    Select(Box<Select>),
    /// Parenthesized SELECT subquery, which may include more set operations
    /// in its body and an optional ORDER BY / LIMIT.
    Query(Box<Query>),
    /// UNION/EXCEPT/INTERSECT of two queries
    SetOperation {
        op: SetOperator,
        all: bool,
        left: Box<SetExpr>,
        right: Box<SetExpr>,
    },
    Values(Values),
}

impl DialectDisplay for SetExpr {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> Result<(), SqlGenError> {
        match self {
            SetExpr::Select(s) => Ok(write!(f, "{}", s.sql(dialect)?)?),
            SetExpr::Query(q) => Ok(write!(f, "({})", q.sql(dialect)?)?),
            SetExpr::Values(v) => Ok(write!(f, "{}", v.sql(dialect)?)?),
            SetExpr::SetOperation {
                left,
                right,
                op,
                all,
            } => {
                let all_str = if *all { " ALL" } else { "" };
                Ok(write!(
                    f,
                    "{} {}{} {}",
                    left.sql(dialect)?,
                    op.sql(dialect)?,
                    all_str,
                    right.sql(dialect)?,
                )?)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum SetOperator {
    Union,
    Except,
    Intersect,
}

impl DialectDisplay for SetOperator {
    fn fmt(&self, f: &mut (dyn fmt::Write), _dialect: &Dialect) -> Result<(), SqlGenError> {
        Ok(f.write_str(match self {
            SetOperator::Union => "UNION",
            SetOperator::Except => "EXCEPT",
            SetOperator::Intersect => "INTERSECT",
        })?)
    }
}

/// A restricted variant of `SELECT` (without CTEs/`ORDER BY`), which may
/// appear either as the only body item of a `Query`, or as an operand
/// to a set operation like `UNION`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Select {
    pub distinct: bool,
    /// MSSQL syntax: `TOP (<N>) [ PERCENT ] [ WITH TIES ]`
    pub top: Option<Top>,
    /// projection expressions
    pub projection: Vec<SelectItem>,
    /// INTO
    pub into: Option<SelectInto>,
    /// FROM
    pub from: Vec<TableWithJoins>,
    /// LATERAL VIEWs
    pub lateral_views: Vec<LateralView>,
    /// WHERE
    pub selection: Option<Expr>,
    /// GROUP BY
    pub group_by: Vec<Expr>,
    /// HAVING
    pub having: Option<Expr>,
}

impl DialectDisplay for Select {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> Result<(), SqlGenError> {
        write!(f, "SELECT{}", if self.distinct { " DISTINCT" } else { "" })?;
        if let Some(ref top) = self.top {
            write!(f, " {}", top.sql(dialect)?)?;
        }
        write!(
            f,
            " {}",
            display_comma_separated(&self.projection).sql(dialect)?
        )?;

        if let Some(ref into) = self.into {
            write!(f, " {}", into.sql(dialect)?)?;
        }

        if !self.from.is_empty() {
            write!(
                f,
                " FROM {}",
                display_comma_separated(&self.from).sql(dialect)?
            )?;
        }
        if !self.lateral_views.is_empty() {
            for lv in &self.lateral_views {
                write!(f, "{}", lv.sql(dialect)?)?;
            }
        }
        if let Some(ref selection) = self.selection {
            write!(f, " WHERE {}", selection.sql(dialect)?)?;
        }
        if !self.group_by.is_empty() {
            write!(
                f,
                " GROUP BY {}",
                display_comma_separated(&self.group_by).sql(dialect)?
            )?;
        }
        if let Some(ref having) = self.having {
            write!(f, " HAVING {}", having.sql(dialect)?)?;
        }
        Ok(())
    }
}

/// A hive LATERAL VIEW with potential column aliases
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct LateralView {
    /// LATERAL VIEW
    pub lateral_view: Expr,
    /// LATERAL VIEW table name
    pub lateral_view_name: ObjectName,
    /// LATERAL VIEW optional column aliases
    pub lateral_col_alias: Vec<Ident>,
    /// LATERAL VIEW OUTER
    pub outer: bool,
}

impl DialectDisplay for LateralView {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> Result<(), SqlGenError> {
        write!(
            f,
            " LATERAL VIEW{outer} {} {}",
            self.lateral_view.sql(dialect)?,
            self.lateral_view_name.sql(dialect)?,
            outer = if self.outer { " OUTER" } else { "" }
        )?;
        if !self.lateral_col_alias.is_empty() {
            write!(
                f,
                " AS {}",
                display_comma_separated(&self.lateral_col_alias).sql(dialect)?
            )?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct With {
    pub recursive: bool,
    pub cte_tables: Vec<Cte>,
}

impl DialectDisplay for With {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> Result<(), SqlGenError> {
        Ok(write!(
            f,
            "WITH {}{}",
            if self.recursive { "RECURSIVE " } else { "" },
            display_comma_separated(&self.cte_tables).sql(dialect)?
        )?)
    }
}

/// A single CTE (used after `WITH`): `alias [(col1, col2, ...)] AS ( query )`
/// The names in the column list before `AS`, when specified, replace the names
/// of the columns returned by the query. The parser does not validate that the
/// number of columns in the query matches the number of columns in the query.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Cte {
    pub alias: TableAlias,
    pub query: Query,
    pub from: Option<Ident>,
}

impl DialectDisplay for Cte {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> Result<(), SqlGenError> {
        write!(
            f,
            "{} AS ({})",
            self.alias.sql(dialect)?,
            self.query.sql(dialect)?
        )?;
        if let Some(ref fr) = self.from {
            write!(f, " FROM {}", fr.sql(dialect)?)?;
        }
        Ok(())
    }
}

/// One item of the comma-separated list following `SELECT`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum SelectItem {
    /// Any expression, not followed by `[ AS ] alias`
    UnnamedExpr(Expr),
    /// An expression, followed by `[ AS ] alias`
    ExprWithAlias { expr: Expr, alias: Ident },
    /// `alias.*` or even `schema.table.*`
    QualifiedWildcard(ObjectName),
    /// An unqualified `*`
    Wildcard,
}

impl DialectDisplay for SelectItem {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> Result<(), SqlGenError> {
        Ok(match &self {
            SelectItem::UnnamedExpr(expr) => write!(f, "{}", expr.sql(dialect)?),
            SelectItem::ExprWithAlias { expr, alias } => {
                write!(f, "{} AS {}", expr.sql(dialect)?, alias.sql(dialect)?)
            }
            SelectItem::QualifiedWildcard(prefix) => write!(f, "{}.*", prefix.sql(dialect)?),
            SelectItem::Wildcard => write!(f, "*"),
        }?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct TableWithJoins {
    pub relation: TableFactor,
    pub joins: Vec<Join>,
}

impl DialectDisplay for TableWithJoins {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> Result<(), SqlGenError> {
        write!(f, "{}", self.relation.sql(dialect)?)?;
        for join in &self.joins {
            write!(f, "{}", join.sql(dialect)?)?;
        }
        Ok(())
    }
}

/// A table name or a parenthesized subquery with an optional alias
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum TableFactor {
    Table {
        name: ObjectName,
        alias: Option<TableAlias>,
        /// Arguments of a table-valued function, as supported by Postgres
        /// and MSSQL. Note that deprecated MSSQL `FROM foo (NOLOCK)` syntax
        /// will also be parsed as `args`.
        ///
        /// This field's value is `Some(v)`, where `v` is a (possibly empty)
        /// vector of arguments, in the case of a table-valued function call,
        /// whereas it's `None` in the case of a regular table name.
        args: Option<Vec<FunctionArg>>,
        /// MSSQL-specific `WITH (...)` hints such as NOLOCK.
        with_hints: Vec<Expr>,
    },
    Derived {
        lateral: bool,
        subquery: Box<Query>,
        alias: Option<TableAlias>,
    },
    /// `TABLE(<expr>)[ AS <alias> ]`
    TableFunction {
        expr: Expr,
        alias: Option<TableAlias>,
    },
    /// SELECT * FROM UNNEST ([10,20,30]) as numbers WITH OFFSET;
    /// +---------+--------+
    /// | numbers | offset |
    /// +---------+--------+
    /// | 10      | 0      |
    /// | 20      | 1      |
    /// | 30      | 2      |
    /// +---------+--------+
    UNNEST {
        alias: Option<TableAlias>,
        array_expr: Box<Expr>,
        with_offset: bool,
        with_offset_alias: Option<Ident>,
    },
    /// Represents a parenthesized table factor. The SQL spec only allows a
    /// join expression (`(foo <JOIN> bar [ <JOIN> baz ... ])`) to be nested,
    /// possibly several times.
    ///
    /// The parser may also accept non-standard nesting of bare tables for some
    /// dialects, but the information about such nesting is stripped from AST.
    NestedJoin(Box<TableWithJoins>),
}

impl DialectDisplay for TableFactor {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> Result<(), SqlGenError> {
        match self {
            TableFactor::Table {
                name,
                alias,
                args,
                with_hints,
            } => {
                write!(f, "{}", name.sql(dialect)?)?;
                if let Some(args) = args {
                    write!(f, "({})", display_comma_separated(args).sql(dialect)?)?;
                }
                if let Some(alias) = alias {
                    write!(f, " AS {}", alias.sql(dialect)?)?;
                }
                if !with_hints.is_empty() {
                    write!(
                        f,
                        " WITH ({})",
                        display_comma_separated(with_hints).sql(dialect)?
                    )?;
                }
                Ok(())
            }
            TableFactor::Derived {
                lateral,
                subquery,
                alias,
            } => {
                if *lateral {
                    write!(f, "LATERAL ")?;
                }
                write!(f, "({})", subquery.sql(dialect)?)?;
                if let Some(alias) = alias {
                    write!(f, " AS {}", alias.sql(dialect)?)?;
                }
                Ok(())
            }
            TableFactor::TableFunction { expr, alias } => {
                write!(f, "TABLE({})", expr.sql(dialect)?)?;
                if let Some(alias) = alias {
                    write!(f, " AS {}", alias.sql(dialect)?)?;
                }
                Ok(())
            }
            TableFactor::UNNEST {
                alias,
                array_expr,
                with_offset,
                with_offset_alias,
            } => {
                write!(f, "UNNEST({})", array_expr.sql(dialect)?)?;
                if let Some(alias) = alias {
                    write!(f, " AS {}", alias.sql(dialect)?)?;
                }
                if *with_offset {
                    write!(f, " WITH OFFSET")?;
                }
                if let Some(alias) = with_offset_alias {
                    write!(f, " AS {}", alias.sql(dialect)?)?;
                }
                Ok(())
            }
            TableFactor::NestedJoin(table_reference) => {
                Ok(write!(f, "({})", table_reference.sql(dialect)?)?)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct TableAlias {
    pub name: Ident,
    pub columns: Vec<Ident>,
}

impl DialectDisplay for TableAlias {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> Result<(), SqlGenError> {
        write!(f, "{}", self.name.sql(dialect)?)?;
        if !self.columns.is_empty() {
            write!(
                f,
                " ({})",
                display_comma_separated(&self.columns).sql(dialect)?
            )?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Join {
    pub relation: TableFactor,
    pub join_operator: JoinOperator,
}

impl DialectDisplay for Join {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> Result<(), SqlGenError> {
        fn prefix(constraint: &JoinConstraint) -> &'static str {
            match constraint {
                JoinConstraint::Natural => "NATURAL ",
                _ => "",
            }
        }
        fn suffix(
            constraint: &'_ JoinConstraint,
            dialect: &Dialect,
        ) -> Result<String, SqlGenError> {
            let mut repr = String::new();
            match constraint {
                JoinConstraint::On(expr) => {
                    write!(repr, " ON {}", expr.sql(dialect)?)?;
                }
                JoinConstraint::Using(attrs) => {
                    write!(
                        repr,
                        " USING({})",
                        display_comma_separated(attrs).sql(dialect)?
                    )?;
                }
                _ => {}
            }
            Ok(repr)
        }
        Ok(match &self.join_operator {
            JoinOperator::Inner(constraint) => write!(
                f,
                " {}JOIN {}{}",
                prefix(constraint),
                self.relation.sql(dialect)?,
                suffix(constraint, dialect)?
            ),
            JoinOperator::LeftOuter(constraint) => write!(
                f,
                " {}LEFT JOIN {}{}",
                prefix(constraint),
                self.relation.sql(dialect)?,
                suffix(constraint, dialect)?
            ),
            JoinOperator::RightOuter(constraint) => write!(
                f,
                " {}RIGHT JOIN {}{}",
                prefix(constraint),
                self.relation.sql(dialect)?,
                suffix(constraint, dialect)?
            ),
            JoinOperator::FullOuter(constraint) => write!(
                f,
                " {}FULL JOIN {}{}",
                prefix(constraint),
                self.relation.sql(dialect)?,
                suffix(constraint, dialect)?
            ),
            JoinOperator::CrossJoin => write!(f, " CROSS JOIN {}", self.relation.sql(dialect)?),
            JoinOperator::CrossApply => write!(f, " CROSS APPLY {}", self.relation.sql(dialect)?),
            JoinOperator::OuterApply => write!(f, " OUTER APPLY {}", self.relation.sql(dialect)?),
        }?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum JoinOperator {
    Inner(JoinConstraint),
    LeftOuter(JoinConstraint),
    RightOuter(JoinConstraint),
    FullOuter(JoinConstraint),
    CrossJoin,
    /// CROSS APPLY (non-standard)
    CrossApply,
    /// OUTER APPLY (non-standard)
    OuterApply,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum JoinConstraint {
    On(Expr),
    Using(Vec<Ident>),
    Natural,
    None,
}

/// An `ORDER BY` expression
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct OrderByExpr {
    pub expr: Expr,
    /// Optional `ASC` or `DESC`
    pub asc: Option<bool>,
    /// Optional `NULLS FIRST` or `NULLS LAST`
    pub nulls_first: Option<bool>,
}

impl DialectDisplay for OrderByExpr {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> Result<(), SqlGenError> {
        write!(f, "{}", self.expr.sql(dialect)?)?;
        match self.asc {
            Some(true) => write!(f, " ASC")?,
            Some(false) => write!(f, " DESC")?,
            None => (),
        }
        match self.nulls_first {
            Some(true) => write!(f, " NULLS FIRST")?,
            Some(false) => write!(f, " NULLS LAST")?,
            None => (),
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Offset {
    pub value: Expr,
    pub rows: OffsetRows,
}

impl DialectDisplay for Offset {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> Result<(), SqlGenError> {
        Ok(write!(
            f,
            "OFFSET {}{}",
            self.value.sql(dialect)?,
            self.rows.sql(dialect)?
        )?)
    }
}

/// Stores the keyword after `OFFSET <number>`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum OffsetRows {
    /// Omitting ROW/ROWS is non-standard MySQL quirk.
    None,
    Row,
    Rows,
}

impl DialectDisplay for OffsetRows {
    fn fmt(&self, f: &mut (dyn fmt::Write), _dialect: &Dialect) -> Result<(), SqlGenError> {
        match self {
            OffsetRows::None => Ok(()),
            OffsetRows::Row => Ok(write!(f, " ROW")?),
            OffsetRows::Rows => Ok(write!(f, " ROWS")?),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Fetch {
    pub with_ties: bool,
    pub percent: bool,
    pub quantity: Option<Expr>,
}

impl DialectDisplay for Fetch {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> Result<(), SqlGenError> {
        let extension = if self.with_ties { "WITH TIES" } else { "ONLY" };
        Ok(if let Some(ref quantity) = self.quantity {
            let percent = if self.percent { " PERCENT" } else { "" };
            write!(
                f,
                "FETCH FIRST {}{} ROWS {}",
                quantity.sql(dialect)?,
                percent,
                extension
            )
        } else {
            write!(f, "FETCH FIRST ROWS {}", extension)
        }?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum LockType {
    Share,
    Update,
}

impl DialectDisplay for LockType {
    fn fmt(&self, f: &mut (dyn fmt::Write), _dialect: &Dialect) -> Result<(), SqlGenError> {
        let select_lock = match self {
            LockType::Share => "FOR SHARE",
            LockType::Update => "FOR UPDATE",
        };
        Ok(write!(f, "{}", select_lock)?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Top {
    /// SQL semantic equivalent of LIMIT but with same structure as FETCH.
    pub with_ties: bool,
    pub percent: bool,
    pub quantity: Option<Expr>,
}

impl DialectDisplay for Top {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> Result<(), SqlGenError> {
        let extension = if self.with_ties { " WITH TIES" } else { "" };
        Ok(if let Some(ref quantity) = self.quantity {
            let percent = if self.percent { " PERCENT" } else { "" };
            write!(
                f,
                "TOP ({}){}{}",
                quantity.sql(dialect)?,
                percent,
                extension
            )
        } else {
            write!(f, "TOP{}", extension)
        }?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Values(pub Vec<Vec<Expr>>);

impl DialectDisplay for Values {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> Result<(), SqlGenError> {
        write!(f, "VALUES ")?;
        let mut delim = "";
        for row in &self.0 {
            write!(f, "{}", delim)?;
            delim = ", ";
            write!(f, "({})", display_comma_separated(row).sql(dialect)?)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct SelectInto {
    pub temporary: bool,
    pub unlogged: bool,
    pub table: bool,
    pub name: ObjectName,
}

impl DialectDisplay for SelectInto {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> Result<(), SqlGenError> {
        let temporary = if self.temporary { " TEMPORARY" } else { "" };
        let unlogged = if self.unlogged { " UNLOGGED" } else { "" };
        let table = if self.table { " TABLE" } else { "" };

        Ok(write!(
            f,
            "INTO{}{}{} {}",
            temporary,
            unlogged,
            table,
            self.name.sql(dialect)?
        )?)
    }
}
