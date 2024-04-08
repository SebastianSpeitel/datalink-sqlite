use std::{
    fmt::{Debug, Display, Write},
    marker::PhantomData,
};

use datalink::{
    links::prelude::*,
    query::{prelude::Text as TextFilter, DataFilter, LinkFilter, Query},
};
use rusqlite::{Row, ToSql};

use crate::{
    database::Database,
    error::{Error, Result},
};

pub trait Operator {
    fn op() -> &'static str;
}

#[derive(Default)]
pub struct Conjunction;
impl Operator for Conjunction {
    #[inline(always)]
    fn op() -> &'static str {
        " AND "
    }
}
#[derive(Default)]
pub struct Disjunction;
impl Operator for Disjunction {
    #[inline(always)]
    fn op() -> &'static str {
        " OR "
    }
}

#[derive(Default)]
pub struct SQLBuilder<C = (), Op: Operator = Conjunction> {
    context: C,
    select: String,
    from: String,
    wher: String,
    params: Vec<Box<dyn ToSql>>,
    op: PhantomData<Op>,
}

impl<C> SQLBuilder<C> {
    #[inline]
    #[must_use]
    pub fn new_conjunct(context: impl Into<C>) -> SQLBuilder<C, Conjunction> {
        SQLBuilder {
            context: context.into(),
            select: String::new(),
            from: String::new(),
            wher: String::new(),
            params: Vec::new(),
            op: PhantomData,
        }
    }

    #[inline]
    #[must_use]
    pub fn new_disjunct(context: impl Into<C>) -> SQLBuilder<C, Disjunction> {
        SQLBuilder {
            context: context.into(),
            select: String::new(),
            from: String::new(),
            wher: String::new(),
            params: Vec::new(),
            op: PhantomData,
        }
    }
}

impl<C, O: Operator> SQLBuilder<C, O> {
    #[inline]
    pub fn context(&self) -> &C {
        &self.context
    }

    #[inline]
    pub fn params(&self) -> rusqlite::ParamsFromIter<&Vec<Box<dyn ToSql>>> {
        rusqlite::params_from_iter(&self.params)
    }

    #[inline]
    pub fn select(&mut self, select: impl AsRef<str>) {
        if select.as_ref().is_empty() {
            return;
        }
        if !self.select.is_empty() {
            self.select.push_str(", ");
        }
        self.select.push_str(select.as_ref());
    }
    #[inline]
    pub fn from(&mut self, from: impl AsRef<str>) {
        if from.as_ref().is_empty() {
            return;
        }
        if !self.from.is_empty() {
            self.from.push_str(", ");
        }
        self.from.push_str(from.as_ref());
    }
    #[inline]
    pub fn wher(&mut self, wher: impl AsRef<str>) {
        if wher.as_ref().is_empty() {
            return;
        }
        if !self.wher.is_empty() {
            self.wher.push_str(O::op());
        }
        self.wher.push_str(wher.as_ref());
    }
    #[inline]
    pub fn with(&mut self, param: (impl ToSql + 'static)) {
        self.params.push(Box::new(param));
    }

    #[inline]
    pub fn extend<C2, O2: Operator>(&mut self, other: SQLBuilder<C2, O2>) {
        self.select(&other.select);
        self.from(&other.from);
        if !other.wher.is_empty() {
            self.wher("(");
            self.wher.push_str(&other.wher);
            self.wher.push(')');
        }
        self.params.extend(other.params);
    }

    #[inline]
    pub fn prepare_cached<'conn>(
        &self,
        conn: &'conn rusqlite::Connection,
    ) -> rusqlite::Result<rusqlite::CachedStatement<'conn>> {
        conn.prepare_cached(&self.to_string())
    }
}

impl<C, O: Operator> Display for SQLBuilder<C, O> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SELECT ")?;
        if self.select.is_empty() {
            f.write_char('1')?;
        } else {
            f.write_str(&self.select)?;
        }
        if !self.from.is_empty() {
            f.write_str(" FROM ")?;
            f.write_str(&self.from)?;
        }
        if !self.wher.is_empty() {
            f.write_str(" WHERE ")?;
            f.write_str(&self.wher)?;
        }
        Ok(())
    }
}

impl<C: Debug, O: Operator> Debug for SQLBuilder<C, O> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("SQLBuilder");
        s.field("select", &self.select);
        s.field("from", &self.from);
        s.field("where", &self.wher);
        s.field("operator", &O::op());
        s.field("context", &self.context);
        let param_cnt = self.params.len();
        s.field("params", &format_args!("[<{param_cnt}>]"));
        s.finish()
    }
}

pub trait SqlFragment {
    type Context;
    fn build_sql(&self, sql: &mut SQLBuilder<Self::Context, impl Operator>) -> Result;
}

#[derive(Debug, Clone)]
pub struct QueryContext {
    pub table: String,
    pub key_col: String,
    pub target_col: String,
}

impl SqlFragment for Query {
    type Context = QueryContext;

    #[inline]
    fn build_sql(&self, sql: &mut SQLBuilder<Self::Context, impl Operator>) -> Result {
        let QueryContext {
            table,
            key_col,
            target_col,
        } = sql.context().to_owned();
        let key = format!("{table}_k");
        let target = format!("{table}_t");
        sql.select(format!("`{table}`.`{key_col}` as `{key}`"));
        sql.select(format!("`{table}`.`{target_col}` as `{target}`"));
        sql.from(format!("`{table}`"));
        let mut selector_sql = SQLBuilder::new_conjunct(LinkContext {
            key_col,
            target_col,
        });
        self.filter().build_sql(&mut selector_sql)?;
        sql.extend(selector_sql);
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct LinkContext {
    pub key_col: String,
    pub target_col: String,
}

impl SqlFragment for LinkFilter {
    type Context = LinkContext;

    #[inline]
    fn build_sql(&self, sql: &mut SQLBuilder<Self::Context, impl Operator>) -> Result {
        use LinkFilter as E;
        match self {
            E::Any => sql.wher("1"),
            E::None => sql.wher("0"),
            E::Key(s) => {
                let mut inner_sql = SQLBuilder::new_conjunct(Column {
                    col: sql.context().key_col.to_owned(),
                });
                s.build_sql(&mut inner_sql)?;
                sql.extend(inner_sql);
            }
            E::Target(s) => {
                let mut inner_sql = SQLBuilder::new_conjunct(Column {
                    col: sql.context().target_col.to_owned(),
                });
                s.build_sql(&mut inner_sql)?;
                sql.extend(inner_sql);
            }
            E::And(and) => {
                for s in and.iter() {
                    s.build_sql(sql)?;
                }
            }
            E::Or(or) => {
                let mut inner_sql = SQLBuilder::new_disjunct(sql.context().to_owned());
                for s in or.iter() {
                    s.build_sql(&mut inner_sql)?;
                }
                sql.extend(inner_sql);
            }
            _ => return Err(Error::InvalidQuery),
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct Column {
    pub col: String,
}

impl SqlFragment for DataFilter {
    type Context = Column;
    #[inline]
    fn build_sql(&self, sql: &mut SQLBuilder<Self::Context, impl Operator>) -> Result {
        use DataFilter as E;
        match self {
            E::Any => sql.wher("1"),
            E::None => sql.wher("0"),
            E::Id(id) => {
                sql.wher(format!("`{}` == ?", sql.context().col));
                sql.with(id.to_string());
            }
            // Stored Data is always unique
            E::Unique => sql.wher("1"),
            E::NotId(id) => {
                sql.wher(format!("`{}` != ?", sql.context().col));
                sql.with(id.to_string());
            }
            E::Not(s) => {
                let mut inner_sql = SQLBuilder::new_conjunct(sql.context().to_owned());

                s.0.build_sql(&mut inner_sql)?;
                sql.select(&inner_sql.select);
                sql.from(&inner_sql.from);
                if !inner_sql.wher.is_empty() {
                    sql.wher(format!("NOT ({})", inner_sql.wher));
                }
                sql.params.extend(inner_sql.params);
            }
            E::And(and) => {
                for s in and.iter() {
                    s.build_sql(sql)?;
                }
            }
            E::Or(or) => {
                let mut inner_sql = SQLBuilder::new_disjunct(sql.context().to_owned());
                for s in or.iter() {
                    s.build_sql(&mut inner_sql)?;
                }
                sql.extend(inner_sql);
            }
            E::Text(s) => {
                s.build_sql(sql)?;
            }
            E::Linked(s) => {
                let tbl = format!("{}_l", sql.context().col.replace('.', "_"));
                let key_col = format!("{tbl}_k");
                let target_col = format!("{tbl}_t");
                let mut inner_sql = SQLBuilder::<LinkContext>::new_conjunct(LinkContext {
                    key_col: key_col.to_owned(),
                    target_col: target_col.to_owned(),
                });
                inner_sql.select(format!("`{tbl}`.`key_uuid` as `{key_col}`"));
                inner_sql.select(format!("`{tbl}`.`target_uuid` as `{target_col}`"));
                inner_sql.from(format!("`links` as `{tbl}`"));
                inner_sql.wher(format!("`{tbl}`.`source_uuid` == `{}`", sql.context().col));
                s.build_sql(&mut inner_sql)?;

                sql.wher(format!("EXISTS ({inner_sql})"));
                sql.params.extend(inner_sql.params);
            }
            _ => return Err(Error::InvalidQuery),
        }
        Ok(())
    }
}

impl SqlFragment for TextFilter {
    type Context = Column;
    #[inline]
    fn build_sql(&self, sql: &mut SQLBuilder<Self::Context, impl Operator>) -> Result {
        let tbl = format!("{}_v", sql.context().col.replace('.', "_"));
        let mut inner_sql = SQLBuilder::<Column>::new_conjunct(sql.context().to_owned());
        inner_sql.from(format!("`values` as `{tbl}`"));
        inner_sql.wher(format!("`{tbl}`.`uuid` == `{}`", sql.context().col));

        {
            if let Some(search) = self.exact() {
                inner_sql.wher(format!("`{tbl}`.`str` LIKE ?"));
                inner_sql.with(search.to_owned());
            } else {
                return Err(Error::InvalidQuery);
            }
        }

        sql.wher(format!("EXISTS ({inner_sql})"));
        sql.params.extend(inner_sql.params);
        Ok(())
    }
}

#[inline]
pub fn build_links<L, C: Debug>(
    db: &Database,
    sql: &SQLBuilder<C>,
    links: &mut (impl Links + ?Sized),
    f: impl Fn(&Row) -> Result<L>,
) -> Result
where
    L: Link,
    L::Key: Sized + 'static,
    L::Target: Sized + 'static,
{
    log::trace!("Building links from: {:?}", &sql);
    let conn = db.conn.lock().unwrap();

    let mut stmt = sql.prepare_cached(&conn)?;

    let mut rows = stmt.query(sql.params())?;

    loop {
        match rows.next()? {
            None => break Ok(()),
            Some(r) => {
                if f(r)?.build_into(links)?.is_break() {
                    break Ok(());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complex() {
        use datalink::query::prelude::*;

        let query = Query::new(
            Link::key(Data::text("foo"))
                & Link::target(Data::text("%") & Data::linked(Link::key(Data::text("created_at")))),
        );
        dbg!(&query);

        let mut sql = SQLBuilder::new_conjunct(QueryContext {
            table: "links".into(),
            key_col: "key_uuid".into(),
            target_col: "target_uuid".into(),
        });
        query.build_sql(&mut sql).unwrap();

        dbg!(&sql);

        let sql = sql.to_string();

        dbg!(sql);

        // assert!(false)
    }
}
