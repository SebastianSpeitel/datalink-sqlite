use std::{
    fmt::{Debug, Display, Write},
    marker::PhantomData,
};

use datalink::{
    links::Links,
    query::{DataSelector, LinkSelector, Query, TextSelector},
};
use rusqlite::{Row, ToSql};

use crate::storeddata::StoredData;
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

impl TryFrom<&Query> for SQLBuilder {
    type Error = Error;

    #[inline]
    fn try_from(query: &Query) -> Result<Self, Self::Error> {
        let mut sql = Self::default();
        query.build_sql(&mut sql)?;
        Ok(sql)
    }
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
    pub fn select(&mut self, select: &str) {
        if select.is_empty() {
            return;
        }
        if !self.select.is_empty() {
            self.select.push_str(", ");
        }
        self.select.push_str(select);
    }
    #[inline]
    pub fn from(&mut self, from: &str) {
        if from.is_empty() {
            return;
        }
        if !self.from.is_empty() {
            self.from.push_str(", ");
        }
        self.from.push_str(from);
    }
    #[inline]
    pub fn wher(&mut self, wher: &str) {
        if wher.is_empty() {
            return;
        }
        if !self.wher.is_empty() {
            self.wher.push_str(O::op());
        }
        self.wher.push_str(wher);
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

impl SqlFragment for Query {
    type Context = ();

    #[inline]
    fn build_sql(&self, sql: &mut SQLBuilder<Self::Context, impl Operator>) -> Result {
        sql.select("`links`.`key_id` as `key`");
        sql.select("`links`.`target_id` as `target`");
        sql.from("`links`");
        let mut selector_sql =
            SQLBuilder::new_conjunct((String::from("key"), String::from("target")));
        self.selector().build_sql(&mut selector_sql)?;
        sql.extend(selector_sql);
        Ok(())
    }
}

impl SqlFragment for LinkSelector {
    type Context = (String, String);

    #[inline]
    fn build_sql(&self, sql: &mut SQLBuilder<Self::Context, impl Operator>) -> Result {
        use LinkSelector as E;
        match self {
            E::Any => sql.wher("1"),
            E::None => sql.wher("0"),
            E::Key(s) => {
                let mut inner_sql = SQLBuilder::new_conjunct(sql.context().0.to_owned());
                s.build_sql(&mut inner_sql)?;
                sql.extend(inner_sql);
            }
            E::Target(s) => {
                let mut inner_sql = SQLBuilder::new_conjunct(sql.context().1.to_owned());
                s.build_sql(&mut inner_sql)?;
                sql.extend(inner_sql);
            }
            E::And(and) => {
                for s in and {
                    s.build_sql(sql)?;
                }
            }
            E::Or(or) => {
                let mut inner_sql = SQLBuilder::new_disjunct(sql.context().to_owned());
                for s in or {
                    s.build_sql(&mut inner_sql)?;
                }
                sql.extend(inner_sql);
            }
            _ => return Err(Error::InvalidQuery),
        }
        Ok(())
    }
}

impl SqlFragment for DataSelector {
    type Context = String;
    #[inline]
    fn build_sql(&self, sql: &mut SQLBuilder<Self::Context, impl Operator>) -> Result {
        use DataSelector as E;
        match self {
            E::Any => sql.wher("1"),
            E::None => sql.wher("0"),
            E::Id(id) => {
                sql.wher(&format!("`{}` == ?", sql.context()));
                sql.with(id.to_string());
            }
            E::NotId(id) => {
                sql.wher(&format!("`{}` != ?", sql.context()));
                sql.with(id.to_string());
            }
            E::Not(s) => {
                let mut inner_sql = SQLBuilder::new_conjunct(sql.context());
                s.build_sql(&mut inner_sql)?;
                sql.select(&inner_sql.select);
                sql.from(&inner_sql.from);
                if !inner_sql.wher.is_empty() {
                    sql.wher(&format!("NOT ({})", inner_sql.wher));
                }
                sql.params.extend(inner_sql.params);
            }
            E::And(and) => {
                for s in and {
                    s.build_sql(sql)?;
                }
            }
            E::Or(or) => {
                let mut inner_sql = SQLBuilder::new_disjunct(sql.context());
                for s in or {
                    s.build_sql(&mut inner_sql)?;
                }
                sql.extend(inner_sql);
            }
            E::Text(s) => {
                s.build_sql(sql)?;
            }
            E::Linked(s) => {
                let tbl = format!("{}_l", sql.context().replace('.', "_"));
                let key_col = format!("{tbl}_k");
                let target_col = format!("{tbl}_t");
                let mut inner_sql = SQLBuilder::<(String, String)>::new_conjunct((
                    key_col.to_owned(),
                    target_col.to_owned(),
                ));
                inner_sql.select(&format!("`{tbl}`.`key_id` as `{key_col}`"));
                inner_sql.select(&format!("`{tbl}`.`target_id` as `{target_col}`"));
                inner_sql.from(&format!("`links` as `{tbl}`"));
                inner_sql.wher(&format!("`{tbl}`.`source_id` == `{}`", sql.context()));
                s.build_sql(&mut inner_sql)?;

                sql.wher(&format!("EXISTS ({inner_sql})"));
                sql.params.extend(inner_sql.params);
            }
            _ => return Err(Error::InvalidQuery),
        }
        Ok(())
    }
}

impl SqlFragment for TextSelector {
    type Context = String;
    #[inline]
    fn build_sql(&self, sql: &mut SQLBuilder<Self::Context, impl Operator>) -> Result {
        let tbl = format!("{}_v", sql.context().replace('.', "_"));
        let mut inner_sql = SQLBuilder::<String>::new_conjunct(sql.context());
        inner_sql.from(&format!("`values` as `{tbl}`"));
        inner_sql.wher(&format!("`{tbl}`.`id` == `{}`", sql.context()));

        {
            let Self { search } = self;
            inner_sql.wher(&format!("`{tbl}`.`str` LIKE ?"));
            inner_sql.with(search.to_owned());
        }

        sql.wher(&format!("EXISTS ({inner_sql})"));
        sql.params.extend(inner_sql.params);
        Ok(())
    }
}

#[inline]
pub fn build_link(links: &mut dyn Links, row: &Row, db: Database) {
    let key_id = row.get_ref("key").unwrap().as_str();
    let target_id = row.get_ref("target").unwrap().as_str().unwrap();

    let target = StoredData {
        db: db.clone(),
        id: target_id.parse().unwrap(),
    };

    if let Ok(key) = key_id {
        let key = StoredData {
            db,
            id: key.parse().unwrap(),
        };
        links.push_keyed(Box::new(key), Box::new(target)).unwrap();
    } else {
        links.push_unkeyed(Box::new(target)).unwrap();
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

        let sql = SQLBuilder::try_from(&query).unwrap();

        dbg!(&sql);

        let sql = sql.to_string();

        dbg!(sql);

        // assert!(false)
    }
}
