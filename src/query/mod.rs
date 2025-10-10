use std::{fmt, marker::PhantomData};

use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use sqlx::{PgPool, Postgres, QueryBuilder, types::Json};

use crate::Result;

/// Direction for sorting results.
#[derive(Clone, Copy, Debug)]
pub enum SortDirection {
    Asc,
    Desc,
}

impl SortDirection {
    fn as_str(self) -> &'static str {
        match self {
            SortDirection::Asc => "asc",
            SortDirection::Desc => "desc",
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum SortKind {
    Text(SortDirection),
    Numeric(SortDirection),
}

#[derive(Clone, Debug)]
pub(crate) struct SortSpec {
    path: JsonPath,
    kind: SortKind,
}

#[derive(Clone, Debug)]
pub(crate) enum Selection {
    Document,
    Fields(Vec<FieldProjection>),
}

#[derive(Clone, Debug)]
pub(crate) struct FieldProjection {
    alias: String,
    path: JsonPath,
}

impl FieldProjection {
    fn new(alias: impl Into<String>, path: impl Into<JsonPath>) -> Self {
        Self {
            alias: alias.into(),
            path: path.into(),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct QuerySpec {
    selection: Selection,
    filters: Vec<Predicate>,
    sort: Vec<SortSpec>,
    limit: Option<i64>,
    offset: Option<i64>,
    include_deleted: bool,
}

impl Default for QuerySpec {
    fn default() -> Self {
        Self {
            selection: Selection::Document,
            filters: Vec::new(),
            sort: Vec::new(),
            limit: None,
            offset: None,
            include_deleted: false,
        }
    }
}

impl QuerySpec {
    pub(crate) fn push_filter(&mut self, predicate: Predicate) {
        self.filters.push(predicate);
    }

    pub(crate) fn filters(&self) -> &[Predicate] {
        &self.filters
    }

    pub(crate) fn push_sort(&mut self, spec: SortSpec) {
        self.sort.push(spec);
    }

    pub(crate) fn sort(&self) -> &[SortSpec] {
        &self.sort
    }

    pub(crate) fn set_selection(&mut self, selection: Selection) {
        self.selection = selection;
    }

    pub(crate) fn selection(&self) -> &Selection {
        &self.selection
    }

    pub(crate) fn selection_mut(&mut self) -> &mut Selection {
        &mut self.selection
    }

    pub(crate) fn set_limit(&mut self, limit: Option<i64>) {
        self.limit = limit;
    }

    pub(crate) fn limit(&self) -> Option<i64> {
        self.limit
    }

    pub(crate) fn set_offset(&mut self, offset: Option<i64>) {
        self.offset = offset;
    }

    pub(crate) fn offset(&self) -> Option<i64> {
        self.offset
    }

    pub(crate) fn build_query(self, pool: PgPool) -> (PgPool, QueryBuilder<'static, Postgres>) {
        let QuerySpec {
            selection,
            filters,
            sort,
            limit,
            offset,
            include_deleted,
        } = self;

        let mut builder = QueryBuilder::new("select ");

        match selection {
            Selection::Document => {
                builder.push("doc");
            }
            Selection::Fields(fields) => {
                if fields.is_empty() {
                    builder.push("doc");
                } else {
                    builder.push("jsonb_build_object(");
                    let mut first = true;
                    for field in fields {
                        if !first {
                            builder.push(", ");
                        }
                        first = false;
                        builder.push_bind(field.alias);
                        builder.push(", ");
                        push_json_expr(&mut builder, &field.path);
                    }
                    builder.push(") as doc");
                }
            }
        }

        builder.push(" from docs");

        let mut has_where = false;
        if !filters.is_empty() {
            builder.push(" where ");
            has_where = true;
            let mut iter = filters.into_iter();
            if let Some(first) = iter.next() {
                first.push_sql(&mut builder);
            }
            for predicate in iter {
                builder.push(" and ");
                predicate.push_sql(&mut builder);
            }
        }
        if !include_deleted {
            builder.push(if has_where { " and " } else { " where " });
            builder.push("deleted_at is null");
            has_where = true;
        }

        if !sort.is_empty() {
            builder.push(" order by ");
            let mut first = true;
            for spec in sort {
                if !first {
                    builder.push(", ");
                }
                first = false;
                match spec.kind {
                    SortKind::Text(direction) => {
                        push_text_expr(&mut builder, &spec.path);
                        builder.push(" ");
                        builder.push(direction.as_str());
                    }
                    SortKind::Numeric(direction) => {
                        builder.push("((");
                        push_text_expr(&mut builder, &spec.path);
                        builder.push(")::numeric) ");
                        builder.push(direction.as_str());
                    }
                }
            }
        }

        if let Some(limit) = limit {
            builder.push(" limit ");
            builder.push_bind(limit);
        }

        if let Some(offset) = offset {
            builder.push(" offset ");
            builder.push_bind(offset);
        }

        (pool, builder)
    }
}

#[derive(Debug, Default)]
pub struct DocumentQueryContext {
    spec: QuerySpec,
}

impl DocumentQueryContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn filter(&mut self, predicate: Predicate) -> &mut Self {
        self.spec.push_filter(predicate);
        self
    }

    pub fn filter_if(
        &mut self,
        condition: bool,
        predicate: impl FnOnce() -> Predicate,
    ) -> &mut Self {
        if condition {
            self.spec.push_filter(predicate());
        }
        self
    }

    pub fn order_by(&mut self, path: impl Into<JsonPath>, direction: SortDirection) -> &mut Self {
        self.spec.push_sort(SortSpec {
            path: path.into(),
            kind: SortKind::Text(direction),
        });
        self
    }

    pub fn order_by_number(
        &mut self,
        path: impl Into<JsonPath>,
        direction: SortDirection,
    ) -> &mut Self {
        self.spec.push_sort(SortSpec {
            path: path.into(),
            kind: SortKind::Numeric(direction),
        });
        self
    }

    pub fn limit(&mut self, limit: i64) -> &mut Self {
        self.spec.set_limit(Some(limit.max(0)));
        self
    }

    pub fn offset(&mut self, offset: i64) -> &mut Self {
        self.spec.set_offset(Some(offset.max(0)));
        self
    }

    pub fn page(&mut self, page: u32, per_page: u32) -> &mut Self {
        let per_page = per_page.max(1);
        let page = page.max(1);
        let offset = (page - 1) as i64 * per_page as i64;
        self.spec.set_limit(Some(per_page as i64));
        self.spec.set_offset(Some(offset));
        self
    }

    pub fn include_deleted(&mut self) -> &mut Self {
        self.spec.include_deleted = true;
        self
    }

    pub fn only_deleted(&mut self) -> &mut Self {
        self.spec.include_deleted = true;
        self.spec.push_filter(Predicate::exists("deleted_at"));
        self
    }

    pub fn select_fields(&mut self, fields: &[(&str, &str)]) -> &mut Self {
        let projections = fields
            .iter()
            .map(|(alias, path)| FieldProjection::new(*alias, *path))
            .collect::<Vec<_>>();

        if projections.is_empty() {
            self.spec.set_selection(Selection::Document);
        } else {
            self.spec.set_selection(Selection::Fields(projections));
        }

        self
    }

    pub fn select_field(&mut self, alias: &str, path: &str) -> &mut Self {
        let projection = FieldProjection::new(alias, path);
        match self.spec.selection_mut() {
            Selection::Document => self.spec.set_selection(Selection::Fields(vec![projection])),
            Selection::Fields(fields) => fields.push(projection),
        }

        self
    }

    pub(crate) fn into_spec(self) -> QuerySpec {
        self.spec
    }
}

pub trait CompiledQuery<R>
where
    R: DeserializeOwned,
{
    fn configure(&self, ctx: &mut DocumentQueryContext);
}

/// A JSON path expressed as segments compatible with Postgres' `#>` operator.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonPath(Vec<String>);

impl JsonPath {
    fn parse_segmented(input: &str) -> Vec<String> {
        let mut segments = Vec::new();
        let mut buffer = String::new();
        let mut chars = input.chars().peekable();

        while let Some(ch) = chars.next() {
            match ch {
                '.' => {
                    if !buffer.is_empty() {
                        segments.push(std::mem::take(&mut buffer));
                    }
                }
                '[' => {
                    if !buffer.is_empty() {
                        segments.push(std::mem::take(&mut buffer));
                    }
                    let mut index = String::new();
                    for next in chars.by_ref() {
                        if next == ']' {
                            break;
                        }
                        index.push(next);
                    }
                    if !index.is_empty() {
                        segments.push(index);
                    }
                }
                _ => buffer.push(ch),
            }
        }

        if !buffer.is_empty() {
            segments.push(buffer);
        }

        segments
    }

    fn parts(&self) -> &[String] {
        &self.0
    }
}

impl From<&str> for JsonPath {
    fn from(value: &str) -> Self {
        Self(Self::parse_segmented(value))
    }
}

impl From<String> for JsonPath {
    fn from(value: String) -> Self {
        Self::from(value.as_str())
    }
}

impl From<Vec<String>> for JsonPath {
    fn from(value: Vec<String>) -> Self {
        Self(value)
    }
}

impl<'a> From<&'a [&'a str]> for JsonPath {
    fn from(value: &'a [&'a str]) -> Self {
        Self(value.iter().map(|segment| segment.to_string()).collect())
    }
}

/// JSONB predicate builder for document queries.
#[derive(Clone, Debug)]
pub enum Predicate {
    Eq { path: JsonPath, value: Value },
    Ne { path: JsonPath, value: Value },
    Gt { path: JsonPath, value: f64 },
    Ge { path: JsonPath, value: f64 },
    Lt { path: JsonPath, value: f64 },
    Le { path: JsonPath, value: f64 },
    Contains { path: JsonPath, value: Value },
    In { path: JsonPath, values: Vec<Value> },
    Exists(JsonPath),
    Not(Box<Predicate>),
    And(Vec<Predicate>),
    Or(Vec<Predicate>),
}

impl Predicate {
    fn to_value<T>(value: T) -> Value
    where
        T: Serialize,
    {
        serde_json::to_value(value).expect("serializable value")
    }

    pub fn eq(path: impl Into<JsonPath>, value: impl Serialize) -> Self {
        Self::Eq {
            path: path.into(),
            value: Self::to_value(value),
        }
    }

    pub fn ne(path: impl Into<JsonPath>, value: impl Serialize) -> Self {
        Self::Ne {
            path: path.into(),
            value: Self::to_value(value),
        }
    }

    pub fn gt(path: impl Into<JsonPath>, value: f64) -> Self {
        Self::Gt {
            path: path.into(),
            value,
        }
    }

    pub fn ge(path: impl Into<JsonPath>, value: f64) -> Self {
        Self::Ge {
            path: path.into(),
            value,
        }
    }

    pub fn lt(path: impl Into<JsonPath>, value: f64) -> Self {
        Self::Lt {
            path: path.into(),
            value,
        }
    }

    pub fn le(path: impl Into<JsonPath>, value: f64) -> Self {
        Self::Le {
            path: path.into(),
            value,
        }
    }

    pub fn contains(path: impl Into<JsonPath>, value: impl Serialize) -> Self {
        Self::Contains {
            path: path.into(),
            value: Self::to_value(value),
        }
    }

    pub fn r#in<I, V>(path: impl Into<JsonPath>, values: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: Serialize,
    {
        Self::In {
            path: path.into(),
            values: values.into_iter().map(Self::to_value).collect(),
        }
    }

    pub fn exists(path: impl Into<JsonPath>) -> Self {
        Self::Exists(path.into())
    }

    pub fn negate(predicate: Predicate) -> Self {
        Self::Not(Box::new(predicate))
    }

    pub fn and(predicates: Vec<Predicate>) -> Self {
        Self::And(predicates)
    }

    pub fn or(predicates: Vec<Predicate>) -> Self {
        Self::Or(predicates)
    }

    fn push_sql(&self, builder: &mut QueryBuilder<'_, Postgres>) {
        match self {
            Predicate::Eq { path, value } => {
                builder.push("(");
                push_json_expr(builder, path);
                builder.push(" = ");
                builder.push_bind(Json(value.clone()));
                builder.push(")");
            }
            Predicate::Ne { path, value } => {
                builder.push("(");
                push_json_expr(builder, path);
                builder.push(" <> ");
                builder.push_bind(Json(value.clone()));
                builder.push(")");
            }
            Predicate::Gt { path, value } => push_numeric_cmp(builder, path, *value, ">"),
            Predicate::Ge { path, value } => push_numeric_cmp(builder, path, *value, ">="),
            Predicate::Lt { path, value } => push_numeric_cmp(builder, path, *value, "<"),
            Predicate::Le { path, value } => push_numeric_cmp(builder, path, *value, "<="),
            Predicate::Contains { path, value } => {
                builder.push("(");
                push_json_expr(builder, path);
                builder.push(" @> ");
                builder.push_bind(Json(value.clone()));
                builder.push(")");
            }
            Predicate::In { path, values } => {
                if values.is_empty() {
                    builder.push("false");
                } else {
                    builder.push("(");
                    push_json_expr(builder, path);
                    builder.push(" in (");
                    let mut separated = builder.separated(", ");
                    for value in values {
                        separated.push_bind(Json(value.clone()));
                    }
                    builder.push(")");
                    builder.push(")");
                }
            }
            Predicate::Exists(path) => {
                builder.push("(");
                push_json_expr(builder, path);
                builder.push(" is not null)");
            }
            Predicate::Not(inner) => {
                builder.push("not (");
                inner.push_sql(builder);
                builder.push(")");
            }
            Predicate::And(predicates) => {
                if predicates.is_empty() {
                    builder.push("true");
                } else {
                    builder.push("(");
                    let mut iter = predicates.iter();
                    if let Some(first) = iter.next() {
                        first.push_sql(builder);
                    }
                    for predicate in iter {
                        builder.push(" and ");
                        predicate.push_sql(builder);
                    }
                    builder.push(")");
                }
            }
            Predicate::Or(predicates) => {
                if predicates.is_empty() {
                    builder.push("false");
                } else {
                    builder.push("(");
                    let mut iter = predicates.iter();
                    if let Some(first) = iter.next() {
                        first.push_sql(builder);
                    }
                    for predicate in iter {
                        builder.push(" or ");
                        predicate.push_sql(builder);
                    }
                    builder.push(")");
                }
            }
        }
    }
}

fn push_json_expr(builder: &mut QueryBuilder<'_, Postgres>, path: &JsonPath) {
    builder.push("doc #> ");
    builder.push_bind(path.parts().to_vec());
}

fn push_text_expr(builder: &mut QueryBuilder<'_, Postgres>, path: &JsonPath) {
    builder.push("doc #>> ");
    builder.push_bind(path.parts().to_vec());
}

fn push_numeric_cmp(
    builder: &mut QueryBuilder<'_, Postgres>,
    path: &JsonPath,
    value: f64,
    op: &str,
) {
    builder.push("((");
    push_text_expr(builder, path);
    builder.push(")::numeric ");
    builder.push(op);
    builder.push(" ");
    builder.push_bind(value);
    builder.push(")");
}

/// Builder for queryable document projections.
pub struct DocumentQuery<T> {
    pool: PgPool,
    spec: QuerySpec,
    _marker: PhantomData<T>,
}

impl<T> fmt::Debug for DocumentQuery<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DocumentQuery")
            .field("selection", self.spec.selection())
            .field("filters", &self.spec.filters())
            .field("sort", &self.spec.sort())
            .field("limit", &self.spec.limit())
            .field("offset", &self.spec.offset())
            .finish()
    }
}

impl<T> DocumentQuery<T> {
    pub(crate) fn new(pool: PgPool) -> Self {
        Self {
            pool,
            spec: QuerySpec::default(),
            _marker: PhantomData,
        }
    }

    pub fn filter(mut self, predicate: Predicate) -> Self {
        self.spec.push_filter(predicate);
        self
    }

    pub fn filter_if(mut self, condition: bool, predicate: impl FnOnce() -> Predicate) -> Self {
        if condition {
            self.spec.push_filter(predicate());
        }
        self
    }

    pub fn order_by(mut self, path: impl Into<JsonPath>, direction: SortDirection) -> Self {
        self.spec.push_sort(SortSpec {
            path: path.into(),
            kind: SortKind::Text(direction),
        });
        self
    }

    pub fn order_by_number(mut self, path: impl Into<JsonPath>, direction: SortDirection) -> Self {
        self.spec.push_sort(SortSpec {
            path: path.into(),
            kind: SortKind::Numeric(direction),
        });
        self
    }

    pub fn limit(mut self, limit: i64) -> Self {
        self.spec.set_limit(Some(limit.max(0)));
        self
    }

    pub fn offset(mut self, offset: i64) -> Self {
        self.spec.set_offset(Some(offset.max(0)));
        self
    }

    pub fn page(mut self, page: u32, per_page: u32) -> Self {
        let per_page = per_page.max(1);
        let page = page.max(1);
        let offset = (page - 1) as i64 * per_page as i64;
        self.spec.set_limit(Some(per_page as i64));
        self.spec.set_offset(Some(offset));
        self
    }

    pub fn include_deleted(mut self) -> Self {
        self.spec.include_deleted = true;
        self
    }

    pub fn only_deleted(mut self) -> Self {
        self.spec.include_deleted = true;
        self.spec.push_filter(Predicate::exists("deleted_at"));
        self
    }

    pub fn select_fields(mut self, fields: &[(&str, &str)]) -> Self {
        let projections = fields
            .iter()
            .map(|(alias, path)| FieldProjection::new(*alias, *path))
            .collect::<Vec<_>>();

        if projections.is_empty() {
            self.spec.set_selection(Selection::Document);
        } else {
            self.spec.set_selection(Selection::Fields(projections));
        }

        self
    }

    pub fn select_field(mut self, alias: &str, path: &str) -> Self {
        let projection = FieldProjection::new(alias, path);
        match self.spec.selection_mut() {
            Selection::Document => self.spec.set_selection(Selection::Fields(vec![projection])),
            Selection::Fields(fields) => fields.push(projection),
        }

        self
    }

    fn build_query(self) -> (PgPool, QueryBuilder<'static, Postgres>) {
        let pool = self.pool.clone();
        self.spec.build_query(pool)
    }
}

impl<T> DocumentQuery<T>
where
    T: DeserializeOwned,
{
    pub async fn fetch_all(self) -> Result<Vec<T>> {
        let (pool, mut builder) = self.build_query();
        let query = builder.build_query_as::<(Value,)>();
        let rows = query.fetch_all(&pool).await?;
        rows.into_iter()
            .map(|(value,)| serde_json::from_value(value).map_err(Into::into))
            .collect()
    }

    pub async fn fetch_optional(self) -> Result<Option<T>> {
        let (pool, mut builder) = self.build_query();
        let query = builder.build_query_as::<(Value,)>();
        let row = query.fetch_optional(&pool).await?;
        match row {
            Some((value,)) => Ok(Some(serde_json::from_value(value)?)),
            None => Ok(None),
        }
    }

    pub async fn fetch_one(self) -> Result<T> {
        let (pool, mut builder) = self.build_query();
        let query = builder.build_query_as::<(Value,)>();
        let (value,) = query.fetch_one(&pool).await?;
        Ok(serde_json::from_value(value)?)
    }

    pub async fn fetch_first(self) -> Result<Option<T>> {
        self.limit(1).fetch_optional().await
    }
}
