//! Runs the diagnostics package's GraphQL reads in-process.
//!
//! The selection sets are derived from the schema's own SDL rather than written
//! out by hand. A hand-written selection is a second copy of the schema that
//! nobody updates: the field added to `Metrics` for the next stall
//! investigation would be missing from exactly the artefact the investigation
//! needs.
//!
//! The SDL is used rather than an introspection query because introspection is
//! disabled on the running schema outside e2e mode — the shape has to come from
//! a source that is always available, and
//! [`weaver_server_api::export_schema_sdl`] renders the same schema this binary
//! serves without touching any runtime state.
//!
//! Only fields that can be asked for without arguments are selected, the walk
//! stops at a fixed depth and at any type already on its own path, and each
//! root field is executed as its own request so one large surface cannot push
//! another past the schema's complexity limit.

use std::collections::HashMap;
use std::sync::OnceLock;

use async_graphql::parser::types::{BaseType, TypeKind, TypeSystemDefinition};
use async_graphql::{Request, Value as GqlValue};
use weaver_server_api::WeaverSchema;
use weaver_server_api::auth::CallerIdentity;
use weaver_server_core::auth::CallerScope;

/// How deep a generated selection set may nest. Six levels reaches every leaf
/// of the diagnostics roots with room to spare, and stays well inside the
/// schema's own depth limit.
const MAX_SELECTION_DEPTH: usize = 6;

/// One field of one type, reduced to what the selection builder needs.
struct FieldShape {
    name: String,
    named_type: String,
    requires_argument: bool,
}

/// What the schema looks like, as far as generating a selection set goes.
pub(super) struct SchemaShape {
    /// Object types by name. Scalars, enums, interfaces, unions and inputs are
    /// deliberately absent: a name that is missing here is a leaf or is not
    /// selectable, and either way the walk stops.
    objects: HashMap<String, Vec<FieldShape>>,
    query_type: String,
}

/// Unwraps list wrappers down to the named type.
fn named_type(base: &BaseType) -> String {
    match base {
        BaseType::Named(name) => name.to_string(),
        BaseType::List(inner) => named_type(&inner.base),
    }
}

impl SchemaShape {
    /// The shape of the schema this binary serves, parsed once.
    pub(super) fn current() -> Result<&'static Self, String> {
        static SHAPE: OnceLock<Result<SchemaShape, String>> = OnceLock::new();
        SHAPE
            .get_or_init(|| Self::from_sdl(&weaver_server_api::export_schema_sdl()))
            .as_ref()
            .map_err(Clone::clone)
    }

    fn from_sdl(sdl: &str) -> Result<Self, String> {
        let document = async_graphql::parser::parse_schema(sdl)
            .map_err(|error| format!("could not parse the schema: {error}"))?;

        let mut objects = HashMap::new();
        let mut query_type = "Query".to_string();
        for definition in &document.definitions {
            match definition {
                TypeSystemDefinition::Schema(schema) => {
                    if let Some(query) = &schema.node.query {
                        query_type = query.node.to_string();
                    }
                }
                TypeSystemDefinition::Type(type_definition) => {
                    let TypeKind::Object(object) = &type_definition.node.kind else {
                        continue;
                    };
                    let fields = object
                        .fields
                        .iter()
                        .map(|field| FieldShape {
                            name: field.node.name.node.to_string(),
                            named_type: named_type(&field.node.ty.node.base),
                            requires_argument: field.node.arguments.iter().any(|argument| {
                                !argument.node.ty.node.nullable
                                    && argument.node.default_value.is_none()
                            }),
                        })
                        .collect();
                    objects.insert(type_definition.node.name.node.to_string(), fields);
                }
                TypeSystemDefinition::Directive(_) => {}
            }
        }

        if !objects.contains_key(&query_type) {
            return Err(format!("the schema declares no `{query_type}` type"));
        }
        Ok(Self {
            objects,
            query_type,
        })
    }

    /// One field of the root query type, by name.
    fn root_field(&self, field: &str) -> Option<&FieldShape> {
        self.objects
            .get(&self.query_type)?
            .iter()
            .find(|candidate| candidate.name == field)
    }

    /// The selection set for one root query field.
    ///
    /// An empty string means the field is a leaf and is selected on its own;
    /// `None` means the schema has no such field, or that nothing under it can
    /// be selected without arguments.
    pub(super) fn root_selection(&self, field: &str) -> Option<String> {
        let shape = self.root_field(field)?;
        if !self.objects.contains_key(&shape.named_type) {
            // A scalar or enum root field, selected without a sub-selection.
            return Some(String::new());
        }
        let mut path = Vec::new();
        self.selection_for(&shape.named_type, MAX_SELECTION_DEPTH, &mut path)
    }

    /// Builds `{ a b c { d } }` for one object type.
    fn selection_for(
        &self,
        type_name: &str,
        depth: usize,
        path: &mut Vec<String>,
    ) -> Option<String> {
        if depth == 0 || path.iter().any(|seen| seen == type_name) {
            return None;
        }
        let fields = self.objects.get(type_name)?;
        path.push(type_name.to_string());

        let mut parts = Vec::new();
        for field in fields {
            if field.requires_argument || field.name.starts_with("__") {
                continue;
            }
            if self.objects.contains_key(&field.named_type) {
                if let Some(nested) = self.selection_for(&field.named_type, depth - 1, path) {
                    parts.push(format!("{} {nested}", field.name));
                }
            } else {
                // Scalars and enums are leaves. Interfaces, unions and inputs
                // never appear as an output type on these roots; if one ever
                // does it is skipped rather than generating an invalid query.
                parts.push(field.name.clone());
            }
        }
        path.pop();

        if parts.is_empty() {
            None
        } else {
            Some(format!("{{ {} }}", parts.join(" ")))
        }
    }
}

/// The query text for one root field, or the reason there is none.
pub(super) fn root_query(
    shape: &SchemaShape,
    field: &str,
    arguments: Option<&str>,
) -> Result<String, String> {
    let root = shape
        .root_field(field)
        .ok_or_else(|| format!("`{field}` is not a query field"))?;
    if root.requires_argument && arguments.is_none() {
        // Asking anyway would send a query the schema rejects outright, losing
        // the whole file instead of recording one honest error.
        return Err(format!("`{field}` cannot be read without arguments"));
    }
    let selection = shape
        .root_selection(field)
        .ok_or_else(|| format!("`{field}` is not a selectable query field"))?;
    let arguments = arguments
        .map(|arguments| format!("({arguments})"))
        .unwrap_or_default();
    Ok(format!(
        "query WeaverDiagnostics {{ {field}{arguments} {selection} }}"
    ))
}

/// Counts the field nodes a generated query resolves.
///
/// This is what the schema's complexity limit counts, so it is what the tests
/// assert against.
#[cfg(test)]
pub(super) fn field_count(query: &str) -> usize {
    query
        .split_whitespace()
        .filter(|token| {
            !token.is_empty()
                && !matches!(*token, "query" | "WeaverDiagnostics" | "{" | "}")
                && !token.starts_with('(')
        })
        .count()
}

/// Runs one root field and returns its value plus any field-level errors.
///
/// Field-level errors do not discard the read: async-graphql still returns the
/// data it resolved, and the errors are handed back so the caller can record
/// them beside a mostly-complete payload.
pub(super) async fn run_root_field(
    schema: &WeaverSchema,
    shape: &SchemaShape,
    scope: CallerScope,
    identity: CallerIdentity,
    field: &str,
    arguments: Option<&str>,
) -> Result<(serde_json::Value, Vec<String>), String> {
    let query = root_query(shape, field, arguments)?;
    let response = schema
        .execute(Request::new(query).data(scope).data(identity))
        .await;
    let errors: Vec<String> = response.errors.iter().map(ToString::to_string).collect();

    if matches!(response.data, GqlValue::Null) {
        return Err(if errors.is_empty() {
            format!("`{field}` returned no data")
        } else {
            errors.join("; ")
        });
    }

    let data = serde_json::to_value(&response.data)
        .map_err(|error| format!("`{field}` result is not representable: {error}"))?;
    let value = data
        .get(field)
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    Ok((value, errors))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SDL: &str = r#"
        type Query {
          version: String!
          metrics: Metrics!
          jobs(status: [String!], limit: Int): [Job!]!
          metricsHistory(range: RangeGql!): Metrics!
          serviceLogs(limit: Int! = 250): Logs!
          cyclic: Node!
        }
        type Metrics { speed: Float! errors: Int! window(seconds: Int!): Float! }
        type Job { id: Int! nested: Nested! }
        type Nested { label: String! }
        type Logs { lines: [String!]! count: Int! }
        type Node { name: String! child: Node! }
    "#;

    fn shape() -> SchemaShape {
        SchemaShape::from_sdl(SDL).expect("test schema parses")
    }

    #[test]
    fn scalar_roots_select_themselves() {
        assert_eq!(shape().root_selection("version"), Some(String::new()));
        assert_eq!(
            root_query(&shape(), "version", None).expect("query"),
            "query WeaverDiagnostics { version  }"
        );
    }

    #[test]
    fn object_roots_expand_every_argument_free_field() {
        assert_eq!(
            shape().root_selection("metrics"),
            Some("{ speed errors }".to_string())
        );
        assert_eq!(
            shape().root_selection("jobs"),
            Some("{ id nested { label } }".to_string())
        );
    }

    #[test]
    fn fields_needing_arguments_are_skipped_and_defaults_are_not() {
        // A nested field with a required argument is left out of its parent's
        // selection rather than asked for without one.
        assert_eq!(
            shape().root_selection("metrics"),
            Some("{ speed errors }".to_string())
        );
        // A root with a required argument is refused unless the caller supplies
        // it, rather than sent as a query the schema rejects outright.
        assert!(root_query(&shape(), "metricsHistory", None).is_err());
        assert!(root_query(&shape(), "metricsHistory", Some("range: LAST_HOUR")).is_ok());
        // A non-null argument with a default needs nothing from the caller.
        assert_eq!(
            shape().root_selection("serviceLogs"),
            Some("{ lines count }".to_string())
        );
        assert!(root_query(&shape(), "serviceLogs", None).is_ok());
    }

    #[test]
    fn recursive_types_terminate() {
        assert_eq!(shape().root_selection("cyclic"), Some("{ name }".to_string()));
    }

    #[test]
    fn arguments_are_rendered_onto_the_root_field() {
        assert_eq!(
            root_query(&shape(), "jobs", Some("limit: 5")).expect("query"),
            "query WeaverDiagnostics { jobs(limit: 5) { id nested { label } } }"
        );
    }

    #[test]
    fn unknown_roots_are_reported_rather_than_queried() {
        assert!(root_query(&shape(), "notAField", None).is_err());
    }
}
