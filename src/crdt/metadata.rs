use sqlparser::ast::{
    Assignment, AssignmentTarget, Expr, Ident, ObjectName, ObjectNamePart, Value,
};
use uhlc::Timestamp;

/// Build the assignment used by every CRDT write path for the row HLC.
/// Keeping this in one place prevents the transformer and INSERT upsert path
/// from drifting in how they encode metadata values.
pub(crate) fn create_hlc_assignment(column: &str, timestamp: &Timestamp) -> Assignment {
    Assignment {
        target: AssignmentTarget::ColumnName(ObjectName(vec![ObjectNamePart::Identifier(
            Ident::new(column),
        )])),
        value: Expr::Value(Value::SingleQuotedString(timestamp.to_string()).into()),
    }
}
