# Reportnet 3 Reader Feature Type Parameters
To access feature type parameters, click the gear icon on a feature type in the workspace. This opens the Feature Type Parameter Editor. 

## Options
### WHERE Clause
A *limited* filtering is supported by supplying a where-clause. 

The where clause must contain exactly one predicate in the form:

    <identifier> = <literal-value>

Where `identifier` is a valid fieldname (FME attribute name), and `literal-value` a valid SQL literal.

Please note that equality (=) _is the only_ operator that can be used in the expression, i.e. `!=, <, >, AND, OR` are all examples of operators that can _not_ be used.

These restrictions also applies when using the Reportnet 3 Reader in a FeatureReader-transformer.

Examples:
```sql
    id = 2
```

```sql
    r = 10.2
```

Double quotes must be used if the field name contains special characters like whitespace:
```sql
    "my field" = 'Some text'
```

Single quotes in a string literal needs to be escaped by doubling them:
```sql
    "my doc" = 'I''d like to code more'
```

Newlines should be ok but can be tricky.
```sql
    "my doc" = 'A longer text 
    Newlines should be ok'
```

In order to use specific newline character(s), url-encoding can be used:

```sql
    "my doc" = 'First line%0D%0ASecond line separated by CRLF'
```

```sql
    "my doc" = 'First line%0ASecond line separated by LF'
```
```sql
    "my doc" = 'First line%0DSecond line separated by CR'
```
