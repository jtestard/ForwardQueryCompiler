CREATE SCHEMA introspect;

SET search_path to introspect;

-- Table with supported primary key
CREATE TABLE supported_pkey(
	id	integer primary key
);

-- Table with unsupported primary key type
-- type_2 is required because tables cannot be empty.
CREATE TABLE unsupported_pkey(
	uuid_key			uuid primary key,
	type_2				integer
);

-- Table references supported primary key type
CREATE TABLE supported_fkey(
	id_ref	integer references supported_pkey (id) on delete cascade
);

-- Table references unsupported primary key type
CREATE TABLE unsupported_fkey(
	uuid_ref	uuid references unsupported_pkey (uuid_key) on delete cascade,
	type_2		integer
);

-- Non-null constraint on supported and unsuppored type
CREATE TABLE non_null_table(
	id			integer not null,
	uuid_type	uuid not null
);
