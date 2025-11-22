"""Tests for SQLAlchemy table generation."""

from sqlalchemy import (
    Boolean as SABoolean,
)
from sqlalchemy import (
    Date as SADate,
)
from sqlalchemy import (
    DateTime,
    MetaData,
)
from sqlalchemy import (
    Float as SAFloat,
)
from sqlalchemy import (
    Integer as SAInteger,
)
from sqlalchemy import (
    String as SAString,
)

from flycatcher import Boolean, Date, Datetime, Float, Integer, Schema, String


class TestSQLAlchemyTableGeneration:
    """Test SQLAlchemy table generation."""

    def test_basic_table_generation(self, simple_schema):
        """Basic table is generated correctly."""
        table = simple_schema.to_sqlalchemy()

        assert table is not None
        assert table.name == "simples"  # Default naming
        assert len(table.columns) == 3

    def test_custom_table_name(self, simple_schema):
        """Custom table name is used."""
        table = simple_schema.to_sqlalchemy(table_name="users")
        assert table.name == "users"

    def test_table_name_generation(self):
        """Table name is generated from schema class name."""

        class UserSchema(Schema):
            id = Integer()

        table = UserSchema.to_sqlalchemy()
        assert table.name == "users"

        class PersonSchema(Schema):
            id = Integer()

        table = PersonSchema.to_sqlalchemy()
        assert table.name == "persons"

    def test_all_field_types_in_table(self):
        """All field types generate correct SQLAlchemy columns."""

        class AllTypesSchema(Schema):
            int_field = Integer()
            str_field = String()
            float_field = Float()
            bool_field = Boolean()
            datetime_field = Datetime()
            date_field = Date()

        table = AllTypesSchema.to_sqlalchemy()

        assert len(table.columns) == 6
        assert isinstance(table.c.int_field.type, type(SAInteger()))
        assert isinstance(table.c.str_field.type, (type(SAString(1)), type(SAString())))
        assert isinstance(table.c.float_field.type, type(SAFloat()))
        assert isinstance(table.c.bool_field.type, type(SABoolean()))
        assert isinstance(table.c.datetime_field.type, type(DateTime()))
        assert isinstance(table.c.date_field.type, type(SADate()))


class TestSQLAlchemyColumnProperties:
    """Test SQLAlchemy column properties."""

    def test_primary_key(self):
        """Primary key flag is set correctly."""

        class UserSchema(Schema):
            id = Integer(primary_key=True)
            name = String()

        table = UserSchema.to_sqlalchemy()
        assert table.c.id.primary_key is True
        assert table.c.name.primary_key is False

    def test_nullable(self):
        """Nullable flag is set correctly."""

        class UserSchema(Schema):
            id = Integer()
            name = String(nullable=True)
            age = Integer(nullable=False)

        table = UserSchema.to_sqlalchemy()
        assert table.c.id.nullable is False  # Default
        assert table.c.name.nullable is True
        assert table.c.age.nullable is False

    def test_unique(self):
        """Unique flag is set correctly."""

        class UserSchema(Schema):
            id = Integer(primary_key=True)
            email = String(unique=True)
            name = String()

        table = UserSchema.to_sqlalchemy()
        assert table.c.email.unique is True
        # unique returns None when False, not False
        assert table.c.name.unique is None or table.c.name.unique is False

    def test_index(self):
        """Index flag is set correctly."""

        class UserSchema(Schema):
            id = Integer(primary_key=True)
            email = String(index=True)
            name = String()

        table = UserSchema.to_sqlalchemy()
        assert table.c.email.index is True
        # index returns None when False, not False
        assert table.c.name.index is None or table.c.name.index is False

    def test_default_values(self):
        """Default values are set correctly."""

        class UserSchema(Schema):
            id = Integer(primary_key=True)
            name = String(default="unknown")
            count = Integer(default=0)
            is_active = Boolean(default=True)

        table = UserSchema.to_sqlalchemy()
        assert table.c.name.default.arg == "unknown"
        assert table.c.count.default.arg == 0
        assert table.c.is_active.default.arg is True

    def test_autoincrement(self):
        """Autoincrement is set correctly."""

        class UserSchema(Schema):
            id = Integer(primary_key=True, autoincrement=True)
            other_id = Integer(primary_key=True, autoincrement=False)

        table = UserSchema.to_sqlalchemy()
        assert table.c.id.autoincrement is True
        assert table.c.other_id.autoincrement is False


class TestSQLAlchemyMetadata:
    """Test SQLAlchemy metadata handling."""

    def test_custom_metadata(self, simple_schema):
        """Custom metadata can be provided."""
        metadata = MetaData()
        table = simple_schema.to_sqlalchemy(metadata=metadata)

        assert table.metadata is metadata
        assert table in metadata.tables.values()

    def test_default_metadata(self, simple_schema):
        """Default metadata is created if not provided."""
        table = simple_schema.to_sqlalchemy()

        assert table.metadata is not None
        assert isinstance(table.metadata, MetaData)
