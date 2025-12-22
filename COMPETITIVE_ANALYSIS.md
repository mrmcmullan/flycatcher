# Flycatcher Competitive Analysis: Dataframely

Based on analysis of [dataframely](https://github.com/Quantco/dataframely) (v2.3.0, 477 stars), this document identifies features to prioritize and core deficits to address.

## 🎯 Critical Features to Prioritize

### 1. **Group Validation** (HIGH PRIORITY)
**Dataframely Feature:**
```python
@dy.rule(group_by=["zip_code"])
def minimum_zip_code_count(cls) -> pl.Expr:
    return pl.len() >= 2
```

**Why it matters:**
- Enables validation across groups (e.g., "each zip_code must have at least 2 records")
- Common in data quality checks (uniqueness per group, aggregate constraints)
- **Flycatcher currently lacks this entirely**

**Implementation Priority:** 🔴 **CRITICAL** - This is a major differentiator

**Recommendation:**
```python
@model_validator(group_by=["zip_code"])
def minimum_zip_code_count():
    return (pl.len() >= 2, "Each zip_code must have at least 2 records")
```

---

### 2. **Enhanced Type Hints** (HIGH PRIORITY)
**Dataframely Feature:**
```python
validated_df: dy.DataFrame[HouseSchema] = HouseSchema.validate(df, cast=True)
```

**Why it matters:**
- Better IDE support (autocomplete, type checking)
- Self-documenting code
- Catches errors at development time
- **Flycatcher currently returns plain `pl.DataFrame`**

**Implementation Priority:** 🔴 **CRITICAL** - Developer experience differentiator

**Recommendation:**
```python
from flycatcher import DataFrame

validated_df: DataFrame[UserSchema] = UserSchema.to_polars_validator().validate(df)
# Or even better:
validated_df: DataFrame[UserSchema] = UserSchema.validate_polars(df)
```

---

### 3. **Automatic Type Casting** (MEDIUM PRIORITY)
**Dataframely Feature:**
```python
HouseSchema.validate(df, cast=True)  # Automatically casts columns to expected types
```

**Why it matters:**
- Reduces boilerplate (no manual `.cast()` calls)
- Ensures type consistency
- **Flycatcher requires manual casting or does it implicitly but not explicitly**

**Current Flycatcher:**
```python
# Flycatcher does cast, but it's implicit in validate()
validator.validate(df, strict=True)  # Casts internally
```

**Enhancement needed:**
- Make casting explicit and configurable
- Add `cast=True/False` parameter (default True for backward compat)

---

### 4. **Direct Polars Expression Support** (MEDIUM PRIORITY)
**Dataframely Approach:**
```python
@dy.rule()
def reasonable_bathroom_to_bedroom_ratio(cls) -> pl.Expr:
    ratio = pl.col("num_bathrooms") / pl.col("num_bedrooms")
    return (ratio >= 1 / 3) & (ratio <= 3)
```

**Flycatcher Approach:**
```python
@model_validator
def check_ratio():
    return (
        (col('num_bathrooms') / col('num_bedrooms') >= 1/3) &
        (col('num_bathrooms') / col('num_bedrooms') <= 3),
        "Ratio must be between 1/3 and 3"
    )
```

**Why it matters:**
- Dataframely allows direct `pl.Expr` for advanced users
- More flexible for complex Polars operations
- **Flycatcher's DSL is more limited**

**Recommendation:**
- Keep DSL for dual Pydantic/Polars support
- Add escape hatch: allow returning `pl.Expr` directly for Polars-only validators
- Document when to use each approach

---

### 5. **Explicit Field Type Definitions** (LOW PRIORITY)
**Dataframely:**
```python
num_bedrooms = dy.UInt8(nullable=False)
price = dy.Float64(nullable=False)
```

**Flycatcher:**
```python
num_bedrooms: int = Field(nullable=False)
price: float = Field(nullable=False)
```

**Why it matters:**
- Dataframely allows precise numeric types (UInt8, Int16, Float32, Float64)
- Better memory efficiency for large datasets
- **Flycatcher uses Python types (int, float) which map to Int64/Float64**

**Recommendation:**
- Consider adding explicit numeric types as optional enhancement
- Keep type annotations as primary API (more Pythonic)
- Add precision hints: `price: float = Field(dtype=pl.Float32)`

---

## 🚨 Core Deficits (Why Users Might Choose Dataframely)

### 1. **No Group Validation** ⚠️ **CRITICAL DEFICIT**
**Impact:** Users who need group-level constraints (e.g., "each customer must have at least 3 orders") cannot use Flycatcher.

**Example use case:**
```python
# Dataframely can do this:
@dy.rule(group_by=["customer_id"])
def minimum_orders(cls) -> pl.Expr:
    return pl.len() >= 3

# Flycatcher cannot do this
```

**User Impact:** High - Forces users to choose dataframely for group validation needs.

---

### 2. **No Enhanced Type Hints** ⚠️ **HIGH IMPACT DEFICIT**
**Impact:** Poor IDE experience compared to dataframely.

**Dataframely:**
```python
df: dy.DataFrame[HouseSchema]  # IDE knows all columns and types
df.zip_code  # Autocomplete works!
```

**Flycatcher:**
```python
df: pl.DataFrame  # IDE has no schema information
df["zip_code"]  # No autocomplete, no type checking
```

**User Impact:** Medium-High - Developers prefer better tooling support.

---

### 3. **Less Mature** ⚠️ **PERCEPTION DEFICIT**
**Dataframely:** v2.3.0 (30 releases, 477 stars, 22 contributors)
**Flycatcher:** v0.1.0 (Alpha)

**Impact:**
- Users may perceive flycatcher as less stable
- Fewer examples/documentation
- Less community support

**Mitigation:**
- Accelerate to v1.0.0
- Build community (examples, tutorials)
- Highlight unique value (Pydantic + SQLAlchemy integration)

---

### 4. **DSL Limitations** ⚠️ **MODERATE DEFICIT**
**Impact:** Advanced Polars users may find DSL restrictive.

**Dataframely:**
```python
# Can use any Polars expression directly
@dy.rule()
def complex_validation(cls) -> pl.Expr:
    return pl.col("a").rolling_mean(window_size=5) > pl.col("b")
```

**Flycatcher:**
```python
# DSL may not support all Polars operations
# Must use explicit format for unsupported operations
@model_validator
def complex_validation():
    return {
        'polars': (pl.col("a").rolling_mean(window_size=5) > pl.col("b"), "Message"),
        'pydantic': lambda v: (v.a > v.b, "Message")  # Can't replicate rolling_mean
    }
```

**User Impact:** Medium - Advanced users may prefer dataframely's flexibility.

---

### 5. **No Test Data Generation** ⚠️ **NICE-TO-HAVE DEFICIT**
**Dataframely:** Can generate test data that complies with schemas
**Flycatcher:** No test data generation

**Impact:** Lower - Not critical, but useful for testing.

---

## ✅ Flycatcher's Competitive Advantages

### 1. **Multi-Target Generation** (UNIQUE)
- Pydantic + Polars + SQLAlchemy from one schema
- **Dataframely cannot do this**

### 2. **Dual-Context Validation**
- Same validators work in Pydantic (row-level) and Polars (bulk)
- **Dataframely is Polars-only**

### 3. **Full-Stack Integration**
- Perfect for APIs → ETL → Database workflows
- **Dataframely is ETL-focused**

### 4. **Pydantic Integration**
- Generates actual Pydantic models
- **Dataframely has no Pydantic support**

---

## 📊 Priority Roadmap

### Phase 1: Critical Features (v0.3.0)
1. ✅ **Group Validation** - `@model_validator(group_by=[...])`
2. ✅ **Enhanced Type Hints** - `DataFrame[Schema]` type
3. ✅ **Explicit Casting** - `cast=True/False` parameter

### Phase 2: Developer Experience (v0.4.0)
4. ✅ **Direct Polars Expression Support** - Allow `pl.Expr` in validators
5. ✅ **Better DSL Coverage** - Expand DSL to match more Polars operations
6. ✅ **Test Data Generation** - Generate compliant test data

### Phase 3: Advanced Features (v1.0.0)
7. ✅ **Precise Numeric Types** - UInt8, Int16, Float32, etc.
8. ✅ **Validation Introspection** - Better error reporting
9. ✅ **Performance Optimization** - Rust components (like dataframely)

---

## 🎯 Strategic Recommendations

### Immediate Actions (Next 3 Months)
1. **Implement group validation** - This is the #1 feature gap
2. **Add enhanced type hints** - Critical for developer experience
3. **Accelerate to v0.3.0** - Show active development

### Medium-Term (6 Months)
4. **Expand DSL coverage** - Match more Polars operations
5. **Add test data generation** - Nice-to-have feature
6. **Build community** - Examples, tutorials, blog posts

### Long-Term (12 Months)
7. **Reach v1.0.0** - Maturity milestone
8. **Consider Rust components** - Performance optimization
9. **Expand ecosystem** - More integrations (PyArrow, etc.)

---

## 💡 Key Insight

**Flycatcher's unique value is multi-target generation (Pydantic + Polars + SQLAlchemy).**

**To compete with dataframely in Polars-only scenarios, you need:**
1. Group validation (critical)
2. Enhanced type hints (critical)
3. Better Polars expression support (important)

**But remember:** Flycatcher's core strength is being the **only** library that generates Pydantic, Polars, and SQLAlchemy from one schema. Don't lose this differentiation while adding Polars features.

---

## 📚 References

- [Dataframely GitHub](https://github.com/Quantco/dataframely)
- [Dataframely Documentation](https://dataframely.readthedocs.io/)
- Dataframely v2.3.0 (Latest, Dec 2024)
- 477 stars, 22 contributors, 30 releases


