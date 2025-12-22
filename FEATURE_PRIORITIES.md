# Feature Priorities: Flycatcher vs Dataframely

## 🎯 Features to Focus On

### 1. **Group Validation** (🔴 CRITICAL)
**What dataframely has:**
```python
@dy.rule(group_by=["zip_code"])
def minimum_zip_code_count(cls) -> pl.Expr:
    return pl.len() >= 2
```

**Why it matters:**
- Essential for data quality checks (e.g., "each customer must have at least 3 orders")
- Common in real-world validation scenarios
- **Flycatcher completely lacks this feature**

**Priority:** Implement in v0.3.0

---

### 2. **Enhanced Type Hints** (🔴 CRITICAL)
**What dataframely has:**
```python
validated_df: dy.DataFrame[HouseSchema] = HouseSchema.validate(df, cast=True)
# IDE knows all columns: validated_df.zip_code, validated_df.price, etc.
```

**Why it matters:**
- Better IDE autocomplete and type checking
- Self-documenting code
- Catches errors at development time
- **Flycatcher returns plain `pl.DataFrame` with no schema information**

**Priority:** Implement in v0.3.0

---

### 3. **Direct Polars Expression Support** (🟡 HIGH)
**What dataframely has:**
```python
@dy.rule()
def complex_validation(cls) -> pl.Expr:
    # Can use any Polars expression directly
    return pl.col("a").rolling_mean(window_size=5) > pl.col("b")
```

**Why it matters:**
- Advanced users need full Polars flexibility
- Flycatcher's DSL may not cover all Polars operations
- **Workaround exists but is clunky**

**Priority:** Add escape hatch in v0.3.0, expand DSL in v0.4.0

---

### 4. **Explicit Type Casting Control** (🟡 MEDIUM)
**What dataframely has:**
```python
HouseSchema.validate(df, cast=True)  # Explicit control
```

**Why it matters:**
- Users want control over when casting happens
- **Flycatcher casts implicitly but doesn't expose control**

**Priority:** Add `cast` parameter in v0.3.0

---

### 5. **Test Data Generation** (🟢 LOW)
**What dataframely has:**
- Can generate test data that complies with schemas

**Why it matters:**
- Useful for testing and development
- **Flycatcher doesn't have this**

**Priority:** Nice-to-have for v0.4.0+

---

## 🚨 Core Deficits (Why Users Choose Dataframely)

### 1. **No Group Validation** ⚠️ **CRITICAL DEFICIT**

**Impact:** Users who need group-level constraints **cannot use Flycatcher**.

**Example:**
```python
# Dataframely can validate: "Each customer must have at least 3 orders"
@dy.rule(group_by=["customer_id"])
def minimum_orders(cls) -> pl.Expr:
    return pl.len() >= 3

# Flycatcher cannot do this - forces users to choose dataframely
```

**User Impact:** **HIGH** - This is a deal-breaker for many data quality use cases.

---

### 2. **No Enhanced Type Hints** ⚠️ **HIGH IMPACT DEFICIT**

**Impact:** Poor developer experience compared to dataframely.

**Dataframely:**
```python
df: dy.DataFrame[HouseSchema]  # IDE autocomplete works!
df.zip_code  # Type-checked, autocomplete available
```

**Flycatcher:**
```python
df: pl.DataFrame  # No schema information
df["zip_code"]  # No autocomplete, no type checking
```

**User Impact:** **MEDIUM-HIGH** - Developers prefer better tooling support.

---

### 3. **Less Mature** ⚠️ **PERCEPTION DEFICIT**

**Dataframely:** v2.3.0, 477 stars, 30 releases, 22 contributors
**Flycatcher:** v0.1.0 (Alpha)

**Impact:**
- Users may perceive flycatcher as less stable
- Fewer examples and community support
- Less battle-tested

**Mitigation:**
- Accelerate to v1.0.0
- Build community and examples
- Highlight unique value (Pydantic + SQLAlchemy)

---

### 4. **DSL Limitations** ⚠️ **MODERATE DEFICIT**

**Impact:** Advanced Polars users may find DSL restrictive.

**Dataframely:**
```python
# Can use any Polars expression
@dy.rule()
def complex(cls) -> pl.Expr:
    return pl.col("a").rolling_mean(5) > pl.col("b")
```

**Flycatcher:**
```python
# Must use explicit format for unsupported operations
@model_validator
def complex():
    return {
        'polars': (pl.col("a").rolling_mean(5) > pl.col("b"), "Message"),
        'pydantic': lambda v: (v.a > v.b, "Message")  # Can't replicate rolling_mean
    }
```

**User Impact:** **MEDIUM** - Advanced users may prefer dataframely's flexibility.

---

### 5. **No Test Data Generation** ⚠️ **NICE-TO-HAVE DEFICIT**

**Impact:** Lower priority, but useful for testing workflows.

**User Impact:** **LOW** - Not critical, but a nice convenience feature.

---

## ✅ Flycatcher's Competitive Advantages (Don't Lose These!)

### 1. **Multi-Target Generation** (UNIQUE)
- **Only library** that generates Pydantic + Polars + SQLAlchemy from one schema
- Dataframely cannot do this

### 2. **Dual-Context Validation**
- Same validators work in Pydantic (row-level) and Polars (bulk)
- Dataframely is Polars-only

### 3. **Full-Stack Integration**
- Perfect for APIs → ETL → Database workflows
- Dataframely is ETL-focused only

---

## 📋 Recommended Action Plan

### Immediate (v0.3.0)
1. ✅ **Group Validation** - `@model_validator(group_by=[...])`
2. ✅ **Enhanced Type Hints** - `DataFrame[Schema]` type
3. ✅ **Explicit Casting** - `cast=True/False` parameter

### Short-Term (v0.4.0)
4. ✅ **Direct Polars Expression Support** - Allow `pl.Expr` in validators
5. ✅ **Expand DSL Coverage** - Match more Polars operations
6. ✅ **Test Data Generation** - Generate compliant test data

### Long-Term (v1.0.0)
7. ✅ **Reach v1.0.0** - Maturity milestone
8. ✅ **Performance Optimization** - Consider Rust components
9. ✅ **Community Building** - Examples, tutorials, blog posts

---

## 💡 Key Takeaway

**To compete with dataframely in Polars-only scenarios, you MUST have:**
1. ✅ Group validation (critical - deal-breaker without it)
2. ✅ Enhanced type hints (critical - developer experience)
3. ✅ Better Polars expression support (important - advanced users)

**But remember:** Flycatcher's **unique value** is multi-target generation (Pydantic + Polars + SQLAlchemy). Don't lose this differentiation while adding Polars features.

---

For detailed analysis, see [COMPETITIVE_ANALYSIS.md](COMPETITIVE_ANALYSIS.md).



