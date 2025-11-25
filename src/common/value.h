/*------------------------------------------------------------------------------
 - Copyright (c) 2024. Websoft research group, Nanjing University.
 -
 - This program is free software: you can redistribute it and/or modify
 - it under the terms of the GNU General Public License as published by
 - the Free Software Foundation, either version 3 of the License, or
 - (at your option) any later version.
 -
 - This program is distributed in the hope that it will be useful,
 - but WITHOUT ANY WARRANTY; without even the implied warranty of
 - MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 - GNU General Public License for more details.
 -
 - You should have received a copy of the GNU General Public License
 - along with this program.  If not, see <https://www.gnu.org/licenses/>.
 -----------------------------------------------------------------------------*/

//
// Created by ziqi on 2024/7/19.
//

#ifndef NJUDB_VALUE_H
#define NJUDB_VALUE_H

#include <string>
#include <vector>
#include <limits>
#include <algorithm>
#include "types.h"
#include "../../common/error.h"
#include "../../common/micro.h"

namespace njudb {

class Value;
class IntValue;
class FloatValue;
class BoolValue;
class StringValue;
class ArrayValue;
DEFINE_SHARED_PTR(Value);
DEFINE_SHARED_PTR(IntValue);
DEFINE_SHARED_PTR(FloatValue);
DEFINE_SHARED_PTR(BoolValue);
DEFINE_SHARED_PTR(StringValue);
DEFINE_SHARED_PTR(ArrayValue);

class ValueFactory;

class Value
{
public:
  Value() = delete;
  Value(FieldType type, size_t size, bool is_null) : type_(type), size_(size), is_null_(is_null) {}
  Value(const Value &value)            = default;
  Value(Value &&value)                 = default;
  Value &operator=(const Value &value) = default;
  Value &operator=(Value &&value)      = default;
  virtual ~Value()                     = default;

  [[nodiscard]] FieldType GetType() const { return type_; }

  [[nodiscard]] size_t GetSize() const { return size_; }

  [[nodiscard]] bool IsNull() const { return is_null_; }

  virtual auto operator==(const Value &value) const -> bool = 0;

  virtual auto operator<(const Value &value) const -> bool = 0;

  virtual auto operator>(const Value &value) const -> bool = 0;

  auto operator!=(const Value &value) const -> bool { return !(*this == value); }

  auto operator<=(const Value &value) const -> bool { return !IsNull() && !value.IsNull() && !(*this > value); }

  auto operator>=(const Value &value) const -> bool { return !IsNull() && !value.IsNull() && !(*this < value); }

  virtual auto operator+=(const Value &value) -> Value &
  {
    NJUDB_THROW(NJUDB_UNSUPPORTED_OP, FieldTypeToString(GetType()));
  }

  // this special operator is used to support average calculation in aggregation
  virtual auto operator/=(int k) -> Value & { NJUDB_THROW(NJUDB_UNSUPPORTED_OP, FieldTypeToString(GetType())); }

  static auto Max(const Value &lval, const Value &rval) -> const Value &
  {
    if (lval.IsNull()) {
      return rval;
    }
    if (rval.IsNull()) {
      return lval;
    }
    return lval > rval ? lval : rval;
  }

  static auto Min(const Value &lval, const Value &rval) -> const Value &
  {
    if (lval.IsNull()) {
      return rval;
    }
    if (rval.IsNull()) {
      return lval;
    }
    return lval < rval ? lval : rval;
  }

  // faster version of Max and Min without receiver's copy
  static auto Max(const ValueSptr &lval, const ValueSptr &rval) -> const ValueSptr &
  {
    if (lval->IsNull()) {
      return rval;
    }
    if (rval->IsNull()) {
      return lval;
    }
    return *lval > *rval ? lval : rval;
  }

  static auto Min(const ValueSptr &lval, const ValueSptr &rval) -> const ValueSptr &
  {
    if (lval->IsNull()) {
      return rval;
    }
    if (rval->IsNull()) {
      return lval;
    }
    return *lval < *rval ? lval : rval;
  }

  [[nodiscard]] virtual auto ToString() const -> std::string { NJUDB_FATAL("never reach here"); }

  static void CheckBasic(const Value &lval, const Value &rval)
  {
    if (lval.GetType() != rval.GetType()) {
      NJUDB_THROW(NJUDB_TYPE_MISSMATCH,
          fmt::format("Type mismatch: {} != {}", FieldTypeToString(lval.GetType()), FieldTypeToString(rval.GetType())));
    }
  }

  /**
   * align two values to the same type, currently int and float to float
   * @param lval
   * @param rval
   */

protected:
  FieldType type_;
  size_t    size_;
  bool      is_null_;
};

class IntValue : public Value
{
public:
  IntValue() = delete;
  explicit IntValue(int32_t value, bool is_null) : Value(FieldType::TYPE_INT, sizeof(int32_t), is_null), value_(value)
  {}
  explicit IntValue(const char *mem) : Value(FieldType::TYPE_INT, sizeof(int32_t), false)
  {
    NJUDB_ASSERT(mem != nullptr, "mem is nullptr");
    value_ = *reinterpret_cast<const int32_t *>(mem);
  }
  IntValue(const IntValue &value)            = default;
  IntValue(IntValue &&value)                 = default;
  IntValue &operator=(const IntValue &value) = default;
  IntValue &operator=(IntValue &&value)      = default;
  ~IntValue() override                       = default;

  auto operator==(const Value &value) const -> bool override
  {
    CheckBasic(*this, value);
    if (IsNull() && value.IsNull()) {
      return true;
    }
    return !IsNull() && !value.IsNull() && value_ == dynamic_cast<const IntValue &>(value).value_;
  }

  auto operator<(const Value &value) const -> bool override
  {
    CheckBasic(*this, value);
    return !IsNull() && !value.IsNull() && value_ < dynamic_cast<const IntValue &>(value).value_;
  }

  auto operator>(const Value &value) const -> bool override
  {
    CheckBasic(*this, value);
    return !IsNull() && !value.IsNull() && value_ > dynamic_cast<const IntValue &>(value).value_;
  }

  auto operator+=(const Value &value) -> Value & override
  {
    CheckBasic(*this, value);
    if (IsNull()) {
      *this = dynamic_cast<const IntValue &>(value);
    } else {
      value_ += dynamic_cast<const IntValue &>(value).value_;
    }
    return *this;
  }

  auto operator/=(int k) -> Value & override
  {
    if (IsNull()) {
      return *this;
    }
    if (k == 0) {
      NJUDB_THROW(NJUDB_UNEXPECTED_NULL, "Divide by zero");
    }
    value_ /= k;
    return *this;
  }

  [[nodiscard]] auto Get() const -> int32_t { return value_; }

  void Set(int32_t value) { value_ = value; }

  [[nodiscard]] auto ToString() const -> std::string override { return IsNull() ? "(null)" : std::to_string(value_); }

private:
  int32_t value_;
};

class FloatValue : public Value
{
public:
  FloatValue() = delete;
  FloatValue(float value, bool is_null) : Value(FieldType::TYPE_FLOAT, sizeof(float), is_null), value_(value) {}
  explicit FloatValue(const char *mem) : Value(FieldType::TYPE_FLOAT, sizeof(float), false)
  {
    NJUDB_ASSERT(mem != nullptr, "mem is nullptr");
    value_ = *reinterpret_cast<const float *>(mem);
  }
  FloatValue(const FloatValue &value)            = default;
  FloatValue(FloatValue &&value)                 = default;
  FloatValue &operator=(const FloatValue &value) = default;
  FloatValue &operator=(FloatValue &&value)      = default;
  ~FloatValue() override                         = default;

  auto operator==(const Value &value) const -> bool override
  {
    CheckBasic(*this, value);
    if (IsNull() && value.IsNull()) {
      return true;
    }
    return !IsNull() && !value.IsNull() && value_ == dynamic_cast<const FloatValue &>(value).value_;
  }

  auto operator<(const Value &value) const -> bool override
  {
    CheckBasic(*this, value);
    return !IsNull() && !value.IsNull() && value_ < dynamic_cast<const FloatValue &>(value).value_;
  }

  auto operator>(const Value &value) const -> bool override
  {
    CheckBasic(*this, value);
    return !IsNull() && !value.IsNull() && value_ > dynamic_cast<const FloatValue &>(value).value_;
  }

  auto operator+=(const Value &value) -> Value & override
  {
    CheckBasic(*this, value);
    if (IsNull()) {
      *this = dynamic_cast<const FloatValue &>(value);
    } else {
      value_ += dynamic_cast<const FloatValue &>(value).value_;
    }
    return *this;
  }

  auto operator/=(int k) -> Value & override
  {
    if (IsNull()) {
      return *this;
    }
    if (k == 0) {
      NJUDB_THROW(NJUDB_UNEXPECTED_NULL, "Divide by zero");
    }
    value_ /= static_cast<float>(k);
    return *this;
  }

  [[nodiscard]] auto Get() const -> float { return value_; }

  void Set(float value) { value_ = value; }

  [[nodiscard]] auto ToString() const -> std::string override { return IsNull() ? "(null)" : std::to_string(value_); }

private:
  float value_;
};

class BoolValue : public Value
{
public:
  BoolValue() = delete;
  BoolValue(bool value, bool is_null) : Value(FieldType::TYPE_BOOL, sizeof(bool), is_null), value_(value) {}

  explicit BoolValue(const char *mem) : Value(FieldType::TYPE_BOOL, sizeof(bool), false)
  {
    NJUDB_ASSERT(mem != nullptr, "mem is nullptr");
    value_ = *reinterpret_cast<const bool *>(mem);
  }

  BoolValue(const BoolValue &value)            = default;
  BoolValue(BoolValue &&value)                 = default;
  BoolValue &operator=(const BoolValue &value) = default;
  BoolValue &operator=(BoolValue &&value)      = default;
  ~BoolValue() override                        = default;

  auto operator==(const Value &value) const -> bool override
  {
    CheckBasic(*this, value);
    if (IsNull() && value.IsNull()) {
      return true;
    }
    return !IsNull() && !value.IsNull() && value_ == dynamic_cast<const BoolValue &>(value).value_;
  }

  auto operator<(const Value &value) const -> bool override
  {
    CheckBasic(*this, value);
    return !IsNull() && !value.IsNull() && value_ < dynamic_cast<const BoolValue &>(value).value_;
  }

  auto operator>(const Value &value) const -> bool override
  {
    CheckBasic(*this, value);
    return !IsNull() && !value.IsNull() && value_ > dynamic_cast<const BoolValue &>(value).value_;
  }

  [[nodiscard]] auto Get() const -> bool { return value_; }

  void Set(bool value) { value_ = value; }

  [[nodiscard]] auto ToString() const -> std::string override { return IsNull() ? "(null)" : std::to_string(value_); }

private:
  bool value_;
};

class StringValue : public Value
{
public:
  StringValue() = delete;
  StringValue(const char *value, size_t size, bool is_null)
      : Value(FieldType::TYPE_STRING, std::min(size, strlen(value)), is_null),
        value_(value, std::min(size, strlen(value)))
  {
    // resize the string to prune out '\0' characters
    // the given size is larger than the actual string size, so we need to resize it
  }
  StringValue(const StringValue &value)            = default;
  StringValue(StringValue &&value)                 = default;
  StringValue &operator=(const StringValue &value) = default;
  StringValue &operator=(StringValue &&value)      = default;
  ~StringValue() override                          = default;

  auto operator==(const Value &value) const -> bool override
  {
    CheckBasic(*this, value);
    if (IsNull() && value.IsNull()) {
      return true;
    }
    return !IsNull() && !value.IsNull() && value_ == dynamic_cast<const StringValue &>(value).value_;
  }

  auto operator<(const Value &value) const -> bool override
  {
    CheckBasic(*this, value);
    return !IsNull() && !value.IsNull() && value_ < dynamic_cast<const StringValue &>(value).value_;
  }

  auto operator>(const Value &value) const -> bool override
  {
    CheckBasic(*this, value);
    return !IsNull() && !value.IsNull() && value_ > dynamic_cast<const StringValue &>(value).value_;
  }

  auto operator+=(const Value &value) -> Value & override
  {
    CheckBasic(*this, value);
    if (IsNull()) {
      *this = dynamic_cast<const StringValue &>(value);
    } else {
      value_ += dynamic_cast<const StringValue &>(value).value_;
      value_.resize(strlen(value_.c_str()));
    }
    return *this;
  }

  [[nodiscard]] auto Get() const -> const std::string & { return value_; }

  void Set(const char *value, size_t size)
  {
    value_ = std::string(value, size);
    value_.resize(strlen(value_.c_str()));
  }

  void Set(const std::string &value)
  {
    value_ = value;
    value_.resize(strlen(value_.c_str()));
  }

  [[nodiscard]] auto ToString() const -> std::string override { return IsNull() ? "(null)" : value_; }

private:
  std::string value_;
};

class ArrayValue : public Value
{
public:
  ArrayValue() : Value(FieldType::TYPE_ARRAY, 0, false) {}
  explicit ArrayValue(const std::vector<ValueSptr> &values, bool is_null = false)
      : Value(FieldType::TYPE_ARRAY, 0, is_null), values_(values)
  {
    for (const auto &v : values_) {
      size_ += v->GetSize();
    }
  }
  ArrayValue(const ArrayValue &value)            = default;
  ArrayValue(ArrayValue &&value)                 = default;
  ArrayValue &operator=(const ArrayValue &value) = default;
  ArrayValue &operator=(ArrayValue &&value)      = default;
  ~ArrayValue() override                         = default;

  auto operator==(const Value &value) const -> bool override
  {
    CheckBasic(*this, value);
    if (IsNull() && value.IsNull()) {
      return true;
    }
    if (IsNull() || value.IsNull()) {
      return false;
    }
    auto &rhs = dynamic_cast<const ArrayValue &>(value);
    if (values_.size() != rhs.values_.size()) {
      return false;
    }
    for (size_t i = 0; i < values_.size(); i++) {
      if (*values_[i] != *rhs.values_[i]) {
        return false;
      }
    }
    return true;
  }

  auto operator<(const Value &value) const -> bool override
  {
    CheckBasic(*this, value);
    if (IsNull() || value.IsNull()) {
      return false;
    }
    auto &rhs = dynamic_cast<const ArrayValue &>(value);
    if (values_.size() != rhs.values_.size()) {
      return values_.size() < rhs.values_.size();
    }
    for (size_t i = 0; i < values_.size(); i++) {
      if (*values_[i] < *rhs.values_[i]) {
        return true;
      }
    }
    return false;
  }

  auto operator>(const Value &value) const -> bool override
  {
    CheckBasic(*this, value);
    if (IsNull() || value.IsNull()) {
      return false;
    }
    auto &rhs = dynamic_cast<const ArrayValue &>(value);
    if (values_.size() != rhs.values_.size()) {
      return values_.size() > rhs.values_.size();
    }
    for (size_t i = 0; i < values_.size(); i++) {
      if (*values_[i] > *rhs.values_[i]) {
        return true;
      }
    }
    return false;
  }

  auto operator+=(const Value &value) -> Value & override
  {
    CheckBasic(*this, value);
    if (IsNull()) {
      *this = dynamic_cast<const ArrayValue &>(value);
      return *this;
    }
    if (values_.size() != dynamic_cast<const ArrayValue &>(value).values_.size()) {
      NJUDB_THROW(NJUDB_TYPE_MISSMATCH,
          fmt::format("sizes: {}, {}", values_.size(), dynamic_cast<const ArrayValue &>(value).values_.size()));
    }
    for (size_t i = 0; i < values_.size(); i++) {
      *values_[i] += *dynamic_cast<const ArrayValue &>(value).values_[i];
    }
    return *this;
  }

  auto operator/=(int k) -> Value & override
  {
    for (auto &v : values_) {
      *v /= k;
    }
    return *this;
  }

  [[nodiscard]] auto Contains(const ValueSptr &value) const -> bool
  {
    return std::any_of(values_.begin(), values_.end(), [&value](const ValueSptr &v) { return *v == *value; });
  }

  void Append(const ValueSptr &value)
  {
    values_.push_back(value);
    size_ += value->GetSize();
  }

  auto GetValueNum() const -> size_t { return values_.size(); }

  [[nodiscard]] auto Get() const -> const std::vector<ValueSptr> & { return values_; }

  void Set(const std::vector<ValueSptr> &values) { values_ = values; }

  [[nodiscard]] auto ToString() const -> std::string override
  {
    if (IsNull()) {
      return "(null)";
    }
    std::string str = "[";
    for (size_t i = 0; i < values_.size(); i++) {
      str += values_[i]->ToString();
      if (i != values_.size() - 1) {
        str += ", ";
      }
    }
    str += "]";
    return str;
  }

private:
  std::vector<ValueSptr> values_;
};

class ValueFactory
{
public:
  static auto CreateIntValue(int value) -> IntValueSptr { return std::make_shared<IntValue>(value, false); }

  static auto CreateFloatValue(float value) -> FloatValueSptr { return std::make_shared<FloatValue>(value, false); }

  static auto CreateBoolValue(bool value) -> BoolValueSptr { return std::make_shared<BoolValue>(value, false); }

  static auto CreateStringValue(const char *value, size_t size) -> StringValueSptr
  {
    return std::make_shared<StringValue>(value, size, false);
  }

  static auto CreateArrayValue(const std::vector<ValueSptr> &values) -> ArrayValueSptr
  {
    return std::make_shared<ArrayValue>(values, false);
  }

  static auto CreateArrayValue() -> ArrayValueSptr { return std::make_shared<ArrayValue>(); }

  static auto CreateValue(FieldType type, const char *data, size_t size = -1) -> ValueSptr
  {
    switch (type) {
      case FieldType::TYPE_BOOL: return ValueFactory::CreateBoolValue(*reinterpret_cast<const bool *>(data));
      case FieldType::TYPE_INT: return ValueFactory::CreateIntValue(*reinterpret_cast<const int32_t *>(data));
      case FieldType::TYPE_FLOAT: return ValueFactory::CreateFloatValue(*reinterpret_cast<const float *>(data));
      case FieldType::TYPE_STRING: return ValueFactory::CreateStringValue(data, size);
      default: NJUDB_FATAL("Unsupported field type");
    }
  }

  static auto CreateNullValue(FieldType type) -> ValueSptr
  {
    switch (type) {
      case FieldType::TYPE_INT: return std::make_shared<IntValue>(0, true);
      case FieldType::TYPE_FLOAT: return std::make_shared<FloatValue>(0.0f, true);
      case FieldType::TYPE_BOOL: return std::make_shared<BoolValue>(false, true);
      case FieldType::TYPE_STRING: return std::make_shared<StringValue>("", 0, true);
      case FieldType::TYPE_ARRAY: return std::make_shared<ArrayValue>(std::vector<ValueSptr>(), true);
      default: NJUDB_FATAL("Unknown FieldType");
    }
  }

  static void AlignTypes(ValueSptr &lval, ValueSptr &rval)
  {
    if (lval->GetType() == rval->GetType()) {
      return;
    } else if (lval->GetType() == FieldType::TYPE_INT && rval->GetType() == FieldType::TYPE_FLOAT) {
      lval = CreateFloatValue(static_cast<float>(std::dynamic_pointer_cast<IntValue>(lval)->Get()));
    } else if (lval->GetType() == FieldType::TYPE_FLOAT && rval->GetType() == FieldType::TYPE_INT) {
      rval = CreateFloatValue(static_cast<float>(std::dynamic_pointer_cast<IntValue>(rval)->Get()));
    } else {
      NJUDB_THROW(NJUDB_TYPE_MISSMATCH,
          fmt::format(
              "Type mismatch: {} != {}", FieldTypeToString(lval->GetType()), FieldTypeToString(rval->GetType())));
    }
  }

  static auto CastTo(const ValueSptr &value, FieldType type) -> ValueSptr
  {
    if (value->GetType() == type) {
      return value;
    }
    if (value->GetType() == FieldType::TYPE_INT) {
      if (type != FieldType::TYPE_FLOAT) {
        NJUDB_THROW(NJUDB_TYPE_MISSMATCH,
            fmt::format("Type mismatch {} != {}", FieldTypeToString(value->GetType()), FieldTypeToString(type)));
      }
      if (value->IsNull()) {
        return CreateNullValue(type);
      }
      return CreateFloatValue(static_cast<float>(std::dynamic_pointer_cast<IntValue>(value)->Get()));
    } else if (value->GetType() == FieldType::TYPE_FLOAT) {
      if (type != FieldType::TYPE_INT) {
        NJUDB_THROW(NJUDB_TYPE_MISSMATCH,
            fmt::format("Type mismatch {} != {}", FieldTypeToString(value->GetType()), FieldTypeToString(type)));
      }
      if (value->IsNull()) {
        return CreateNullValue(type);
      }
      return CreateIntValue(static_cast<int>(std::dynamic_pointer_cast<FloatValue>(value)->Get()));
    } else {
      NJUDB_THROW(NJUDB_TYPE_MISSMATCH,
          fmt::format("Type mismatch {} != {}", FieldTypeToString(value->GetType()), FieldTypeToString(type)));
    }
  }

  static auto CreateMinValueForType(FieldType type) -> ValueSptr
  {
    switch (type) {
      case TYPE_INT:
        return CreateIntValue(std::numeric_limits<int32_t>::min());
      case TYPE_FLOAT:
        return CreateFloatValue(-std::numeric_limits<float>::max());
      case TYPE_BOOL:
        return CreateBoolValue(false);
      case TYPE_STRING:
        return CreateStringValue("", 0);
      default:
        NJUDB_THROW(NJUDB_TYPE_MISSMATCH, "Unsupported field type for min value");
    }
  }

  static auto CreateMaxValueForType(FieldType type) -> ValueSptr
  {
    switch (type) {
      case TYPE_INT:
        return CreateIntValue(std::numeric_limits<int32_t>::max());
      case TYPE_FLOAT:
        return CreateFloatValue(std::numeric_limits<float>::max());
      case TYPE_BOOL:
        return CreateBoolValue(true);
      case TYPE_STRING:
        // Create a large string for max comparison
        return CreateStringValue("\xFF\xFF\xFF\xFF", 4);
      default:
        NJUDB_THROW(NJUDB_TYPE_MISSMATCH, "Unsupported field type for max value");
    }
  }
};

}  // namespace njudb

#endif  // NJUDB_VALUE_H
