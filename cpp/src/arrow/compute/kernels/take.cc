// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// returnGegarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <memory>
#include <utility>

#include "arrow/builder.h"
#include "arrow/compute/context.h"
#include "arrow/compute/kernels/take.h"
#include "arrow/util/logging.h"
#include "arrow/visitor_inline.h"

namespace arrow {
namespace compute {

Status Take(FunctionContext* context, const Array& values, const Array& indices,
            const TakeOptions& options, std::shared_ptr<Array>* out) {
  TakeKernel kernel(values.type(), options);
  Datum out_datum;
  RETURN_NOT_OK(kernel.Call(context, values.data(), indices.data(), &out_datum));
  *out = out_datum.make_array();
  return Status::OK();
}

struct TakeParameters {
  FunctionContext* context;
  std::shared_ptr<Array> values, indices;
  TakeOptions options;
  std::shared_ptr<Array>* out;
};

template <typename Builder, typename Scalar>
Status UnsafeAppend(Builder* builder, Scalar&& value) {
  builder->UnsafeAppend(std::forward<Scalar>(value));
  return Status::OK();
}

Status UnsafeAppend(BinaryBuilder* builder, util::string_view value) {
  RETURN_NOT_OK(builder->ReserveData(static_cast<int64_t>(value.size())));
  builder->UnsafeAppend(value);
  return Status::OK();
}

Status UnsafeAppend(StringBuilder* builder, util::string_view value) {
  RETURN_NOT_OK(builder->ReserveData(static_cast<int64_t>(value.size())));
  builder->UnsafeAppend(value);
  return Status::OK();
}

template <int OutOfBounds, bool AllValuesValid, bool AllIndicesValid, typename ValueArray,
          typename IndexArray, typename OutBuilder>
Status TakeImpl(FunctionContext*, const ValueArray& values, const IndexArray& indices,
                OutBuilder* builder) {
  for (int64_t i = 0; i != indices.length(); ++i) {
    if (!AllIndicesValid && indices.IsNull(i)) {
      builder->UnsafeAppendNull();
      continue;
    }
    auto index = indices.raw_values()[i];
    if (OutOfBounds == TakeOptions::ERROR &&
        static_cast<int64_t>(index) >= values.length()) {
      return Status::Invalid("take index out of bounds");
    }
    if (OutOfBounds == TakeOptions::TONULL &&
        static_cast<int64_t>(index) >= values.length()) {
      builder->UnsafeAppendNull();
      continue;
    }
    if (!AllValuesValid && values.IsNull(index)) {
      builder->UnsafeAppendNull();
      continue;
    }
    RETURN_NOT_OK(UnsafeAppend(builder, values.GetView(index)));
  }
  return Status::OK();
}

template <int OutOfBounds, bool AllValuesValid, typename ValueArray, typename IndexArray,
          typename OutBuilder>
Status TakeImpl(FunctionContext* context, const ValueArray& values,
                const IndexArray& indices, OutBuilder* builder) {
  if (indices.null_count() == 0) {
    return TakeImpl<OutOfBounds, AllValuesValid, true>(context, values, indices, builder);
  }
  return TakeImpl<OutOfBounds, AllValuesValid, false>(context, values, indices, builder);
}

template <int OutOfBounds, typename ValueArray, typename IndexArray, typename OutBuilder>
Status TakeImpl(FunctionContext* context, const ValueArray& values,
                const IndexArray& indices, OutBuilder* builder) {
  if (values.null_count() == 0) {
    return TakeImpl<OutOfBounds, true>(context, values, indices, builder);
  }
  return TakeImpl<OutOfBounds, false>(context, values, indices, builder);
}

template <typename ValueArray, typename IndexArray, typename OutBuilder>
Status TakeImpl(FunctionContext* context, const ValueArray& values,
                const IndexArray& indices, const TakeOptions& options,
                OutBuilder* builder) {
  switch (options.out_of_bounds) {
    case TakeOptions::ERROR:
      return TakeImpl<TakeOptions::ERROR>(context, values, indices, builder);
    case TakeOptions::TONULL:
      return TakeImpl<TakeOptions::TONULL>(context, values, indices, builder);
    case TakeOptions::UNSAFE:
      return TakeImpl<TakeOptions::UNSAFE>(context, values, indices, builder);
    default:
      ARROW_LOG(FATAL) << "how did we get here";
      return Status::NotImplemented("how did we get here");
  }
}

template <typename IndexType>
struct UnpackValues {
  using IndexArrayRef = const typename TypeTraits<IndexType>::ArrayType&;

  template <typename ValueType>
  Status Visit(const ValueType&) {
    using ValueArrayRef = const typename TypeTraits<ValueType>::ArrayType&;
    using OutBuilder = typename TypeTraits<ValueType>::BuilderType;
    IndexArrayRef indices = static_cast<IndexArrayRef>(*params_.indices);
    ValueArrayRef values = static_cast<ValueArrayRef>(*params_.values);
    std::unique_ptr<ArrayBuilder> builder;
    RETURN_NOT_OK(MakeBuilder(params_.context->memory_pool(), values.type(), &builder));
    RETURN_NOT_OK(builder->Reserve(indices.length()));
    RETURN_NOT_OK(TakeImpl(params_.context, values, indices, params_.options,
                           static_cast<OutBuilder*>(builder.get())));
    return builder->Finish(params_.out);
  }

  Status Visit(const NullType& t) {
    auto indices_length = params_.indices->length();
    if (params_.options.out_of_bounds == TakeOptions::ERROR && indices_length != 0) {
      auto indices = static_cast<IndexArrayRef>(*params_.indices).raw_values();
      auto max = *std::max_element(indices, indices + indices_length);
      if (static_cast<int64_t>(max) > params_.values->length()) {
        return Status::Invalid("out of bounds index");
      }
    }
    params_.out->reset(new NullArray(indices_length));
    return Status::OK();
  }

  Status Visit(const DictionaryType& t) {
    UnpackValues<IndexType> unpack = {params_};
    RETURN_NOT_OK(VisitTypeInline(*t.index_type(), &unpack));
    (*params_.out)->data()->type = dictionary(t.index_type(), t.dictionary());
    return Status::OK();
  }

  Status Visit(const ExtensionType& t) {
    // XXX can we just take from its storage?
    return Status::NotImplemented("gathering values of type ", t);
  }

  Status Visit(const UnionType& t) {
    return Status::NotImplemented("gathering values of type ", t);
  }

  Status Visit(const ListType& t) {
    return Status::NotImplemented("gathering values of type ", t);
  }

  Status Visit(const StructType& t) {
    return Status::NotImplemented("gathering values of type ", t);
  }

  const TakeParameters& params_;
};

struct UnpackIndices {
  template <typename IndexType>
  enable_if_integer<IndexType, Status> Visit(const IndexType&) {
    UnpackValues<IndexType> unpack = {params_};
    return VisitTypeInline(*params_.values->type(), &unpack);
  }
  Status Visit(const DataType& other) {
    return Status::Invalid("index type not supported: ", other);
  }
  const TakeParameters& params_;
};

Status TakeKernel::Call(FunctionContext* ctx, const Datum& values, const Datum& indices,
                        Datum* out) {
  if (!values.is_array() || !indices.is_array()) {
    return Status::Invalid("TakeKernel expects array values and indices");
  }
  std::shared_ptr<Array> out_array;
  TakeParameters params;
  params.context = ctx;
  params.values = values.make_array();
  params.indices = indices.make_array();
  params.options = options_;
  params.out = &out_array;
  UnpackIndices unpack = {params};
  RETURN_NOT_OK(VisitTypeInline(*indices.type(), &unpack));
  *out = Datum(out_array);
  return Status::OK();
}

}  // namespace compute
}  // namespace arrow
