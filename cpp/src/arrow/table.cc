// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
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

#include "arrow/table.h"

#include <algorithm>
#include <cstdlib>
#include <limits>
#include <memory>
#include <sstream>
#include <utility>

#include "arrow/array.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/util/stl.h"

namespace arrow {

using internal::checked_cast;

Column::Column(const std::shared_ptr<Field>& field, const ArrayVector& chunks)
    : field_(field) {
  data_ = std::make_shared<ChunkedArray>(chunks, field->type());
}

Column::Column(const std::shared_ptr<Field>& field, const std::shared_ptr<Array>& data)
    : field_(field) {
  if (!data) {
    data_ = std::make_shared<ChunkedArray>(ArrayVector({}), field->type());
  } else {
    data_ = std::make_shared<ChunkedArray>(ArrayVector({data}), field->type());
  }
}

Column::Column(const std::string& name, const std::shared_ptr<Array>& data)
    : Column(::arrow::field(name, data->type()), data) {}

Column::Column(const std::string& name, const std::shared_ptr<ChunkedArray>& data)
    : Column(::arrow::field(name, data->type()), data) {}

Column::Column(const std::shared_ptr<Field>& field,
               const std::shared_ptr<ChunkedArray>& data)
    : field_(field), data_(data) {}

Status Column::Flatten(MemoryPool* pool,
                       std::vector<std::shared_ptr<Column>>* out) const {
  std::vector<std::shared_ptr<Column>> flattened;
  std::vector<std::shared_ptr<Field>> flattened_fields = field_->Flatten();
  std::vector<std::shared_ptr<ChunkedArray>> flattened_data;
  RETURN_NOT_OK(data_->Flatten(pool, &flattened_data));
  DCHECK_EQ(flattened_fields.size(), flattened_data.size());
  for (size_t i = 0; i < flattened_fields.size(); ++i) {
    flattened.push_back(std::make_shared<Column>(flattened_fields[i], flattened_data[i]));
  }
  *out = flattened;
  return Status::OK();
}

bool Column::Equals(const Column& other) const {
  if (!field_->Equals(other.field())) {
    return false;
  }
  return data_->Equals(other.data());
}

bool Column::Equals(const std::shared_ptr<Column>& other) const {
  if (this == other.get()) {
    return true;
  }
  if (!other) {
    return false;
  }

  return Equals(*other.get());
}

Status Column::ValidateData() {
  for (int i = 0; i < data_->num_chunks(); ++i) {
    std::shared_ptr<DataType> type = data_->chunk(i)->type();
    if (!this->type()->Equals(type)) {
      return Status::Invalid("In chunk ", i, " expected type ", this->type()->ToString(),
                             " but saw ", type->ToString());
    }
  }
  return Status::OK();
}

// ----------------------------------------------------------------------
// Table methods

/// \class SimpleTable
/// \brief A basic, non-lazy in-memory table, like SimpleRecordBatch
class SimpleTable : public Table {
 public:
  SimpleTable(const std::shared_ptr<Schema>& schema,
              const std::vector<std::shared_ptr<Column>>& columns, int64_t num_rows = -1)
      : columns_(columns) {
    schema_ = schema;
    if (num_rows < 0) {
      if (columns.size() == 0) {
        num_rows_ = 0;
      } else {
        num_rows_ = columns[0]->length();
      }
    } else {
      num_rows_ = num_rows;
    }
  }

  SimpleTable(const std::shared_ptr<Schema>& schema,
              const std::vector<std::shared_ptr<Array>>& columns, int64_t num_rows = -1) {
    schema_ = schema;
    if (num_rows < 0) {
      if (columns.size() == 0) {
        num_rows_ = 0;
      } else {
        num_rows_ = columns[0]->length();
      }
    } else {
      num_rows_ = num_rows;
    }

    columns_.resize(columns.size());
    for (size_t i = 0; i < columns.size(); ++i) {
      columns_[i] =
          std::make_shared<Column>(schema->field(static_cast<int>(i)), columns[i]);
    }
  }

  std::shared_ptr<Column> column(int i) const override { return columns_[i]; }

  Status RemoveColumn(int i, std::shared_ptr<Table>* out) const override {
    std::shared_ptr<Schema> new_schema;
    RETURN_NOT_OK(schema_->RemoveField(i, &new_schema));

    *out = Table::Make(new_schema, internal::DeleteVectorElement(columns_, i),
                       this->num_rows());
    return Status::OK();
  }

  Status AddColumn(int i, const std::shared_ptr<Column>& col,
                   std::shared_ptr<Table>* out) const override {
    DCHECK(col != nullptr);

    if (col->length() != num_rows_) {
      return Status::Invalid(
          "Added column's length must match table's length. Expected length ", num_rows_,
          " but got length ", col->length());
    }

    std::shared_ptr<Schema> new_schema;
    RETURN_NOT_OK(schema_->AddField(i, col->field(), &new_schema));

    *out = Table::Make(new_schema, internal::AddVectorElement(columns_, i, col));
    return Status::OK();
  }

  Status SetColumn(int i, const std::shared_ptr<Column>& col,
                   std::shared_ptr<Table>* out) const override {
    DCHECK(col != nullptr);

    if (col->length() != num_rows_) {
      return Status::Invalid(
          "Added column's length must match table's length. Expected length ", num_rows_,
          " but got length ", col->length());
    }

    std::shared_ptr<Schema> new_schema;
    RETURN_NOT_OK(schema_->SetField(i, col->field(), &new_schema));

    *out = Table::Make(new_schema, internal::ReplaceVectorElement(columns_, i, col));
    return Status::OK();
  }

  std::shared_ptr<Table> ReplaceSchemaMetadata(
      const std::shared_ptr<const KeyValueMetadata>& metadata) const override {
    auto new_schema = schema_->AddMetadata(metadata);
    return Table::Make(new_schema, columns_);
  }

  Status Flatten(MemoryPool* pool, std::shared_ptr<Table>* out) const override {
    std::vector<std::shared_ptr<Field>> flattened_fields;
    std::vector<std::shared_ptr<Column>> flattened_columns;
    for (const auto& column : columns_) {
      std::vector<std::shared_ptr<Column>> new_columns;
      RETURN_NOT_OK(column->Flatten(pool, &new_columns));
      for (const auto& new_col : new_columns) {
        flattened_fields.push_back(new_col->field());
        flattened_columns.push_back(new_col);
      }
    }
    auto flattened_schema =
        std::make_shared<Schema>(flattened_fields, schema_->metadata());
    *out = Table::Make(flattened_schema, flattened_columns);
    return Status::OK();
  }

  Status Validate() const override {
    // Make sure columns and schema are consistent
    if (static_cast<int>(columns_.size()) != schema_->num_fields()) {
      return Status::Invalid("Number of columns did not match schema");
    }
    for (int i = 0; i < num_columns(); ++i) {
      const Column* col = columns_[i].get();
      if (col == nullptr) {
        return Status::Invalid("Column ", i, " was null");
      }
      if (!col->field()->Equals(*schema_->field(i))) {
        return Status::Invalid("Column field ", i, " named ", col->name(),
                               " is inconsistent with schema");
      }
    }

    // Make sure columns are all the same length
    for (int i = 0; i < num_columns(); ++i) {
      const Column* col = columns_[i].get();
      if (col->length() != num_rows_) {
        return Status::Invalid("Column ", i, " named ", col->name(), " expected length ",
                               num_rows_, " but got length ", col->length());
      }
    }
    return Status::OK();
  }

 private:
  std::vector<std::shared_ptr<Column>> columns_;
};

Table::Table() : num_rows_(0) {}

std::shared_ptr<Table> Table::Make(const std::shared_ptr<Schema>& schema,
                                   const std::vector<std::shared_ptr<Column>>& columns,
                                   int64_t num_rows) {
  return std::make_shared<SimpleTable>(schema, columns, num_rows);
}

std::shared_ptr<Table> Table::Make(const std::shared_ptr<Schema>& schema,
                                   const std::vector<std::shared_ptr<Array>>& arrays,
                                   int64_t num_rows) {
  return std::make_shared<SimpleTable>(schema, arrays, num_rows);
}

Status Table::FromRecordBatches(const std::shared_ptr<Schema>& schema,
                                const std::vector<std::shared_ptr<RecordBatch>>& batches,
                                std::shared_ptr<Table>* table) {
  const int nbatches = static_cast<int>(batches.size());
  const int ncolumns = static_cast<int>(schema->num_fields());

  for (int i = 0; i < nbatches; ++i) {
    if (!batches[i]->schema()->Equals(*schema, false)) {
      return Status::Invalid("Schema at index ", static_cast<int>(i),
                             " was different: \n", schema->ToString(), "\nvs\n",
                             batches[i]->schema()->ToString());
    }
  }

  std::vector<std::shared_ptr<Column>> columns(ncolumns);
  std::vector<std::shared_ptr<Array>> column_arrays(nbatches);

  for (int i = 0; i < ncolumns; ++i) {
    for (int j = 0; j < nbatches; ++j) {
      column_arrays[j] = batches[j]->column(i);
    }
    columns[i] = std::make_shared<Column>(schema->field(i), column_arrays);
  }

  *table = Table::Make(schema, columns);
  return Status::OK();
}

Status Table::FromRecordBatches(const std::vector<std::shared_ptr<RecordBatch>>& batches,
                                std::shared_ptr<Table>* table) {
  if (batches.size() == 0) {
    return Status::Invalid("Must pass at least one record batch");
  }

  return FromRecordBatches(batches[0]->schema(), batches, table);
}

Status ConcatenateTables(const std::vector<std::shared_ptr<Table>>& tables,
                         std::shared_ptr<Table>* table) {
  if (tables.size() == 0) {
    return Status::Invalid("Must pass at least one table");
  }

  std::shared_ptr<Schema> schema = tables[0]->schema();

  const int ntables = static_cast<int>(tables.size());
  const int ncolumns = static_cast<int>(schema->num_fields());

  for (int i = 1; i < ntables; ++i) {
    if (!tables[i]->schema()->Equals(*schema, false)) {
      return Status::Invalid("Schema at index ", static_cast<int>(i),
                             " was different: \n", schema->ToString(), "\nvs\n",
                             tables[i]->schema()->ToString());
    }
  }

  std::vector<std::shared_ptr<Column>> columns(ncolumns);
  for (int i = 0; i < ncolumns; ++i) {
    std::vector<std::shared_ptr<Array>> column_arrays;
    for (int j = 0; j < ntables; ++j) {
      const std::vector<std::shared_ptr<Array>>& chunks =
          tables[j]->column(i)->data()->chunks();
      for (const auto& chunk : chunks) {
        column_arrays.push_back(chunk);
      }
    }
    columns[i] = std::make_shared<Column>(schema->field(i), column_arrays);
  }
  *table = Table::Make(schema, columns);
  return Status::OK();
}

bool Table::Equals(const Table& other) const {
  if (this == &other) {
    return true;
  }
  if (!schema_->Equals(*other.schema())) {
    return false;
  }
  if (this->num_columns() != other.num_columns()) {
    return false;
  }

  for (int i = 0; i < this->num_columns(); i++) {
    if (!this->column(i)->Equals(other.column(i))) {
      return false;
    }
  }
  return true;
}

// ----------------------------------------------------------------------
// Convert a table to a sequence of record batches

class TableBatchReader::TableBatchReaderImpl {
 public:
  explicit TableBatchReaderImpl(const Table& table)
      : table_(table),
        column_data_(table.num_columns()),
        chunk_numbers_(table.num_columns(), 0),
        chunk_offsets_(table.num_columns(), 0),
        absolute_row_position_(0),
        max_chunksize_(std::numeric_limits<int64_t>::max()) {
    for (int i = 0; i < table.num_columns(); ++i) {
      column_data_[i] = table.column(i)->data().get();
    }
  }

  Status ReadNext(std::shared_ptr<RecordBatch>* out) {
    if (absolute_row_position_ == table_.num_rows()) {
      *out = nullptr;
      return Status::OK();
    }

    // Determine the minimum contiguous slice across all columns
    int64_t chunksize = std::min(table_.num_rows(), max_chunksize_);
    std::vector<const Array*> chunks(table_.num_columns());
    for (int i = 0; i < table_.num_columns(); ++i) {
      auto chunk = column_data_[i]->chunk(chunk_numbers_[i]).get();
      int64_t chunk_remaining = chunk->length() - chunk_offsets_[i];

      if (chunk_remaining < chunksize) {
        chunksize = chunk_remaining;
      }

      chunks[i] = chunk;
    }

    // Slice chunks and advance chunk index as appropriate
    std::vector<std::shared_ptr<ArrayData>> batch_data(table_.num_columns());

    for (int i = 0; i < table_.num_columns(); ++i) {
      // Exhausted chunk
      const Array* chunk = chunks[i];
      const int64_t offset = chunk_offsets_[i];
      std::shared_ptr<ArrayData> slice_data;
      if ((chunk->length() - offset) == chunksize) {
        ++chunk_numbers_[i];
        chunk_offsets_[i] = 0;
        if (offset > 0) {
          // Need to slice
          slice_data = chunk->Slice(offset, chunksize)->data();
        } else {
          // No slice
          slice_data = chunk->data();
        }
      } else {
        chunk_offsets_[i] += chunksize;
        slice_data = chunk->Slice(offset, chunksize)->data();
      }
      batch_data[i] = std::move(slice_data);
    }

    absolute_row_position_ += chunksize;
    *out = RecordBatch::Make(table_.schema(), chunksize, std::move(batch_data));

    return Status::OK();
  }

  std::shared_ptr<Schema> schema() const { return table_.schema(); }

  void set_chunksize(int64_t chunksize) { max_chunksize_ = chunksize; }

 private:
  const Table& table_;
  std::vector<ChunkedArray*> column_data_;
  std::vector<int> chunk_numbers_;
  std::vector<int64_t> chunk_offsets_;
  int64_t absolute_row_position_;
  int64_t max_chunksize_;
};

TableBatchReader::TableBatchReader(const Table& table) {
  impl_.reset(new TableBatchReaderImpl(table));
}

TableBatchReader::~TableBatchReader() {}

std::shared_ptr<Schema> TableBatchReader::schema() const { return impl_->schema(); }

void TableBatchReader::set_chunksize(int64_t chunksize) {
  impl_->set_chunksize(chunksize);
}

Status TableBatchReader::ReadNext(std::shared_ptr<RecordBatch>* out) {
  return impl_->ReadNext(out);
}

}  // namespace arrow
