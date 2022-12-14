#include <iostream>
#include <memory>
#include <utility>
#include <vector>
#include <string>

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/dataset/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/compute/api.h>
#include <arrow/compute/api_vector.h>
#include <arrow/compute/cast.h>
#include <arrow/compute/exec/exec_plan.h>
#include <arrow/compute/exec/expression.h>

#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/path_util.h>
#include <arrow/util/future.h>
#include <arrow/util/range.h>
#include <arrow/util/thread_pool.h>
#include <arrow/util/vector.h>

namespace cp = arrow::compute;

arrow::Result<std::shared_ptr<arrow::Buffer>> PackRecordBatch(const std::shared_ptr<arrow::RecordBatch>& batch) {
  ARROW_ASSIGN_OR_RAISE(auto buffer_output_stream,
                        arrow::io::BufferOutputStream::Create());

  auto options = arrow::ipc::IpcWriteOptions::Defaults();
  auto codec = arrow::Compression::LZ4_FRAME;

  ARROW_ASSIGN_OR_RAISE(options.codec, arrow::util::Codec::Create(codec));
  ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeStreamWriter(
                                        buffer_output_stream, batch->schema(), options));

  ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
  ARROW_RETURN_NOT_OK(writer->Close());

  ARROW_ASSIGN_OR_RAISE(auto buffer, buffer_output_stream->Finish());
  return buffer;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> UnpackRecordBatch(uint8_t* data, int32_t size) {
  auto buffer = std::make_shared<arrow::Buffer>(data, size);
  auto buffer_reader = std::make_shared<arrow::io::BufferReader>(buffer);
  auto options = arrow::ipc::IpcReadOptions::Defaults();
  ARROW_ASSIGN_OR_RAISE(
      auto reader, arrow::ipc::RecordBatchStreamReader::Open(buffer_reader, options));
  ARROW_ASSIGN_OR_RAISE(auto batches, reader->ToRecordBatches());
  return batches[0];
}

arrow::compute::Expression GetFilter(std::string selectivity) {
  if (selectivity == "100") {
      return arrow::compute::greater(arrow::compute::field_ref("total_amount"),
                                      arrow::compute::literal(-200));
  } else if (selectivity == "10") {
      return arrow::compute::greater(arrow::compute::field_ref("total_amount"),
                                      arrow::compute::literal(27));
  } else if (selectivity == "1") {
      return arrow::compute::greater(arrow::compute::field_ref("total_amount"),
                                      arrow::compute::literal(69));
  } else {
    return arrow::compute::literal(true);
  }
}

arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> ScanDataset(cp::ExecContext& exec_context, std::string backend, std::string selectivity) {
  std::string uri = "file:///mnt/cephfs/dataset";

  auto schema = arrow::schema({
    arrow::field("VendorID", arrow::int64()),
    arrow::field("tpep_pickup_datetime", arrow::timestamp(arrow::TimeUnit::MICRO)),
    arrow::field("tpep_dropoff_datetime", arrow::timestamp(arrow::TimeUnit::MICRO)),
    arrow::field("passenger_count", arrow::int64()),
    arrow::field("trip_distance", arrow::float64()),
    arrow::field("RatecodeID", arrow::int64()),
    arrow::field("store_and_fwd_flag", arrow::utf8()),
    arrow::field("PULocationID", arrow::int64()),
    arrow::field("DOLocationID", arrow::int64()),
    arrow::field("payment_type", arrow::int64()),
    arrow::field("fare_amount", arrow::float64()),
    arrow::field("extra", arrow::float64()),
    arrow::field("mta_tax", arrow::float64()),
    arrow::field("tip_amount", arrow::float64()),
    arrow::field("tolls_amount", arrow::float64()),
    arrow::field("improvement_surcharge", arrow::float64()),
    arrow::field("total_amount", arrow::float64())
  });
  
  std::string path;
  ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::FileSystemFromUri(uri, &path)); 
  auto format = std::make_shared<arrow::dataset::ParquetFileFormat>();
    
  arrow::fs::FileSelector s;
  s.base_dir = std::move(path);
  s.recursive = true;

  arrow::dataset::FileSystemFactoryOptions options;
  ARROW_ASSIGN_OR_RAISE(auto factory, 
    arrow::dataset::FileSystemDatasetFactory::Make(std::move(fs), s, std::move(format), options));
  arrow::dataset::FinishOptions finish_options;
  ARROW_ASSIGN_OR_RAISE(auto dataset,factory->Finish(finish_options));

  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<cp::ExecPlan> plan,
                        cp::ExecPlan::Make(&exec_context));

  ARROW_ASSIGN_OR_RAISE(auto scanner_builder, dataset->NewScan());
  ARROW_RETURN_NOT_OK(scanner_builder->Filter(GetFilter(selectivity)));
  ARROW_RETURN_NOT_OK(scanner_builder->Project(schema->field_names()));

  ARROW_ASSIGN_OR_RAISE(auto scanner, scanner_builder->Finish());

  std::shared_ptr<arrow::RecordBatchReader> reader; 
  if (backend == "dataset") {
    std::cout << "Using dataset backend: " << uri << std::endl;
    ARROW_ASSIGN_OR_RAISE(reader, scanner->ToRecordBatchReader());
  } else if (backend == "dataset+mem") {
    std::cout << "Using dataset+mem backend: " << uri << std::endl;
    ARROW_ASSIGN_OR_RAISE(auto table, scanner->ToTable())
    auto im_ds = std::make_shared<arrow::dataset::InMemoryDataset>(table);
    ARROW_ASSIGN_OR_RAISE(auto im_ds_scanner_builder, im_ds->NewScan());
    ARROW_ASSIGN_OR_RAISE(auto im_ds_scanner, im_ds_scanner_builder->Finish());
    ARROW_ASSIGN_OR_RAISE(reader, im_ds_scanner->ToRecordBatchReader());
  }

  return reader;
}
