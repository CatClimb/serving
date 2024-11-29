/*  

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
==============================================================================*/

// libevent based request implementation

#include "tensorflow_serving/util/net_http/server/internal/evhttp_request.h"
#include <thread>  // 引入线程库
#include <chrono> 
#include <zlib.h>
#include <iostream>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "absl/strings/match.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "libevent/include/event2/buffer.h"
#include "libevent/include/event2/event.h"
#include "libevent/include/event2/http.h"
#include "libevent/include/event2/keyvalq_struct.h"
#include "tensorflow_serving/util/net_http/compression/gzip_zlib.h"
#include "tensorflow_serving/util/net_http/internal/net_logging.h"
#include "tensorflow_serving/util/net_http/public/header_names.h"

namespace tensorflow {
namespace serving {
namespace net_http {

ParsedEvRequest::~ParsedEvRequest() {
  if (decoded_uri) {
    evhttp_uri_free(decoded_uri);
  }

  if (request && evhttp_request_is_owned(request)) {
    evhttp_request_free(request);
  }
}

ParsedEvRequest::ParsedEvRequest(evhttp_request* request_in)
    : request(request_in) {}

bool ParsedEvRequest::decode() {
  switch (evhttp_request_get_command(request)) {
    case EVHTTP_REQ_GET:
      method = "GET";
      break;
    case EVHTTP_REQ_POST:
      method = "POST";
      break;
    case EVHTTP_REQ_HEAD:
      method = "HEAD";
      break;
    case EVHTTP_REQ_PUT:
      method = "PUT";
      break;
    case EVHTTP_REQ_DELETE:
      method = "DELETE";
      break;
    case EVHTTP_REQ_OPTIONS:
      method = "OPTIONS";
      break;
    case EVHTTP_REQ_TRACE:
      method = "TRACE";
      break;
    case EVHTTP_REQ_CONNECT:
      method = "CONNECT";
      break;
    case EVHTTP_REQ_PATCH:
      method = "PATCH";
      break;
    default:
      return false;
  }

  uri = evhttp_request_get_uri(request);

  decoded_uri = evhttp_uri_parse(uri);
  if (decoded_uri == nullptr) {
    return false;
  }

  // NB: need double-check "/" is OK
  path = evhttp_uri_get_path(decoded_uri);
  if (path == nullptr) {
    path = "/";
  }

  path_and_query = path;
  const char* query = evhttp_uri_get_query(decoded_uri);
  if (query != nullptr) {
    path_and_query.push_back('?');
    path_and_query.append(query);
  }

  const char* fragment = evhttp_uri_get_fragment(decoded_uri);
  if (fragment != nullptr) {
    path_and_query.push_back('#');
    path_and_query.append(fragment);
  }

  headers = evhttp_request_get_input_headers(request);

  return true;
}

EvHTTPRequest::EvHTTPRequest(std::unique_ptr<ParsedEvRequest> request,
                             ServerSupport* server)
    : server_(server),
      parsed_request_(std::move(request)),
      output_buf(nullptr) {}

EvHTTPRequest::~EvHTTPRequest() {
  if (output_buf != nullptr) {
    evbuffer_free(output_buf);
  }
}

absl::string_view EvHTTPRequest::uri_path() const {
  return parsed_request_->path_and_query;
}

absl::string_view EvHTTPRequest::http_method() const {
  return parsed_request_->method;
}

bool EvHTTPRequest::Initialize() {
  output_buf = evbuffer_new();
  return output_buf != nullptr;
}

void EvHTTPRequest::WriteResponseBytes(const char* data, int64_t size) {
  assert(size >= 0);
  if (output_buf == nullptr) {
    NET_LOG(FATAL, "Request not initialized.");
    return;
  }

  
}
void EvHTTPRequest::StreamResponse(absl::string_view data,HTTPStatusCode status){
  int64_t data_size = static_cast<int64_t>(data.size());
  int64_t chunk_size=1024;
  int64_t offset=0;
  int64_t remaining_size;
  int64_t current_chunk_size;
  int64_t chunk_count=0;
  const char* chunk_data="";
    //分块响应开启
  evhttp_send_reply_start(parsed_request_->request,static_cast<int>(status),"OK");
  std::cout << "分块响应发送开启" << std::endl;
  while(offset < data_size){
    chunk_count++;
    std::cout << "块数id：" <<chunk_count<<std::endl;
    // std::this_thread::sleep_for(std::chrono::milliseconds(10));
    remaining_size = data_size-offset;
    current_chunk_size = (remaining_size < chunk_size) ? remaining_size : chunk_size;
    chunk_data = data.substr(offset, current_chunk_size).data();
    offset+=current_chunk_size;
    //分块数据写入
    std::cout << "分块数据写入" << std::endl;
    NET_LOG(OK, "块数id：",chunk_count);
    int ret = evbuffer_add(output_buf, chunk_data, current_chunk_size);
    if (ret == -1) {
      std::cout << "分块写入缓存失败" << std::endl;
      NET_LOG(ERROR, "Failed to write %zu bytes data to output buffer",
              data_size);
    }
    //分块响应
    size_t buffer_len = evbuffer_get_length(output_buf); // 获取缓冲区中数据的长度
    // if (buffer_len > 0) {
    //     std::string data(buffer_len, '\0');  
    //     evbuffer_copyout(output_buf, &data[0], buffer_len);
    //     // 打印转换后的字符串
    //     std::cout << "分块响应之前数据: " << data << std::endl;
    //     std::cout << "分块响应之前大小:" << buffer_len << std::endl;
    // } else {
    //     std::cout << "分块响应之前数据为空:" << std::endl;
    // }
    evhttp_send_reply_chunk(parsed_request_->request,output_buf);
    // size_t buffer_len1 = evbuffer_get_length(output_buf); 
    // if (buffer_len1 > 0) {
    //     std::string data1(buffer_len1, '\0');  
    //     evbuffer_copyout(output_buf, &data1[0], buffer_len1); 
    //     std::cout << "分块响应之后数据: " << data1 << std::endl;
    //     std::cout << "分块响应之后大小:" << buffer_len1 << std::endl;
    // } else {
    //     std::cout << "分块响应之后数据为空:" << std::endl;
    // }
    std::cout << "分块响应" << std::endl;
  }
  evhttp_request* request_1 =parsed_request_->request;
  bool result =
    server_->EventLoopSchedule([this, request_1]() {
       EvSendReply2(request_1);
       });
  if (!result) {
    NET_LOG(ERROR, "Failed to EventLoopSchedule ReplyWithStatus()");
    Abort();
    // TODO(wenboz): should have a forced abort that doesn't write back anything
    // to the event-loop
  }
}

void EvHTTPRequest::WriteResponseString(absl::string_view data) {
  WriteResponseBytes(data.data(), static_cast<int64_t>(data.size()));
}

std::unique_ptr<char[], ServerRequestInterface::BlockDeleter>
EvHTTPRequest::ReadRequestBytes(int64_t* size) {
  evbuffer* input_buf =
      evhttp_request_get_input_buffer(parsed_request_->request);
  if (input_buf == nullptr) {
    *size = 0;
    return nullptr;  // no body
  }

  // possible a reentry after gzip uncompression
  if (evbuffer_get_length(input_buf) == 0) {
    *size = 0;
    return nullptr;  // EOF
  }

  // Uncompress the entire body
  if (NeedUncompressGzipContent()) {
    return ReadRequestGzipBytes(input_buf, size);
  }

  auto buf_size = reinterpret_cast<size_t*>(size);

  *buf_size = evbuffer_get_contiguous_space(input_buf);
  assert(*buf_size > 0);

  char* block = std::allocator<char>().allocate(*buf_size);
  int ret = evbuffer_remove(input_buf, block, *buf_size);

  if (ret != *buf_size) {
    NET_LOG(ERROR, "Unexpected: read less than specified num_bytes : %zu",
            *buf_size);
    std::allocator<char>().deallocate(block, *buf_size);
    *buf_size = 0;
    return nullptr;  // don't return corrupted buffer
  }

  return std::unique_ptr<char[], ServerRequestInterface::BlockDeleter>(
      block, ServerRequestInterface::BlockDeleter(*buf_size));
}

std::unique_ptr<char[], ServerRequestInterface::BlockDeleter>
EvHTTPRequest::ReadRequestGzipBytes(evbuffer* input_buf, int64_t* size) {
  std::vector<absl::Span<char>> buf_list;

  size_t body_length = 0;
  while (true) {
    auto buf_size = evbuffer_get_contiguous_space(input_buf);

    if (buf_size == 0) {
      break;  // EOF
    }

    char* block = std::allocator<char>().allocate(buf_size);
    int ret = evbuffer_remove(input_buf, block, buf_size);
    if (ret != buf_size) {
      NET_LOG(ERROR, "Unexpected: read less than specified num_bytes : %zu",
              buf_size);
      std::allocator<char>().deallocate(block, buf_size);
      for (auto buf : buf_list) {
        std::allocator<char>().deallocate(buf.data(), buf.size());
      }
      *size = 0;
      return nullptr;  // don't return corrupted buffer
    }

    body_length += buf_size;
    buf_list.emplace_back(block, buf_size);
  }

  char* comp_body = std::allocator<char>().allocate(body_length);

  size_t pos = 0;
  for (auto buf : buf_list) {
    memcpy(comp_body + pos, buf.data(), buf.size());
    pos += buf.size();
    std::allocator<char>().deallocate(buf.data(), buf.size());
  }

  char* uncomp_body;
  auto uncomp_size = reinterpret_cast<size_t*>(size);
  UncompressGzipBody(comp_body, body_length,
                     reinterpret_cast<void**>(&uncomp_body), uncomp_size);

  std::allocator<char>().deallocate(comp_body, body_length);

  if (uncomp_body != nullptr) {
    return std::unique_ptr<char[], ServerRequestInterface::BlockDeleter>(
        uncomp_body, ServerRequestInterface::BlockDeleter(*uncomp_size));
  } else {
    NET_LOG(ERROR, "Failed to uncompress the gzipped body");
    *uncomp_size = 0;
    return nullptr;
  }
}

bool EvHTTPRequest::NeedUncompressGzipContent() {
  if (handler_options_ != nullptr &&
      handler_options_->auto_uncompress_input()) {
    auto content_encoding = GetRequestHeader(HTTPHeaders::CONTENT_ENCODING);
    return absl::StrContains(content_encoding, "gzip");
  }

  return false;
}

void EvHTTPRequest::UncompressGzipBody(void* input, size_t input_size,
                                       void** uncompressed_input,
                                       size_t* uncompressed_input_size) {
  int64_t max = handler_options_->auto_uncompress_max_size() > 0
                    ? handler_options_->auto_uncompress_max_size()
                    : ZLib::kMaxUncompressedBytes;

  // our APIs don't need expose the actual content-length
  *uncompressed_input_size = static_cast<size_t>(max);

  ZLib zlib;
  int err = zlib.UncompressGzipAndAllocate(
      reinterpret_cast<Bytef**>(uncompressed_input),
      reinterpret_cast<uLongf*>(uncompressed_input_size),
      static_cast<Bytef*>(input), static_cast<uLong>(input_size));
  if (err != Z_OK) {
    NET_LOG(ERROR, "Got zlib error: %d", err);
  }
}

// Note: passing string_view incurs a copy of underlying std::string data
// (stack)
absl::string_view EvHTTPRequest::GetRequestHeader(
    absl::string_view header) const {
  std::string header_str(header.data(), header.size());
  return absl::NullSafeStringView(
      evhttp_find_header(parsed_request_->headers, header_str.c_str()));
}

std::vector<absl::string_view> EvHTTPRequest::request_headers() const {
  auto result = std::vector<absl::string_view>();
  auto ev_headers = parsed_request_->headers;

  for (evkeyval* header = ev_headers->tqh_first; header;
       header = header->next.tqe_next) {
    result.emplace_back(header->key);
  }

  return result;
}

void EvHTTPRequest::OverwriteResponseHeader(absl::string_view header,
                                            absl::string_view value) {
  evkeyvalq* ev_headers =
      evhttp_request_get_output_headers(parsed_request_->request);

  std::string header_str = std::string(header.data(), header.size());
  const char* header_cstr = header_str.c_str();

  evhttp_remove_header(ev_headers, header_cstr);
  evhttp_add_header(ev_headers, header_cstr,
                    std::string(value.data(), value.size()).c_str());
}

void EvHTTPRequest::AppendResponseHeader(absl::string_view header,
                                         absl::string_view value) {
  evkeyvalq* ev_headers =
      evhttp_request_get_output_headers(parsed_request_->request);

  int ret = evhttp_add_header(ev_headers,
                              std::string(header.data(), header.size()).c_str(),
                              std::string(value.data(), value.size()).c_str());

  if (ret != 0) {
    NET_LOG(ERROR,
            "Unexpected: failed to set the request header"
            " %.*s: %.*s",
            static_cast<int>(header.size()), header.data(),
            static_cast<int>(value.size()), value.data());
  }
}

void EvHTTPRequest::PartialReplyWithStatus(HTTPStatusCode status) {
  NET_LOG(FATAL, "PartialReplyWithStatus not implemented.");
}

void EvHTTPRequest::PartialReply() {
  NET_LOG(FATAL, "PartialReplyWithStatus not implemented.");
}

ServerRequestInterface::CallbackStatus
EvHTTPRequest::PartialReplyWithFlushCallback(std::function<void()> callback) {
  NET_LOG(FATAL, "PartialReplyWithStatus not implemented.");
  return CallbackStatus::NOT_SCHEDULED;
}

void EvHTTPRequest::ReplyWithStatus(HTTPStatusCode status) {
  bool result =
      server_->EventLoopSchedule([this, status]() { EvSendReply(status); });

  if (!result) {
    NET_LOG(ERROR, "Failed to EventLoopSchedule ReplyWithStatus()");
    Abort();
    // TODO(wenboz): should have a forced abort that doesn't write back anything
    // to the event-loop
  }
}

void EvHTTPRequest::EvSendReply(HTTPStatusCode status) {
  evhttp_send_reply(parsed_request_->request, static_cast<int>(status), nullptr,
                    output_buf);
  server_->DecOps();
  delete this;
}
void EvHTTPRequest::EvSendReply2(evhttp_request* request) {
  //分块响应结束
  std::cout << "分块响应执行完毕" << std::endl;
  evhttp_send_reply_end(request);
  server_->DecOps();
  delete this;
}
void EvHTTPRequest::Reply() { ReplyWithStatus(HTTPStatusCode::OK); }

// Treats this as 500 for now and let libevent decide what to do
// with the connection.
void EvHTTPRequest::Abort() {
  evhttp_send_error(parsed_request_->request, HTTP_INTERNAL, nullptr);
  server_->DecOps();
  delete this;
}

}  // namespace net_http
}  // namespace serving
}  // namespace tensorflow
