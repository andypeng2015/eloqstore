use crate::store::{ReadRequest, BatchWriteRequest, ScanRequest, FloorRequest, TruncateRequest};
use crate::store::{KvRequest, RequestType};
use crate::types::TableIdent;
use crate::error::KvError;

/// Wrapper enum for different request types to allow ownership transfer
pub enum RequestWrapper {
    Read(ReadRequest),
    BatchWrite(BatchWriteRequest),
    Scan(ScanRequest),
    Floor(FloorRequest),
    Truncate(TruncateRequest),
}

impl RequestWrapper {
    /// Create from a KvRequest reference
    pub fn from_request(req: &dyn KvRequest) -> Option<Self> {
        match req.request_type() {
            RequestType::Read => {
                req.as_any()
                    .downcast_ref::<ReadRequest>()
                    .map(|r| RequestWrapper::Read(r.clone()))
            }
            RequestType::BatchWrite => {
                req.as_any()
                    .downcast_ref::<BatchWriteRequest>()
                    .map(|r| RequestWrapper::BatchWrite(r.clone()))
            }
            RequestType::Scan => {
                req.as_any()
                    .downcast_ref::<ScanRequest>()
                    .map(|r| RequestWrapper::Scan(r.clone()))
            }
            RequestType::Floor => {
                req.as_any()
                    .downcast_ref::<FloorRequest>()
                    .map(|r| RequestWrapper::Floor(r.clone()))
            }
            RequestType::Truncate => {
                req.as_any()
                    .downcast_ref::<TruncateRequest>()
                    .map(|r| RequestWrapper::Truncate(r.clone()))
            }
            _ => None,
        }
    }

    /// Get as boxed trait object
    pub fn into_box(self) -> Box<dyn KvRequest> {
        match self {
            RequestWrapper::Read(r) => Box::new(r),
            RequestWrapper::BatchWrite(r) => Box::new(r),
            RequestWrapper::Scan(r) => Box::new(r),
            RequestWrapper::Floor(r) => Box::new(r),
            RequestWrapper::Truncate(r) => Box::new(r),
        }
    }
}