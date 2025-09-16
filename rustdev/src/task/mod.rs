//! Task system for coordinating storage operations

pub mod traits;
pub mod scheduler;
pub mod read;
pub mod write;

pub use traits::{Task, TaskResult, TaskPriority, TaskType, TaskContext};
pub use scheduler::{TaskScheduler, TaskHandle};

// Export task implementations
pub use read::{ReadTask, BatchReadTask};
pub use write::{WriteTask, BatchWriteTask, DeleteTask};