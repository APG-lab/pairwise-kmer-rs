
use std::io;
use std::num;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PublicError
{
    #[error("ApplicationError: {0}")]
    ApplicationError (String),

    #[error("DataError: {0}")]
    DataError (String),

    #[error("IOError: {0}")]
    IOError (String)
}

impl From<io::Error> for PublicError
{
    fn from (err: io::Error)
    -> PublicError
    {
        PublicError::IOError (err.to_string ())
    }
}

impl From<num::ParseIntError> for PublicError
{
    fn from (err: num::ParseIntError)
    -> PublicError
    {
        PublicError::DataError (err.to_string ())
    }
}
