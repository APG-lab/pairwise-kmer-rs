
use crate::helper;
use log::debug;
use std::collections;
use std::process;

pub fn read_hash_counts (jf_file_path: String)
    -> Result<(usize, collections::HashMap<String, usize>), helper::error::PublicError>
{
    match process::Command::new ("jellyfish")
        .args (["dump", "--column", "--tab", &jf_file_path])
        .stdout(process::Stdio::piped ())
        .spawn ()
    {
        Ok (child) => {
            match child.wait_with_output ()
            {
                Ok (out) => {
                    let mut result = collections::HashMap::new ();
                    let mut lc = 0;
                    let mut total = 0;
                    let buf_string = String::from_utf8_lossy (&out.stdout);
                    for line in buf_string.split ('\n')
                    {
                        lc += 1;
                        if lc % 10_000_000 == 0
                        {
                            debug! ("Read {} lines from {}", lc, jf_file_path);
                        }
                        if !line.is_empty ()
                        {
                            let ldata = line.split ('\t').map (String::from).collect::<Vec<String>> ();
                            if ldata.len () < 2
                            {
                                log::debug! ("ldata too short: {:?}", ldata);
                            }
                            let count = ldata[1].parse::<usize> ().map_err (|e| helper::error::PublicError::DataError (format! ("ldata: '{:?}' err: {}", ldata, e.to_string ())))?;
                            result.insert (ldata[0].clone (), count );
                            total += count;
                        }
                    }
                    Ok ( (total, result) )
                },
                Err (e) => Err (helper::error::PublicError::DataError (format! ("No output for {}. Reason: {}", &jf_file_path, e.to_string ())))
            }
        },
        Err (e) => Err (helper::error::PublicError::ApplicationError (format! ("Failed to run jellyfish dump for {}: {}", &jf_file_path, e.to_string ())))
    }
}

