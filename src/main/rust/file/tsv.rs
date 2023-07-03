
use crate::helper;
use std::fs;
use std::io::Read;

pub fn read_tsv (tsv_file_path: String)
    -> Result<Vec<Vec<String>>, helper::error::PublicError>
{
    let mut f = fs::File::open (tsv_file_path)?;

    let mut contents = String::new ();
    f.read_to_string (&mut contents)?;

    //println! ("read {:?}", contents);

    let result: Vec<Vec<String>> = contents.trim_end_matches ("\n").split ("\n").map (| line | -> Vec<String> {
        let ldata: Vec<String> = line.split ("\t").map (String::from).collect ();
        ldata
    }).collect ();

    //println! ("result {:?}", result);
    Ok (result)
}

