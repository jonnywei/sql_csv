//!   # CSV SQL query
//!   A friendly query csv file use sql syntax
//!
//!   Usage:
//!   ```
//!   $sql_csv.exe c:\temp\db.csv  c:\temp\author.csv
//!   read csv file c:\temp\db.csv to table db
//!   read csv file c:\temp\author.csv to table author
//!   >select * from db;
//!   Result:
//!   +----+-----------+------+---------+
//!   | id | name      | size | sport   |
//!   +----+-----------+------+---------+
//!   | 1  | Xiaoputao | 3    | Hiking  |
//!   | 2  | Zgu       | 3    | Running |
//!   | 3  | Xiaopang  | 2    | Walking |
//!   +----+-----------+------+---------+
//!   >
//!   ```
use anyhow::Result;
use datafusion::prelude::*;
use std::io::{BufRead, Write};
use std::path::Path;
use std::{env, io};


#[tokio::main]
async fn main() -> Result<()> {
    let mut args = env::args();
    let mut ctx = ExecutionContext::new();
    args.next();
    let mut count = 0;
    while let Some(csv_file) = args.next() {
        let file_path = Path::new(&csv_file);
        let file_name = file_path.file_name().unwrap().to_str().unwrap();
        let end = file_name.rfind(".").unwrap();
        let table_name = &file_name[0..end];
        println!(
            "read csv file {} to table {}",
            file_path.to_str().unwrap(),
            table_name
        );
        ctx.register_csv(
            &table_name,
            file_path.to_str().unwrap(),
            CsvReadOptions::new(),
        )
        .await?;
        count += 1;
    }
    if count == 0 {
        println!("Please choose at least one CSV file");
        return Ok(());
    }
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let mut lines = stdin.lock().lines();
    stdout.write_all(b">");
    stdout.flush();
    while let Some(Ok(line)) = lines.next() {
        if !line.starts_with("--") {
            if line.eq_ignore_ascii_case("exit") || line.eq_ignore_ascii_case("quit") {
                return Ok(());
            }
            if line.eq("") {
                continue;
            }
            println!("Result:");

            let df = ctx.sql(&line).await;
            if df.is_err() {
                println!("{}", df.as_ref().err().unwrap());
            } else {
                df?.show().await?;
            }
            stdout.write_all(b">");
            stdout.flush();
        }
    }
    Ok(())
}
