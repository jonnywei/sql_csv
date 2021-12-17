//! # CSV SQL Query
//! A friendly query csv file use sql syntax
//!
//! ## Installation
//! ```
//! $ cargo install sql_csv
//! ```
//! ## Support Commands
//! **load**
//!
//! Load csv file
//! ```
//! load /home/path/to/xxx.csv
//! ```
//! **store**
//!
//! Store last success SQL query result to csv file
//! ```
//! store /path/to/xxx.csv
//! ```
//! **SQL**
//!
//! All SQL query support.
//!
//! Usage:
//! ```
//! $sql_csv.exe c:\temp\user.csv  c:\temp\author.csv
//! read csv file c:\temp\user.csv to table user
//! read csv file c:\temp\author.csv to table author
//! >select * from user;
//! Result:
//! +----+-----------+------+---------+
//! | id | name      | size | sport   |
//! +----+-----------+------+---------+
//! | 1  | Xiaoputao | 3    | Hiking  |
//! | 2  | Zgu       | 3    | Running |
//! | 3  | Xiaopang  | 2    | Walking |
//! +----+-----------+------+---------+
//! >
//! >load c:\temp\abc.csv
//! load csv file c:\temp\abc.csv to table abc
//! Load ok.
//! >select * from abc;
//! Result:
//! +----+-----------+------+---------+
//! | id | name      | size | sport   |
//! +----+-----------+------+---------+
//! | 1  | Xiaoputao | 3    | Hiking  |
//! | 2  | Zgu       | 3    | Running |
//! | 3  | Xiaopang  | 2    | Walking |
//! +----+-----------+------+---------+
//! >store c:\temp\bar.csv
//! Store ok.
//! >
//! ```
//!
use anyhow::Result;
use datafusion::prelude::*;
use std::io::{BufRead, Stdout, Write};
use std::path::Path;
use std::{env, io};
use std::sync::Arc;
use datafusion::physical_plan::ExecutionPlan;


#[tokio::main]
async fn main() -> Result<()> {
    let mut args = env::args();
    let config = ExecutionConfig::new().with_target_partitions(1);
    let mut ctx = ExecutionContext::with_config(config);

    args.next();
    let mut count = 0;
    while let Some(csv_file) = args.next() {
        load_csv_file(&mut ctx, csv_file).await?;
        count += 1;
    }
    if count == 0 {
        println!("NO CSV file Load.Use `load /home/path/to/xxx.csv` Load CSV file");
        // return Ok(());
    }
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let mut lines = stdin.lock().lines();
    let mut last_sql  = "".to_string();

    loop {
        print_tip(&mut stdout);
        if let Some(Ok(line)) = lines.next(){
            if line.starts_with("--") {
                continue;
            }
            if line.eq_ignore_ascii_case("exit") || line.eq_ignore_ascii_case("quit") {
                println!("Exit.");
                return Ok(());
            }
            if line.eq("") {
                continue;
            }
            if line.starts_with("store ") {
                let to_csv_file = line[5..].trim().to_string();
                let df = ctx.sql(&*last_sql).await?;
                let plan =   ctx.create_physical_plan(&df.to_logical_plan()).await?;
                write_csv_file(&ctx, plan,to_csv_file).await?;
                println!("Store ok.");
                continue;
            }
            if line.starts_with("load ") {
                let csv_file = line[4..].trim().to_string();
                load_csv_file(&mut ctx, csv_file).await?;
                println!("Load ok.");
                continue;
            }
            println!("Result:");
            let df = ctx.sql(&line).await;
            if df.is_err() {
                println!("{}", df.as_ref().err().unwrap());
            } else {
                last_sql = line.to_string();
                let df = df?;
                df.show().await?;
            }
        }
    }
    while let Some(Ok(line)) = lines.next() {

        print_tip(&mut stdout);

    }
    Ok(())
}

fn print_tip(stdout:&mut Stdout) ->Result<()>{

    stdout.write_all(b">")?;
    stdout.flush()?;
    Ok(())
}


async fn write_csv_file(ctx: &ExecutionContext,plan: Arc<dyn ExecutionPlan>,file: String) -> Result<()> {
    let path = std::env::temp_dir();
    // println!("{:#?}",&path.to_str());
    let temp_dir = &path.join("sql_csv_temp");
    std::fs::remove_dir_all(&temp_dir);
    ctx.write_csv(plan,&temp_dir.to_str().unwrap()).await?;
    let temp_csv = &temp_dir.join("part-0.csv");
    std::fs::copy(temp_csv,file);
    std::fs::remove_dir_all(&temp_dir);
    Ok(())

}


 async fn load_csv_file(ctx: &mut ExecutionContext, csv_file: String) -> Result<()> {
    let file_path = Path::new(&csv_file);
    let file_name = file_path.file_name().unwrap().to_str().unwrap();
    let end = file_name.rfind(".").unwrap();
    let table_name = &file_name[0..end];
    println!(
        "load csv file {} to table {}",
        file_path.to_str().unwrap(),
        table_name
    );
    ctx.register_csv(
        &table_name,
        file_path.to_str().unwrap(),
        CsvReadOptions::new(),
    ).await?;
     Ok(())
}