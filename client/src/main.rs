use std::io::Write;

enum Statement {
    Select,
    Insert,
    Unknown,
}

fn main() -> Result<(), std::io::Error> {
    println!("welcome to equilia client.");

    // TODO: handle connection :)

    loop {
        print!("equilia > ");
        std::io::stdout().flush().unwrap();
        let mut buffer = String::new();
        std::io::stdin().read_line(&mut buffer)?;
        let b = buffer.trim();

        if "exit".eq(b) || "quit".eq(b) {
            break;
        }

        let statement = if b.starts_with("select") {
            Statement::Select
        } else if b.starts_with("select") {
            Statement::Insert
        } else {
            Statement::Unknown
        };

        match statement {
            Statement::Select => todo!(),
            Statement::Insert => todo!(),
            Statement::Unknown => println!("unrecognized statement."),
        }
    }

    println!("bye.");
    Ok(())
}
