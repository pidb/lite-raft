#![feature(impl_trait_in_assoc_type)]
use std::cmp;
use std::error::Error;
use std::io;
use std::io::Write;

use console::Key;
use console::Term;
use tonic::transport::Channel;

use crate::grpc::kv_service_client::KvServiceClient;
use crate::grpc::PutRequest;
use crate::grpc::PutResponse;

pub mod grpc {
    tonic::include_proto!("kv");
}

struct ClientWrap {
    client: Option<KvServiceClient<Channel>>,
}

impl ClientWrap {
    pub fn new() -> Self {
        Self { client: None }
    }

    pub async fn connect(&mut self, addr: &str) -> Result<(), Box<dyn Error>> {
        if let Some(client) = self.client.take() {
            drop(client)
        }
        let client = KvServiceClient::connect(addr.to_string()).await?;
        self.client = Some(client);
        Ok(())
    }

    pub fn is_connected(&self) -> bool {
        if let Some(cc) = self.client.as_ref() {
            return true;
        }

        false
    }

    pub async fn put(&mut self, key: String, value: &[u8]) {
        let resp = self
            .client
            .as_mut()
            .unwrap()
            .put(PutRequest {
                key,
                value: value.to_vec(),
            })
            .await
            .unwrap();
        println!("{:?}", resp);
    }
}

fn cyan_style_stdout(out: &str) {
    println!("{}", console::style(out).cyan());
    io::stdout().flush().unwrap();
}

fn red_style_stderr(out: &str) {
    println!("{}", console::style(out).red());
    io::stdout().flush().unwrap();
}

fn smart_read_line(term: &Term, input: &mut String) {
    input.clear();
    loop {
        let key = term.read_key().unwrap();
        match key {
            Key::Char(c) => {
                print!("{}", c);
                input.push(c);
            }
            Key::Enter => {
                println!();
                break;
            }

            Key::Backspace => {
                input.pop();
                term.clear_line().unwrap();
                print!("{}", input);
            }

            _ => {}
        };
        io::stdout().flush().unwrap();
    }
}

#[tokio::main]
async fn main() {
    let mut client = ClientWrap::new();
    let stdin = io::stdin();
    let mut input = String::new();
    let term = Term::stdout();
    console::set_colors_enabled(true);
    console::set_colors_enabled_stderr(true);

    loop {
        if !client.is_connected() {
            cyan_style_stdout("Please enter the server addr");
            smart_read_line(&term, &mut input);

            if let Err(err) = client.connect(&input.trim()).await {
                red_style_stderr(format!("connect to {} error: {}", input.trim(), err).as_str());
                continue;
            }
        }

        input.clear();
        cyan_style_stdout("Select operations: [Put, Get]");
        stdin.read_line(&mut input).unwrap();

        match input.to_lowercase().trim() {
            "put" => {
                cyan_style_stdout("Please enter key");
                smart_read_line(&term, &mut input);

                let key = input.trim().to_string();
                cyan_style_stdout("Please enter value");
                smart_read_line(&term, &mut input);
                let value = input.trim().to_string();

                println!("key = {}, value = {}", key, value);
                client.put(key, value.as_bytes()).await;
            }
            "get" => println!("do put"),
            _ => {
                red_style_stderr(format!("invalid operation for {}", input.trim()).as_str());
                continue;
            }
        }
    }
}
