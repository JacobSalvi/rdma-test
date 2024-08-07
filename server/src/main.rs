use clap::{arg, value_parser, Command};

fn main() {
    let matches = Command::new("RC PingPong Server").version("1.0").about("")
        .arg(arg!(-p --packets <PACKETS> "Number of packets to process in the experiment.").value_parser(value_parser!(u64).range(1..)).required(true))
        .arg(arg!(-d --dev <NAME> "Interface to attach XDP program to.").required(true))
        .arg(arg!(-g --gidx <IDX> "Group index to attach the XDP program to").value_parser(value_parser!(i32)).required(true))
        .get_matches();
    let ib_devname: &str = matches.get_one::<String>("dev").unwrap();
    let gidx: i32 = *matches.get_one::<i32>("gidx").unwrap();
    let iters: u64 = *matches.get_one::<u64>("packets").unwrap();
    if let Err(e) = rdma_lib::run_server(iters, ib_devname, gidx, None) {
        println!("{e}")
    };
}
