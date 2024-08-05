use std::{net::{Ipv4Addr, SocketAddrV4}, str::FromStr};

use rand::{rngs::StdRng, Rng, SeedableRng};
use rdma::{ah, bindings, device::{Gid, LinkLayer, Mtu, PortAttr}, mr::{self, AccessFlags}, pd::ProtectionDomain, poll_cq_attr, qp, wr};



const PACKET_SIZE: usize = 1024;
const RECEIVE_DEPTH: u32 = 500;
pub const IB_PORT: u8 = 1;
const PRIORITY: u8 = 0;
const PINGPONG_RECV_WRID: u64 = 1;
const PINGPONG_SEND_WRID: u64 = 2;

struct Context{
    recv_buf: *mut u8,
    send_buf: *mut u8,
    context: rdma::ctx::Context, 
    _pd: rdma::pd::ProtectionDomain,
    recv_mr: rdma::mr::MemoryRegion,
    send_mr: rdma::mr::MemoryRegion,
    cq: rdma::cq::CompletionQueue, 
    qp: rdma::qp::QueuePair,
    qpx: rdma::qp_ex::QueuePairEx,
}

fn setup_memaligned_buffer(size: usize) -> Result<*mut u8, std::io::Error>{
    unsafe{
        let page_size=  libc::sysconf(libc::_SC_PAGESIZE);

        let buf = libc::memalign(page_size as usize, size);
        if buf.is_null() {
            return Err(std::io::Error::new(std::io::ErrorKind::Other,"Couldn't allocate aligned memory"));
        }

        libc::memset(buf, 0, size);
        Ok(buf as *mut u8)
    }
}


impl Context{
    pub fn new(device: &rdma::device::Device) -> Result<Self, std::io::Error>{
        let recv_buf = setup_memaligned_buffer(PACKET_SIZE)?;
        let send_buf = setup_memaligned_buffer(PACKET_SIZE)?;
        let context = device.open()?;
        let pd = ProtectionDomain::alloc(&context)?;
        let recv_mr = unsafe {
            rdma::mr::MemoryRegion::register(&pd,recv_buf, PACKET_SIZE , AccessFlags::all(), ())?
        }; 
        let send_mr = unsafe {
            rdma::mr::MemoryRegion::register(&pd, send_buf, PACKET_SIZE, AccessFlags::all(), ())?
        };
        let mut cq_options = rdma::cq::CompletionQueue::options();
        cq_options.cqe(RECEIVE_DEPTH as usize+1);
        cq_options.wc_flags(bindings::IBV_WC_EX_WITH_COMPLETION_TIMESTAMP as u64);
        let cq = rdma::cq::CompletionQueue::create(&context, cq_options)?;
        let mut qp_options: rdma::qp::QueuePairOptions = rdma::qp::QueuePair::options();
        qp_options.cap(rdma::qp::QueuePairCapacity{max_send_wr: 1, max_recv_wr: RECEIVE_DEPTH, max_send_sge: 1, max_recv_sge: 1, max_inline_data: 0});
        qp_options.qp_type(rdma::qp::QueuePairType::RC);
        qp_options.comp_mask(bindings::IBV_QP_INIT_ATTR_PD | bindings::IBV_QP_INIT_ATTR_SEND_OPS_FLAGS);
        qp_options.send_ops_flags(bindings::IBV_QP_EX_WITH_SEND);
        qp_options.send_cq(&cq);
        qp_options.recv_cq(&cq);
        qp_options.pd(&pd);

        let qp = rdma::qp::QueuePair::create(&context, qp_options)?;
        let qpx = qp.to_qp_ex()?;
        let mut modify_options: qp::ModifyOptions = qp::ModifyOptions::default();
        modify_options.qp_state(qp::QueuePairState::Initialize);
        modify_options.pkey_index(0);
        modify_options.port_num(IB_PORT);
        modify_options.qp_access_flags(mr::AccessFlags::empty());
        qp.modify(modify_options)?;
        Ok(Context{recv_buf, send_buf, context, _pd: pd, recv_mr, send_mr, cq, qp, qpx})
        
    }

    pub fn pp_ib_connect(&self, port: u8, local_psn: u32, mtu: rdma::device::Mtu, sl: u8, dest: &IbNodeInfo, gid_idx: u8) -> Result<(), std::io::Error>{
        let mut modify_options = qp::ModifyOptions::default();
        modify_options.qp_state(qp::QueuePairState::ReadyToReceive);
        modify_options.path_mtu(mtu);
        modify_options.dest_qp_num(dest.qpn);
        modify_options.rq_psn(dest.psn);
        modify_options.max_dest_rd_atomic(1);
        modify_options.min_rnr_timer(12);
        let mut ah_option = ah::AddressHandleOptions::default();
        ah_option.dest_lid(dest.lid);
        ah_option.service_level(sl);
        ah_option.port_num(port);
        if dest.gid.interface_id() != 0 {
            ah_option.global_route_header(rdma::ah::GlobalRoute { dest_gid: dest.gid, flow_label: 0,
                sgid_index: gid_idx, hop_limit: 1, traffic_class: 0 });
        }
        modify_options.ah_attr(ah_option);
        match self.qp.modify(modify_options){
            Ok(_) => (),
            Err(e) =>{
                    println!("{e}");
                    return Err(std::io::Error::new(std::io::ErrorKind::Other,"Failed to modify QP to RTR"))
                }
        };

        let mut modify_options = qp::ModifyOptions::default(); 
        modify_options.qp_state(qp::QueuePairState::ReadyToSend);
        modify_options.timeout(31);
        modify_options.retry_cnt(7);
        modify_options.rnr_retry(7);
        modify_options.sq_psn(local_psn);
        modify_options.max_rd_atomic(1);
        match self.qp.modify(modify_options){
            Ok(_) => (),
            Err(_) => return Err(std::io::Error::new(std::io::ErrorKind::Other,"Failed to modify QP to RTR"))
        };
        
        Ok(())
    }

    pub fn post_recv(&self, n: u32) -> u32{
        let sge = wr::Sge{addr: self.recv_buf as u64, length: PACKET_SIZE as u32, lkey: self.recv_mr.lkey()}; 
        let mut wr = wr::RecvRequest::zeroed();
        wr.id(PINGPONG_RECV_WRID);
        wr.sg_list(&[sge]);
        let mut i = 0;
        while i< n{
            unsafe{
                if self.qp.post_recv(&wr).is_err() {
                    break
                }
            } 
            i+=1;
        }
        println!("Posted{i} receives");
        i    
    }


     fn post_send(&mut self) -> Result<(), std::io::Error>{
        self.qpx.start_wr(); 
        self.qpx.wr_id(PINGPONG_SEND_WRID);
        self.qpx.wr_flags(bindings::IBV_SEND_SIGNALED);
        self.qpx.post_send()?;
        
        let buf = self.send_buf;
        self.qpx.set_sge(self.send_mr.lkey(), buf as u64, PACKET_SIZE as u32);
        self.qpx.wr_complete()?;

       Ok(()) 
    }
    
    pub fn parse_single_wc(&mut self, available_recv: &mut u32, is_server: bool) -> Result<(), std::io::Error>{
        let status = self.cq.status();
        let wr_id = self.cq.wr_id();
        if status != bindings::IBV_WC_SUCCESS{
            println!("Failed: status {status} for wr_id {wr_id}");
            return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed: status {} for wr_id {}", status, wr_id).as_str()));
        }
        match wr_id {
            PINGPONG_SEND_WRID => println!("Sent packet"),
            PINGPONG_RECV_WRID => {
                println!("Received packet");
                match is_server{
                    false => {
                    }
                    true => {
                        self.post_send()?
                    }
                }
                *available_recv -= 1;
                if *available_recv <=1{
                    println!("before post_recv");
                    *available_recv += self.post_recv(RECEIVE_DEPTH - *available_recv);
                    println!("after post_recv");
                    if *available_recv < RECEIVE_DEPTH {
                        return Err(std::io::Error::new(std::io::ErrorKind::Other, "Couldn't post enough receives, there are only {available_recv}"));

                    }
                }
            }
            _ => {
                     return Err(std::io::Error::new(std::io::ErrorKind::Other, "completion for unknown wr_id {wr_id}"));
                 }
            
        }
        let _wr_id = <u64 as std::convert::TryInto<u8>>::try_into(wr_id).unwrap();
        Ok(())
    }
}


#[derive(Clone)]
pub struct IbNodeInfo {
    pub lid: u16,
    pub qpn: u32,
    pub psn: u32,
    pub gid: Gid 
}


impl IbNodeInfo{
    fn new(context:  &Context, ib_port: u8, gidx: i32) -> Result<Self, std::io::Error>{
        let port_attr = PortAttr::query(&context.context, ib_port)?;

        let lid = port_attr.lid();
        if port_attr.link_layer() != LinkLayer::Ethernet && lid ==0{
            return Err(std::io::Error::new(std::io::ErrorKind::Other, "Couldn't get local LID"));
        }
        let gid = Gid::query(&context.context, ib_port, gidx)?;
        let mut r = StdRng::seed_from_u64(0);
        let psn = r.gen::<u32>() & 0xffffff; 
        
        Ok(IbNodeInfo{lid, gid, qpn: context.qp.qp_num(), psn})
    }
    
    pub fn serialize(&self) -> [u8; 26]{
        let lid = self.lid.to_le_bytes();
        let qpn = self.qpn.to_le_bytes();
        let psn = self.psn.to_le_bytes();
        let gid = self.gid.as_bytes();
        let mut res = [0; 26];
        for i in 0..26{
            if i < 2 {
                res[i] = lid[i];
            } else if i < 6{
                res[i] = qpn[i-2];
            } else if i< 10{
                res[i] = psn[i-6];
            } else {
                res[i] = gid[i-10];
            }
        }
        res
    }
    
    pub fn deserialize(bytes: &[u8]) -> Self{
        let lid = u16::from_le_bytes(bytes[..2].try_into().unwrap());
        let qpn = u32::from_le_bytes(bytes[2..6].try_into().unwrap());
        let psn = u32::from_le_bytes(bytes[6..10].try_into().unwrap());
        let gid = Gid::from_bytes(bytes[10..26].try_into().unwrap());
        IbNodeInfo{lid, qpn, psn, gid}
    }

    pub fn print(&self){
        let res = self.gid.to_ipv6_addr(); 
        println!("Address: LID 0x{:04x}, QPN 0x{:06x}, PSN 0x{:06x}, GID {}", self.lid, self.qpn, self.psn, res);
    }
}

pub fn ib_device_find_by_name<'a>(device_list: &'a rdma::device::DeviceList, name: &str) -> Result<Option<&'a rdma::device::Device>, std::io::Error>{
    let mut dev: Option<&rdma::device::Device> = None;
    for device in device_list.as_slice() {
        if name == device.name(){
            dev = Some(device);
        }
    }
    Ok(dev)
}

pub fn new_sockaddr(ip: &str, port: u16) -> SocketAddrV4{
    let ip_addr = Ipv4Addr::from_str(ip).unwrap_or_else(|err| {
        eprintln!("Invalid IP address: {}", err);
        std::process::exit(1);
    });
    SocketAddrV4::new(ip_addr, port)
}

pub fn exchange_data(server_ip: Option<&str>, buffer: &[u8]) -> Result<(usize, [u8;1024]), std::io::Error>{
    let port = if server_ip.is_none() {1234} else {1235};
    let socket_addr = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port);
    let socket = std::net::UdpSocket::bind(socket_addr)?;
    let mut out_buffer = [0; 1024];
    match server_ip{
        Some(sip) => {
            let server_addr = new_sockaddr(sip, 1234);
            socket.send_to(buffer, server_addr)?;
            let (size, _) = socket.recv_from(&mut out_buffer)?;
            Ok((size, out_buffer))
        }
        None => {
            println!("before receive");
            let (size, client_addr) = socket.recv_from(&mut out_buffer)?;
            socket.send_to(buffer, client_addr)?; 
            Ok((size, out_buffer))
        }
    }
}

pub fn run_client(iters: u64, ib_devname: &str, port_gid_idx: i32, server_ip: Option<&str>)-> Result<(), std::io::Error>{
    let device_list = rdma::device::DeviceList::available();
    let device_list: rdma::device::DeviceList = device_list?;
    
    
    let device = ib_device_find_by_name(&device_list, ib_devname)?;
    if device.is_none(){
        return Err(std::io::Error::new(std::io::ErrorKind::Other, "device not found"));
    }
    let device = device.unwrap();
    let mut ctx = Context::new(device)?;
    let local_info = IbNodeInfo::new(&ctx, IB_PORT, port_gid_idx)?;
    local_info.print();

    let (_, buf) = exchange_data(server_ip, &local_info.serialize())?;
    let remote_info = IbNodeInfo::deserialize(&buf);
    remote_info.print();
    ctx.pp_ib_connect(IB_PORT, local_info.psn, Mtu::Mtu1024, PRIORITY, &remote_info, (port_gid_idx as usize).try_into().unwrap())?;
    poll(iters, &mut ctx, false)?;
    Ok(())
    
}

pub fn run_server(iters: u64, ib_devname: &str, port_gid_idx: i32, server_ip: Option<&str>)-> Result<(), std::io::Error>{
    let device_list = rdma::device::DeviceList::available();
    let device_list: rdma::device::DeviceList = device_list?;
    
    
    let device = ib_device_find_by_name(&device_list, ib_devname)?;
    if device.is_none(){
        return Err(std::io::Error::new(std::io::ErrorKind::Other, "device not found"));
    }
    let device = device.unwrap();
    let mut ctx = Context::new(device)?;
    let local_info = IbNodeInfo::new(&ctx, IB_PORT, port_gid_idx)?;
    local_info.print();

    let (_, buf) = exchange_data(server_ip, &local_info.serialize())?;
    let remote_info = IbNodeInfo::deserialize(&buf);
    remote_info.print();
    ctx.pp_ib_connect(IB_PORT, local_info.psn, Mtu::Mtu1024, PRIORITY, &remote_info, (port_gid_idx as usize).try_into().unwrap())?;
    poll(iters, &mut ctx, true)?;

    Ok(())
    
}

fn poll(iters: u64, ctx: &mut Context, is_server: bool) -> Result<(), std::io::Error>{
    let mut available_receive: u32 = ctx.post_recv(RECEIVE_DEPTH);
    let mut recv_count: u64 = 0;
    
    while recv_count < iters {
        let mut attr = poll_cq_attr::PollCQAttr::new_empty();
        let mut res;
        loop{
            res = ctx.cq.start_poll(&mut attr);
            if res != libc::ENOENT {
                break;
            }
        }
        println!("after second loop");
        if res != 0 {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, "Failed to poll CQ"));
        }
        println!("status: {}", ctx.cq.status());
        let res = ctx.parse_single_wc(&mut available_receive, is_server);
        if res.is_err() {
            ctx.cq.end_poll();
            return Err(std::io::Error::new(std::io::ErrorKind::Other, "Failed to parse WC"));
        }
        recv_count +=1;
        let mut ret = ctx.cq.next_poll();
        if ret  == 0{
            let parse_ret = ctx.parse_single_wc(&mut available_receive, is_server);
            match parse_ret{
                Ok(_) => recv_count +=1,
                Err(_) => {
                    ret = 1;
                } 
            }
        }
        ctx.cq.end_poll();
        if ret != 0 && ret!= libc::ENOENT{
            return Err(std::io::Error::new(std::io::ErrorKind::Other, "Failed to poll CQ"));
        }
    }
    Ok(())
}
